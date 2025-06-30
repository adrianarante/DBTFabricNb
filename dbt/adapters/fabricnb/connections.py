from contextlib import contextmanager
from dataclasses import dataclass
import dbt.common.exceptions # noqa
from dbt.adapters.base import Credentials

from dbt.adapters.sql import SQLConnectionManager as connection_cls

from dbt.logger import GLOBAL_LOGGER as logger

from typing import Any, Optional, Union, Tuple, List, Generator, Iterable, Sequence
from abc import ABC, abstractmethod
from dbt.dataclass_schema import StrEnum
from dbt.contracts.connection import ConnectionState, AdapterResponse
from dbt.events import AdapterLogger
from livysession import LivySessionManager, LivySessionConnectionWrapper
import time
import json 
import re

@dataclass
class FabricNbCredentials(Credentials):
    """
    Defines database specific credentials that get added to
    profiles.yml to connect to new adapter
    """

    # Add credentials members here, like:
    # host: str
    # port: int
    # username: str
    # password: str

    _ALIASES = {
        "dbname":"database",
        "pass":"password",
        "user":"username"
    }

    @property
    def type(self):
        """Return name of adapter."""
        return "fabricnb"

    @property
    def unique_field(self):
        """
        Hashed and included in anonymous telemetry to track adapter adoption.
        Pick a field that can uniquely identify one team/organization building with this adapter
        """
        return self.host

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return ("host","port","username","user")
    
class FabricConnectionMethod(StrEnum):
    # TODO probably should change this
    LIVY = "livy"

class FabricConnectionWrapper(ABC):
    @abstractmethod
    def cursor(self) -> "FabricConnectionWrapper":
        pass

    @abstractmethod
    def cancel(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def rollback(self) -> None:
        pass

    @abstractmethod
    def fetchall(self) -> Optional[List]:
        pass

    @abstractmethod
    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        pass

    @property
    @abstractmethod
    def description(
        self,
    ) -> Sequence[
        Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
    ]:
        pass


class FabricNbConnectionManager(connection_cls):
    TYPE = "fabricnb"


    @contextmanager
    def exception_handler(self, sql: str):
        """
        Returns a context manager, that will handle exceptions raised
        from queries, catch, log, and raise dbt exceptions it knows how to handle.
        """
        # ## Example ##
        # try:
        #     yield
        # except myadapter_library.DatabaseError as exc:
        #     self.release(connection_name)

        #     logger.debug("myadapter error: {}".format(str(e)))
        #     raise dbt.exceptions.DatabaseException(str(exc))
        # except Exception as exc:
        #     logger.debug("Error running SQL: {}".format(sql))
        #     logger.debug("Rolling back transaction.")
        #     self.release(connection_name)
        #     raise dbt.exceptions.RuntimeException(str(exc))
        pass

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.
        """
        # ## Example ##
        # if connection.state == "open":
        #     logger.debug("Connection is already open, skipping open.")
        #     return connection

        # credentials = connection.credentials

        # try:
        #     handle = myadapter_library.connect(
        #         host=credentials.host,
        #         port=credentials.port,
        #         username=credentials.username,
        #         password=credentials.password,
        #         catalog=credentials.database
        #     )
        #     connection.state = "open"
        #     connection.handle = handle
        # return connection
        # pass
        """Need to override the SparkConnectionManager class to use fabric-sparknb instead of fabric-spark"""
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        creds = connection.credentials
        exc = None
        handle: FabricConnectionWrapper = None

        for i in range(1 + creds.connect_retries):
            try:
                if creds.method == FabricConnectionMethod.LIVY:
                    try:
                        thread_id = cls.get_thread_identifier()
                        if thread_id not in cls.connection_managers:
                            cls.connection_managers[thread_id] = LivySessionManager()
                        handle = LivySessionConnectionWrapper(
                            cls.connection_managers[thread_id].connect(creds)
                        )
                        connection.state = ConnectionState.OPEN
                        # SparkConnectionManager.fetch_spark_version(handle)
                    except Exception as ex:
                        logger.debug("Connection error: {}".format(ex))
                        connection.state = ConnectionState.FAIL
                else:
                    raise dbt.exceptions.DbtProfileError(
                        f"invalid credential method: {creds.method}"
                    )
                break
            except Exception as e:
                exc = e
                if isinstance(e, EOFError):
                    # The user almost certainly has invalid credentials.
                    # Perhaps a token expired, or something
                    msg = "Failed to connect"
                    if creds.token is not None:
                        msg += ", is your token valid?"
                    raise dbt.exceptions.FailedToConnectError(msg) from e
                retryable_message = _is_retryable_error(e)
                if retryable_message and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {retryable_message}\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                elif creds.retry_all and creds.connect_retries > 0:
                    msg = cls._build_retry_all_message(exc, creds, i)
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                else:
                    raise dbt.exceptions.FailedToConnectError("failed to connect") from e
        else:
            raise exc  # type: ignore

        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection

    @classmethod
    def get_response(cls,cursor):
        """
        Gets a cursor object and returns adapter-specific information
        about the last executed command generally a AdapterResponse ojbect
        that has items such as code, rows_affected,etc. can also just be a string ex. "OK"
        if your cursor does not offer rich metadata.
        """
        # ## Example ##
        # return cursor.status_message
        pass

    def cancel(self, connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        # ## Example ##
        # tid = connection.handle.transaction_id()
        # sql = "select cancel_transaction({})".format(tid)
        # logger.debug("Cancelling query "{}" ({})".format(connection_name, pid))
        # _, cursor = self.add_query(sql, "master")
        # res = cursor.fetchone()
        # logger.debug("Canceled query "{}": {}".format(connection_name, res))
        pass
