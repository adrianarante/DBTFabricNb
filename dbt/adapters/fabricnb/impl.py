
from concurrent.futures import Future
from dataclasses import dataclass

from typing import Any, Dict, Iterable, List, Optional, Union, Tuple, Callable, Set
from typing_extensions import TypeAlias

from catalog import catalog

import dbt
from dbt.adapters.sql import SQLAdapter as adapter_cls
from dbt.adapters.base.connections import  AdapterResponse
from dbt.adapters.fabricnb import FabricNbConnectionManager
from dbt.adapters.base import AdapterConfig
from dbt.adapters.base import BaseRelation

# logger = AdapterLogger("fabricsparknb")

GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME = "get_columns_in_relation_raw"
LIST_SCHEMAS_MACRO_NAME = "list_schemas"
LIST_RELATIONS_MACRO_NAME = "list_relations_without_caching"
LIST_RELATIONS_SHOW_TABLES_MACRO_NAME = "list_relations_show_tables_without_caching"
DESCRIBE_TABLE_EXTENDED_MACRO_NAME = "describe_table_extended_without_caching"

KEY_TABLE_OWNER = "Owner"
KEY_TABLE_STATISTICS = "Statistics"

TABLE_OR_VIEW_NOT_FOUND_MESSAGES = (
    "[TABLE_OR_VIEW_NOT_FOUND]",
    "Table or view not found",
    "NoSuchTableException",
)

# From dbt-fabricnb-spark implementation
# TODO check if necessary, and double-check if compatible
# for fabric-only stuff
@dataclass
class FabricNbConfig(AdapterConfig):
    file_format: str = "parquet"
    location_root: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    clustered_by: Optional[Union[List[str], str]] = None
    buckets: Optional[int] = None
    options: Optional[Dict[str, str]] = None
    merge_update_columns: Optional[str] = None

    

class FabricNbAdapter(adapter_cls):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """
    # Relation: TypeAlias = SparkRelation
    # RelationInfo = Tuple[str, str, str]
    # Column: TypeAlias = SparkColumn
    ConnectionManager: TypeAlias = FabricNbConnectionManager
    AdapterSpecificConfigs: TypeAlias = FabricNbConfig
    # ConnectionManager = FabricNbConnectionManager

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "datenow()"
    
    # From dbt-fabricnb-spark implementation
    # TODO check if necessary, and double-check if compatible
    # for fabric-only stuff
    def get_relation(self, database: str, schema: str, identifier: str) -> Optional[BaseRelation]:
        if not self.Relation.get_default_include_policy().database:
            database = None  # type: ignore

        #return super().get_relation(database, schema, identifier)
    
        relations_list = self.list_relations(database, schema)

        matches = self._make_match(relations_list, database, schema, identifier)

        if len(matches) > 1:
            kwargs = {
                "identifier": identifier,
                "schema": schema,
                "database": database,
            }
            raise dbt.exceptions.RelationReturnedMultipleResultsError(kwargs, matches)

        elif matches:
            return matches[0]

        return None
    
    # From dbt-fabricnb-spark implementation
    # TODO check if necessary, and double-check if compatible
    # for fabric-only stuff
    def list_schemas(self, database: str) -> List[str]:
        # disabled this one, not sure if this is callable for fabric-only stuff
        results = catalog.ListSchemas(profile=self.config)

        # TODO assign this to a proper data source
        return '' # [row[0] for row in results]

    # From dbt-fabricnb-spark implementation
    # TODO check if necessary, and double-check if compatible
    # for fabric-only stuff
    def check_schema_exists(self, database: str, schema: str) -> bool:
        #logger.debug("Datalake name is ", schema)
        schema = schema.lower()
        results = catalog.ListSchema(profile=self.config, schema=schema)

        exists = False # True if schema in [row[0] for row in results] else False
        return exists
    
    # From dbt-fabricnb-spark implementation
    # TODO check if necessary, and double-check if compatible
    # for fabric-only stuff
    def get_catalog(
        self, manifest: Manifest, selected_nodes: Optional[Set] = None
    ) -> Tuple[agate.Table, List[Exception]]:
        schema_map = self._get_catalog_schemas(manifest)
        if len(schema_map) > 1:
            raise dbt.exceptions.CompilationError(
                f"Expected only one database in get_catalog, found " f"{list(schema_map)}"
            )

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(
                        tpe.submit_connected(
                            self,
                            schema,
                            self._get_one_catalog,
                            info,
                            [schema],
                            manifest,
                        )
                    )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions
    
    # From dbt-fabricnb-spark implementation
    # TODO check if necessary, and double-check if compatible
    # for fabric-only stuff
    def quote(self, identifier: str) -> str:  # type: ignore
        return "`{}`".format(identifier)

    # From dbt-fabricnb-spark implementation
    # TODO check if necessary, and double-check if compatible
    # for fabric-only stuff
    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False, limit: Optional[int] = None
    ) -> Tuple[AdapterResponse, agate.Table]:
        """Execute the given SQL. This is a thin wrapper around
        ConnectionManager.execute.

        :param str sql: The sql to execute.
        :param bool auto_begin: If set, and dbt is not currently inside a
            transaction, automatically begin one.
        :param bool fetch: If set, fetch results.
        :param Optional[int] limit: If set, only fetch n number of rows
        :return: A tuple of the query status and results (empty if fetch=False).
        :rtype: Tuple[AdapterResponse, agate.Table]
        """
        # Convert self.config to a JSON string
        project_root = (self.config.project_root).replace('\\', '/')

        # Inject the JSON into the SQL as a comment
        sql = '/*{"project_root": "'+ project_root + '"}*/' + f'\n{sql}'
        
        #print("sql is ", sql)

        return self.connections.execute(sql=sql, auto_begin=auto_begin, fetch=fetch, limit=limit)
    
    # From dbt-fabricnb-spark implementation
    # TODO check if necessary, and double-check if compatible
    # for fabric-only stuff
    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = "EXCEPT",
    ) -> str:
        """Generate SQL for a query that returns a single row with two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ", ".join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql
    
    # From dbt-fabricnb-spark implementation
    # TODO check if necessary, and double-check if compatible
    # for fabric-only stuff
    def standardize_grants_dict(self, grants_table: agate.Table) -> dict:
        grants_dict: Dict[str, List[str]] = {}
        for row in grants_table:
            grantee = row["Principal"]
            privilege = row["ActionType"]
            object_type = row["ObjectType"]

            # we only want to consider grants on this object
            # (view or table both appear as 'TABLE')
            # and we don't want to consider the OWN privilege
            if object_type == "TABLE" and privilege != "OWN":
                if privilege in grants_dict.keys():
                    grants_dict[privilege].append(grantee)
                else:
                    grants_dict.update({privilege: [grantee]})
        return grants_dict

    # From dbt-fabricnb-spark implementation
    # TODO check if necessary, and double-check if compatible
    # for fabric-only stuff
    def debug_query(self) -> None:
        """Override for DebugTask method"""
        self.execute("select 1 as id")

# spark does something interesting with joins when both tables have the same
# static values for the join condition and complains that the join condition is
# "trivial". Which is true, though it seems like an unreasonable cause for
# failure! It also doesn't like the `from foo, bar` syntax as opposed to
# `from foo cross join bar`.
COLUMNS_EQUAL_SQL = """
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} EXCEPT
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} EXCEPT
             SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a
    cross join table_b
)
select
    INT(row_count_diff.difference) as row_count_difference,
    INT(diff_count.num_missing) as num_mismatched
from row_count_diff
cross join diff_count
""".strip()