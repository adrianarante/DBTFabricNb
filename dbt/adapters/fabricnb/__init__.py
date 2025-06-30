from dbt.adapters.fabricnb.connections import FabricNbConnectionManager # noqa
from dbt.adapters.fabricnb.connections import FabricNbCredentials
from dbt.adapters.fabricnb.impl import FabricNbAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import fabricnb


Plugin = AdapterPlugin(
    adapter=FabricNbAdapter,
    credentials=FabricNbCredentials,
    include_path=fabricnb.PACKAGE_PATH
    )
