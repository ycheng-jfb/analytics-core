from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="carrier_service_uw",
    column_list=[
        Column("carrier_service_uw_id", "INT", uniqueness=True),
        Column("carrier_service_id", "INT"),
        Column("carrier_id", "INT"),
        Column("code", "VARCHAR(25)"),
        Column("label", "VARCHAR(255)"),
        Column("rate_version", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("carrier_service_hash", "BINARY(20)"),
        Column("external_code", "VARCHAR(25)"),
    ],
    watermark_column="datetime_modified",
)
