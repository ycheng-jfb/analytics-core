from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="zip_city_state",
    column_list=[
        Column("zip_city_state_id", "INT", uniqueness=True),
        Column("zip", "VARCHAR(5)"),
        Column("city", "VARCHAR(35)"),
        Column("state", "VARCHAR(2)"),
        Column("areacode", "VARCHAR(3)"),
        Column("county", "VARCHAR(50)"),
        Column("fips", "VARCHAR(5)"),
        Column("timezone", "VARCHAR(5)"),
        Column("dst_flag", "VARCHAR(1)"),
        Column("latitude", "DOUBLE"),
        Column("longitude", "DOUBLE"),
        Column("type", "VARCHAR(1)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
