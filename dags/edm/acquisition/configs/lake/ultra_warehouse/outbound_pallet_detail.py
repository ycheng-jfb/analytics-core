from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="outbound_pallet_detail",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("outbound_pallet_detail_id", "INT", uniqueness=True),
        Column("outbound_pallet_id", "INT"),
        Column("package_id", "INT"),
        Column("administrator_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
