from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="item",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("item_id", "INT", uniqueness=True),
        Column("item_type_id", "INT"),
        Column("item_number", "VARCHAR(30)"),
        Column("description", "VARCHAR(255)"),
        Column("current_cost", "NUMBER(19, 4)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("coo_iso2", "VARCHAR(2)", source_name="COO_ISO2"),
        Column("market", "BOOLEAN"),
        Column("primary_store_group_id", "INT"),
    ],
)
