from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="membership_product_wait_list_component",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("membership_product_wait_list_component_id", "INT", uniqueness=True),
        Column("membership_product_wait_list_id", "INT"),
        Column("component_product_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
