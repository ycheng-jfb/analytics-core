from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="customer_solicitation_permission",
    watermark_column="datetime_modified",
    initial_load_value="2020-01-14",
    schema_version_prefix="v2",
    column_list=[
        Column("customer_solicitation_permission_id", "INT", uniqueness=True),
        Column("customer_id", "INT"),
        Column("type", "VARCHAR(25)"),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("ip", "VARCHAR(15)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
