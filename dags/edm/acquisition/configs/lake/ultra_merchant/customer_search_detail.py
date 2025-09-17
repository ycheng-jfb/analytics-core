from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="customer_search_detail",
    watermark_column="datetime_modified",
    initial_load_value="2020-05-10 20:00:00",
    schema_version_prefix="v2",
    column_list=[
        Column("customer_search_detail_id", "INT", uniqueness=True),
        Column("customer_search_id", "INT"),
        Column("name", "VARCHAR(50)"),
        Column("value", "VARCHAR(3500)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
