from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="customer_search",
    watermark_column="datetime_modified",
    initial_load_value="2020-01-14",
    schema_version_prefix="v2",
    column_list=[
        Column("customer_search_id", "INT", uniqueness=True),
        Column("customer_id", "INT"),
        Column("session_id", "INT"),
        Column("store_group_id", "INT"),
        Column("query_text", "VARCHAR(500)"),
        Column("query_time_ms", "INT"),
        Column("query_count", "INT"),
        Column("query_version", "VARCHAR(25)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("keywords", "VARCHAR(500)"),
        Column("facet", "INT"),
        Column("autosuggest", "INT"),
    ],
)
