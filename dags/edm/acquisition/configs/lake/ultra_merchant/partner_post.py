from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="partner_post",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("partner_post_id", "INT", uniqueness=True),
        Column("partner_id", "INT"),
        Column("partner_post_type_id", "INT"),
        Column("label", "VARCHAR(75)"),
        Column("record_format", "VARCHAR(8000)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
