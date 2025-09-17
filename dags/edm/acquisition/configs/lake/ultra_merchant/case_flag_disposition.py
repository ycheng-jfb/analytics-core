from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="case_flag_disposition",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("case_flag_disposition_id", "INT", uniqueness=True),
        Column("case_disposition_type_id", "INT"),
        Column("store_group_id", "INT"),
        Column("label", "VARCHAR(100)"),
        Column("statuscode", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
