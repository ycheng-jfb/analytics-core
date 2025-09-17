from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="case_source_location",
    watermark_column="datetime_added",
    enable_archive=False,
    schema_version_prefix="v2",
    column_list=[
        Column("case_source_location_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(100)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
