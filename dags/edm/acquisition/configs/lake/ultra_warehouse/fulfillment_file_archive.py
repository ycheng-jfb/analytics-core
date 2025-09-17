from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="fulfillment_file_archive",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("fulfillment_file_archive_id", "INT", uniqueness=True),
        Column("fulfillment_id", "INT"),
        Column("print_material_sequence_id", "INT"),
        Column("archive_filename", "VARCHAR(255)"),
        Column("archive_directory", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("status_code_id", "INT"),
    ],
)
