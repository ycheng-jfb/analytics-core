from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="cycle_count_sequence",
    watermark_column="datetime_modified",
    column_list=[
        Column("cycle_count_sequence_id", "INT", uniqueness=True),
        Column("cycle_count_id", "INT"),
        Column("sequence", "INT"),
        Column("cycle_count_type_id", "INT"),
        Column("administrator_id", "INT"),
        Column("is_accurate", "INT"),
        Column("datetime_submitted", "TIMESTAMP_NTZ(3)"),
        Column("status_code_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("accuracy", "DOUBLE"),
    ],
)
