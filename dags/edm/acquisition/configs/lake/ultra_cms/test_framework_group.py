from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultracms",
    schema="dbo",
    table="test_framework_group",
    watermark_column="datetime_modified",
    schema_version_prefix="v3",
    column_list=[
        Column("test_framework_group_id", "INT", uniqueness=True),
        Column("group_name", "VARCHAR(20)"),
        Column("activated_group", "INT"),
        Column("device_type", "VARCHAR(50)"),
        Column("number_of_groups", "INT"),
        Column("active", "BOOLEAN"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
