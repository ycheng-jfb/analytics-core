from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="case_flag_disposition_case_flag_type",
    schema_version_prefix="v2",
    column_list=[
        Column("case_flag_disposition_id", "INT", uniqueness=True),
        Column("case_flag_type_id", "INT", uniqueness=True),
        Column("sort_order", "INT"),
        Column("form_control_type", "VARCHAR(50)"),
        Column("is_required", "INT"),
        Column("unique_identifier", "VARCHAR(36)"),
    ],
)
