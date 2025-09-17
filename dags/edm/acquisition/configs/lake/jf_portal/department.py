from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="jf_portal",
    schema="dbo",
    table="Department",
    watermark_column="DateUpdate",
    schema_version_prefix="v3",
    column_list=[
        Column("department_id", "INT", uniqueness=True, source_name="DepartmentId"),
        Column("department", "VARCHAR(50)", source_name="Department"),
        Column("division_id", "VARCHAR(50)", source_name="DivisionId"),
        Column("email_from_addr", "VARCHAR(100)", source_name="EmailFromAddr"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
    ],
)
