from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="review_template_field",
    schema_version_prefix="v2",
    column_list=[
        Column("review_template_field_id", "INT", uniqueness=True),
        Column("review_template_id", "INT"),
        Column("review_template_field_type_id", "INT"),
        Column("review_form_control_type_id", "INT"),
        Column("review_template_field_group_id", "INT"),
        Column("label", "VARCHAR(255)"),
        Column("description", "VARCHAR(512)"),
        Column("page_number", "INT"),
        Column("enable_comment", "INT"),
        Column("internal_use_only", "INT"),
        Column("required", "INT"),
        Column("placement_manual", "INT"),
        Column("sort", "INT"),
        Column("active", "INT"),
        Column("special_field", "INT"),
        Column("smarter_remarketer", "BOOLEAN"),
        Column("fireworks", "INT"),
    ],
)
