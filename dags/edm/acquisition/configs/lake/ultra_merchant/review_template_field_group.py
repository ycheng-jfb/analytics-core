from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="review_template_field_group",
    schema_version_prefix="v2",
    column_list=[
        Column("review_template_field_group_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(100)"),
        Column("aggregate_fields", "INT"),
        Column("aggregate_by_answers", "INT"),
    ],
)
