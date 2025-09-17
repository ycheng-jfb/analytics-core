from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultracms",
    schema="dbo",
    table="navigationizer_brand_level_mapping",
    schema_version_prefix="v2",
    column_list=[Column("store_group_id", "INT"), Column("default_store_group", "INT")],
)
