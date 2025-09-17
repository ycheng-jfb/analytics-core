from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="product_bundle_component_delete_log",
    column_list=[
        Column("product_bundle_component_delete_log_id", "INT"),
        Column("product_bundle_component_id", "INT", uniqueness=True),
        Column("bundle_product_id", "INT"),
        Column("component_product_id", "INT"),
        Column("price_contribution_percentage", "DOUBLE"),
        Column("is_free", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("datetime_deleted", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_modified",
)
