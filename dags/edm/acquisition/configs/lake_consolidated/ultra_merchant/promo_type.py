from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="promo_type",
    column_list=[
        Column("promo_type_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(100)"),
        Column("processing_priority", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("show_cms", "INT"),
        Column("cms_label", "VARCHAR(50)"),
        Column("cms_description", "VARCHAR(500)"),
        Column("cms_icon", "VARCHAR(255)"),
        Column("cms_example", "VARCHAR(500)"),
    ],
    watermark_column="datetime_added",
)
