from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="store",
    column_list=[
        Column("store_id", "INT", uniqueness=True),
        Column("store_group_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("alias", "VARCHAR(50)"),
        Column("code", "VARCHAR(25)"),
        Column("http_base_url", "VARCHAR(128)"),
        Column("https_base_url", "VARCHAR(128)"),
        Column("support_url", "VARCHAR(128)"),
        Column("support_email", "VARCHAR(75)"),
        Column("support_phone", "VARCHAR(50)"),
        Column("support_phone_international", "VARCHAR(50)"),
        Column("support_hours", "VARCHAR(100)"),
        Column("receipt_email", "VARCHAR(75)"),
        Column("receipt_subject", "VARCHAR(128)"),
        Column("receipt_body_text", "VARCHAR"),
        Column("receipt_body_html", "VARCHAR"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("return_days", "INT"),
        Column("autoship_return_days", "INT"),
        Column("sample_product_group_id", "INT"),
        Column("sample_quantity", "INT"),
        Column("autoship_incentive_product_group_id", "INT"),
        Column("autoship_incentive_quantity", "INT"),
        Column("default_autoship_shipping_option_id", "INT"),
        Column("default_print_material_sequence_id", "INT"),
        Column("menu_id", "INT"),
        Column("erp_code", "VARCHAR(25)"),
        Column("resource_bundle_id", "INT"),
        Column("default_language_id", "INT"),
        Column("http_media_url", "VARCHAR(128)"),
        Column("https_media_url", "VARCHAR(128)"),
        Column("auto_login", "INT"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
