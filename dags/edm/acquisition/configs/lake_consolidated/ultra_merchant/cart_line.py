from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="cart_line",
    company_join_sql="""
     SELECT DISTINCT
         L.CART_LINE_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.Session  AS S
     ON DS.STORE_ID = S.STORE_ID
     INNER JOIN {database}.{schema}.CART AS C
     ON S.SESSION_ID = C.SESSION_ID
      INNER JOIN {database}.{source_schema}.cart_line AS L
      ON L.CART_ID=C.CART_ID """,
    column_list=[
        Column("cart_line_id", "INT", uniqueness=True, key=True),
        Column("cart_id", "INT", key=True),
        Column("product_id", "INT", key=True),
        Column("offer_id", "INT"),
        Column("pricing_option_id", "INT", key=True),
        Column("product_type_id", "INT"),
        Column("retail_unit_price", "NUMBER(19, 4)"),
        Column("purchase_unit_price", "NUMBER(19, 4)"),
        Column("unit_price_adjustment", "NUMBER(19, 4)"),
        Column("shipping_price", "NUMBER(19, 4)"),
        Column("unit_discount", "NUMBER(19, 4)"),
        Column("quantity", "INT"),
        Column("extended_shipping_price", "NUMBER(19, 4)"),
        Column("extended_price", "NUMBER(19, 4)"),
        Column("autoship_frequency_id", "INT"),
        Column("autoship_quantity", "INT"),
        Column("is_multipay", "INT"),
        Column("group_key", "VARCHAR(50)"),
        Column("exclude_from_discounts", "INT"),
        Column("lpn_code", "VARCHAR(255)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("normal_unit_price", "NUMBER(19, 4)"),
        Column("tokens_applied", "INT"),
    ],
    watermark_column="datetime_modified",
)
