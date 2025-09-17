from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table="dim_item_price",
        use_surrogate_key=True,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_item_price.sql"],
        column_list=[
            Column("product_id", "NUMBER(38,0)", uniqueness=True),
            Column("item_id", "NUMBER(38,0)", uniqueness=True),
            Column("master_product_id", "NUMBER(38,0)"),
            Column("store_id", "NUMBER(38,0)"),
            Column("sku", "VARCHAR(30)"),
            Column("product_sku", "VARCHAR(30)"),
            Column("base_sku", "VARCHAR(30)"),
            Column("product_name", "VARCHAR(100)"),
            Column("vip_unit_price", "NUMBER(19,4)"),
            Column("retail_unit_price", "NUMBER(19,4)"),
            Column("membership_brand_id", "NUMBER(38,0)"),
            Column("is_active", "BOOLEAN"),
            Column("meta_original_product_id", "NUMBER(38,0)"),
            Column("meta_original_item_id", "NUMBER(38,0)"),
        ],
        watermark_tables=[
            "lake_consolidated.ultra_merchant_history.product",
            "lake_consolidated.ultra_merchant_history.pricing_option",
            "edw_prod.stg.dim_product_price_history",
            "lake_consolidated.ultra_merchant.item",
        ],
    )
