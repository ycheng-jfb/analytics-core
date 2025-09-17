from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_scd import SnowflakeMartSCDOperator


def get_mart_operator():
    return SnowflakeMartSCDOperator(
        table="dim_product_price_history",
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_product_price_history.sql"],
        use_surrogate_key=True,
        column_list=[
            Column("product_id", "INT", uniqueness=True, type_2=True),
            Column("master_product_id", "INT", type_2=True),
            Column("meta_original_product_id", "INT", type_2=True),
            Column("meta_original_master_product_id", "INT", type_2=True),
            Column("store_id", "INT", type_2=True),
            Column("vip_unit_price", "NUMBER(19,4)", type_2=True),
            Column("retail_unit_price", "NUMBER(19,4)", type_2=True),
            Column("warehouse_unit_price", "NUMBER(19,4)", type_2=True),
            Column("sale_price", "NUMBER(19,4)", type_2=True),
            Column("is_deleted", "BOOLEAN", type_2=True),
        ],
        watermark_tables=[
            "lake_consolidated.ultra_merchant_history.product",
            "lake_consolidated.ultra_merchant_history.pricing_option",
        ],
    )
