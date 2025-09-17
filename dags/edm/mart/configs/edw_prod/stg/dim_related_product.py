from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table="dim_related_product",
        use_surrogate_key=True,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_related_product.sql"],
        column_list=[
            Column("master_product_id", "INT", uniqueness=True),
            Column("meta_original_master_product_id", "INT"),
            Column("master_store_group_id", "INT"),
            Column("store_group_id", "INT"),
            Column("product_type_id", "INT"),
            Column("default_store_id", "INT"),
            Column("default_warehouse_id", "INT"),
            Column("default_product_category_id", "INT"),
            Column("alias", "VARCHAR(100)"),
            Column("group_code", "VARCHAR(100)"),
            Column("label", "VARCHAR(100)"),
            Column("related_group_code", "VARCHAR(150)"),
            Column("date_expected", "TIMESTAMP_NTZ"),
            Column("is_deleted", "BOOLEAN"),
            Column("company_id", "INT"),
        ],
        watermark_tables=[
            "lake_consolidated.ultra_merchant.product",
            "lake_consolidated.ultra_merchant.store_group",
        ],
    )
