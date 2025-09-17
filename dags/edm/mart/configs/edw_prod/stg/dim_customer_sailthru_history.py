from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_scd import SnowflakeMartSCDOperator


def get_mart_operator():
    return SnowflakeMartSCDOperator(
        table="dim_customer_sailthru_history",
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_customer_sailthru_history.sql"],
        use_surrogate_key=True,
        column_list=[
            Column("customer_id", "INT", uniqueness=True, type_2=True),
            Column("meta_original_customer_id", "INT"),
            Column("store_id", "INT", type_2=True),
            Column("is_test_customer", "BOOLEAN"),
            Column("is_opt_out", "BOOLEAN", type_2=True),
            Column("is_deleted", "BOOLEAN", type_2=True),
        ],
        watermark_tables=[
            "lake_consolidated.ultra_merchant_history.customer",
            "lake_view.sailthru.data_exporter_profile",
        ],
    )
