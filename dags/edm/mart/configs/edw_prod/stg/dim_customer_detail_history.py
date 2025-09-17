from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_scd import SnowflakeMartSCDOperator


def get_mart_operator():
    return SnowflakeMartSCDOperator(
        table="dim_customer_detail_history",
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_customer_detail_history.sql"],
        use_surrogate_key=True,
        column_list=[
            Column("customer_id", "INT", uniqueness=True, type_2=True),
            Column("meta_original_customer_id", "INT"),
            Column("membership_id", "INT"),
            Column("membership_event_type", "VARCHAR(50)", type_2=True),
            Column("membership_type_detail", "VARCHAR(20)", type_2=True),
            Column("membership_type", "VARCHAR(100)", type_2=True),
            Column("membership_plan_id", "INT", type_2=True),
            Column("membership_price", "NUMBER(19,4)", type_2=True),
            Column("is_test_customer", "BOOLEAN"),
            Column("is_opt_out", "BOOLEAN", type_2=True),
            Column("is_sms_opt_out", "BOOLEAN", type_2=True),
            Column("is_deleted", "BOOLEAN", type_2=True),
        ],
        watermark_tables=[
            "lake_consolidated.ultra_merchant_history.customer",
            "lake_consolidated.ultra_merchant_history.membership",
            "lake_consolidated.ultra_merchant.membership_type",
            "edw_prod.stg.dim_customer_sailthru_history",
            "edw_prod.stg.fact_membership_event",
            "edw_prod.reference.iterable_subscription_log",
            "edw_prod.reference.test_customer",
            "lake_history.emarsys.email_subscribes",
            "lake.media.attentive_attentive_sms_legacy",
        ],
    )
