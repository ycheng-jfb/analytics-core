from include.airflow.operators.snowflake_mart_base import Column, KeyLookupJoin
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator
from include.utils.snowflake import ForeignKey


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table="fact_customer_action",
        use_surrogate_key=True,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_fact_customer_action.sql"],
        column_list=[
            Column("customer_id", "INT", uniqueness=True),
            Column("meta_original_customer_id", "INT"),
            Column(
                "customer_action_type_key",
                "NUMBER(38,0)",
                lookup_table="dim_customer_action_type",
                lookup_table_key="customer_action_type_key",
                key_lookup_list=[
                    KeyLookupJoin(
                        stg_column="customer_action_type", stg_column_type="VARCHAR(50)"
                    )
                ],
            ),
            Column(
                "order_id", "INT", foreign_key=ForeignKey("fact_order"), uniqueness=True
            ),
            Column("meta_original_order_id", "INT"),
            Column("store_id", "INT", foreign_key=ForeignKey("dim_store")),
            Column(
                "customer_action_local_datetime", "TIMESTAMP_TZ(3)", uniqueness=True
            ),
            Column("customer_action_period_date", "DATE"),
            Column("customer_action_period_local_date", "DATE"),
            Column("event_count", "INT"),
        ],
        watermark_tables=[
            "edw_prod.stg.dim_membership_event_type",
            "edw_prod.stg.fact_membership_event",
            'lake_consolidated.ultra_merchant."ORDER"',
            "lake_consolidated.ultra_merchant.order_classification",
            "lake_consolidated.ultra_merchant.membership",
            "lake_consolidated.ultra_merchant.membership_period",
            "lake_consolidated.ultra_merchant.membership_skip",
            "lake_consolidated.ultra_merchant.period",
            "lake_consolidated.ultra_merchant.session",
            "lake_consolidated.ultra_merchant.customer",
        ],
    )
