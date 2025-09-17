from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator
from include.utils.snowflake import ForeignKey


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table="fact_refund_line",
        use_surrogate_key=False,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_fact_refund_line.sql"],
        column_list=[
            Column("refund_id", "NUMBER(38,0)"),
            Column("refund_line_id", "NUMBER(38,0)", uniqueness=True),
            Column("meta_original_refund_line_id", "NUMBER(38,0)"),
            Column("order_id", "NUMBER(38,0)", foreign_key=ForeignKey("fact_order")),
            Column(
                "order_line_id",
                "NUMBER(38,0)",
                foreign_key=ForeignKey("fact_order_line"),
            ),
            Column(
                "customer_id", "NUMBER(38,0)", foreign_key=ForeignKey("dim_customer")
            ),
            Column("activation_key", "NUMBER(38,0)"),
            Column("first_activation_key", "NUMBER(38,0)"),
            Column("store_id", "NUMBER(38,0)", foreign_key=ForeignKey("dim_store")),
            Column("refund_status_key", "NUMBER(38,0)"),
            Column("refund_payment_method_key", "NUMBER(38,0)"),
            Column("refund_request_local_datetime", "TIMESTAMP_TZ(3)"),
            Column("refund_completion_local_datetime", "TIMESTAMP_TZ(3)"),
            Column("refund_completion_date_usd_conversion_rate", "NUMBER(18,6)"),
            Column("refund_completion_date_eur_conversion_rate", "NUMBER(18,6)"),
            Column("effective_vat_rate", "NUMBER(18,6)"),
            Column("product_refund_local_amount", "NUMBER(19,4)"),
            Column("product_cash_refund_local_amount", "NUMBER(19,4)"),
            Column("product_store_credit_refund_local_amount", "NUMBER(19,4)"),
            Column("product_cash_store_credit_refund_local_amount", "NUMBER(19,4)"),
            Column("product_noncash_store_credit_refund_local_amount", "NUMBER(19,4)"),
            Column("product_unknown_store_credit_refund_local_amount", "NUMBER(19,4)"),
            Column("is_chargeback", "BOOLEAN"),
            Column("is_test_customer", "BOOLEAN"),
            Column("is_deleted", "BOOLEAN"),
            Column("source_order_id", "NUMBER(38,0)"),
            Column("source_order_line_id", "NUMBER(38,0)"),
        ],
        watermark_tables=[
            "lake_consolidated.ultra_merchant.refund_line",
            "lake_consolidated.ultra_merchant.refund",
            "edw_prod.stg.fact_refund",
            "edw_prod.stg.dim_refund_payment_method",
            "edw_prod.reference.test_customer",
            "lake_consolidated.ultra_merchant.order_line_split_map",
        ],
    )
