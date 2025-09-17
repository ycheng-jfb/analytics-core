from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator
from include.utils.snowflake import ForeignKey


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table="fact_chargeback",
        use_surrogate_key=False,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_fact_chargeback.sql"],
        column_list=[
            Column("order_id", "INT"),
            Column("meta_original_order_id", "INT"),
            Column("customer_id", "INT", foreign_key=ForeignKey("dim_customer")),
            Column("activation_key", "NUMBER(38,0)"),
            Column("first_activation_key", "NUMBER(38,0)"),
            Column(
                "store_id", "INT", foreign_key=ForeignKey("dim_store"), uniqueness=True
            ),
            Column("chargeback_status_key", "INT"),
            Column("chargeback_payment_key", "INT"),
            Column("chargeback_reason_key", "INT"),
            Column("first_chargeback_reason_key", "INT"),
            Column("chargeback_datetime", "TIMESTAMP_TZ(3)", uniqueness=True),
            Column("chargeback_date_eur_conversion_rate", "NUMBER(18,6)"),
            Column("chargeback_date_usd_conversion_rate", "NUMBER(18,6)"),
            Column("chargeback_local_amount", "NUMBER(19,4)"),
            Column("chargeback_payment_transaction_local_amount", "NUMBER(19,4)"),
            Column("chargeback_tax_local_amount", "NUMBER(19,4)"),
            Column("chargeback_vat_local_amount", "NUMBER(19,4)"),
            Column("effective_vat_rate", "NUMBER(18,6)"),
            Column("source", "VARCHAR"),
            Column("is_deleted", "BOOLEAN"),
            Column("is_test_customer", "BOOLEAN"),
            Column("source_order_id", "INT", uniqueness=True),
        ],
        watermark_tables=[
            "edw_prod.stg.dim_store",
            "edw_prod.stg.fact_activation",
            "edw_prod.stg.fact_order",
            "lake.oracle_ebs.chargeback_eu",
            "lake.oracle_ebs.chargeback_us",
            "lake_consolidated.ultra_merchant.order_detail",
            "lake_consolidated.ultra_merchant.order_classification",
            "edw_prod.reference.currency_exchange_rate",
            "edw_prod.reference.test_customer",
        ],
    )
