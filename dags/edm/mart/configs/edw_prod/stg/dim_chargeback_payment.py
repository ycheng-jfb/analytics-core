from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table="dim_chargeback_payment",
        use_surrogate_key=True,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_chargeback_payment.sql"],
        column_list=[
            Column("chargeback_payment_processor", "VARCHAR(20)", uniqueness=True),
            Column("chargeback_payment_method", "VARCHAR(20)", uniqueness=True),
            Column("chargeback_payment_type", "VARCHAR(20)", uniqueness=True),
            Column("chargeback_payment_bank", "VARCHAR(1000)", uniqueness=True),
        ],
        watermark_tables=[
            "lake_consolidated.ultra_merchant.psp_notification_log",
            "lake_consolidated.ultra_merchant.payment_transaction_psp",
            "lake_consolidated.ultra_merchant.creditcard",
            "lake_consolidated.ultra_merchant.payment",
            "lake.oracle_ebs.chargeback_us",
            "lake.oracle_ebs.chargeback_eu",
        ],
    )
