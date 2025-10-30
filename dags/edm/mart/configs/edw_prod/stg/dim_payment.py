from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table='dim_payment',
        use_surrogate_key=True,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_dim_payment.sql'],
        column_list=[
            Column('payment_method', 'VARCHAR(55)', uniqueness=True),
            Column('raw_creditcard_type', 'VARCHAR(55)', uniqueness=True),
            Column('creditcard_type', 'VARCHAR(55)', uniqueness=True),
            Column('is_applepay', 'BOOLEAN'),
            Column('is_prepaid_creditcard', 'BOOLEAN', uniqueness=True),
            Column('is_mastercard_checkout', 'BOOLEAN', uniqueness=True),
            Column('is_visa_checkout', 'BOOLEAN', uniqueness=True),
            Column('funding_type', 'VARCHAR(25)', uniqueness=True),
            Column('prepaid_type', 'VARCHAR(25)', uniqueness=True),
            Column('card_product_type', 'VARCHAR(25)', uniqueness=True),
        ],
        watermark_tables=[
            'lake_consolidated.ultra_merchant."ORDER"',
            'lake_consolidated.ultra_merchant.creditcard',
            'lake_consolidated.ultra_merchant.psp',
            'lake_consolidated.ultra_merchant.payment_transaction_creditcard',
            'lake_consolidated.ultra_merchant.payment_transaction_creditcard_data',
            'lake_consolidated.ultra_merchant.payment_transaction_psp',
        ],
    )
