from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table='fact_order_credit',
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_fact_order_credit.sql'],
        use_surrogate_key=False,
        column_list=[
            Column('order_id', 'NUMBER(38,0)', uniqueness=True),
            Column('meta_original_order_id', 'NUMBER(38,0)'),
            Column('credit_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('noncash_credit_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('cash_credit_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('billed_cash_credit_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('refund_cash_credit_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('other_cash_credit_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('billed_cash_credit_redeemed_same_month_local_amount', 'NUMBER(19,4)'),
            Column('billed_cash_credit_redeemed_equivalent_count', 'NUMBER(19,4)'),
            Column('billed_cash_credit_sub_credit_redeemed_local_amount', 'NUMBER(19,4)'),
            Column(
                'billed_cash_credit_sub_converted_to_token_redeemed_local_amount', 'NUMBER(38, 10)'
            ),
            Column(
                'billed_cash_credit_sub_converted_to_variable_redeemed_local_amount',
                'NUMBER(19,4)',
            ),
            Column('billed_cash_credit_sub_giftco_roundtrip_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('billed_cash_credit_sub_token_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('other_cash_credit_sub_direct_gift_card_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('other_cash_credit_sub_gift_card_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('other_cash_credit_sub_legacy_credit_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('other_cash_credit_sub_other_credit_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('refund_cash_credit_sub_credit_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('refund_cash_credit_sub_token_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('noncash_credit_sub_credit_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('noncash_credit_sub_direct_gift_card_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('noncash_credit_sub_gift_card_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('noncash_credit_sub_token_redeemed_local_amount', 'NUMBER(19,4)'),
            Column('is_deleted', 'BOOLEAN'),
            Column('is_test_customer', 'BOOLEAN'),
            Column('is_test_order', 'BOOLEAN'),
        ],
        watermark_tables=[
            'edw_prod.stg.dim_credit',
            'edw_prod.stg.fact_order',
            'lake_consolidated.ultra_merchant.store_credit',
            'lake_consolidated.ultra_merchant.gift_certificate',
            'lake_consolidated.ultra_merchant.order_credit',
            'lake_consolidated.ultra_merchant_history.membership',
        ],
    )
