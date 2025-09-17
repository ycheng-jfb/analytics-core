from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator
from include.utils.snowflake import ForeignKey


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table='fact_activation_loyalty_tier',
        use_surrogate_key=True,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_fact_activation_loyalty_tier.sql'],
        column_list=[
            Column('customer_id', 'NUMBER(38,0)', uniqueness=True),
            Column('meta_original_customer_id', 'NUMBER(38,0)'),
            Column('membership_event_key', 'NUMBER(38,0)'),
            Column('activation_key', 'NUMBER(38,0)'),
            Column('store_id', 'NUMBER(38,0)', foreign_key=ForeignKey('dim_store')),
            Column('order_id', 'NUMBER(38,0)', foreign_key=ForeignKey('fact_order')),
            Column('session_id', 'NUMBER(38,0)'),
            Column('membership_event_loyalty_tier_type', 'VARCHAR(50)'),
            Column('membership_type_detail', 'VARCHAR(20)'),
            Column('membership_reward_tier', 'VARCHAR(50)'),
            Column('event_start_local_datetime', 'TIMESTAMP_TZ(3)', uniqueness=True),
            Column('event_end_local_datetime', 'TIMESTAMP_TZ(3)'),
            Column('recent_activation_local_datetime', 'TIMESTAMP_TZ(3)'),
            Column('membership_tier_points', 'NUMBER(38,0)'),
            Column('required_points_earned', 'NUMBER(38,0)'),
            Column('membership_reward_tier_id', 'NUMBER(38,0)'),
            Column('is_current', 'BOOLEAN'),
            Column('is_deleted', 'BOOLEAN'),
            Column('is_test_customer', 'BOOLEAN'),
        ],
        watermark_tables=[
            'edw_prod.stg.fact_membership_event',
            'edw_prod.stg.fact_activation',
            'lake_consolidated.ultra_merchant.membership',
            'lake_consolidated.ultra_merchant.membership_reward_signal',
            'lake_consolidated.ultra_merchant_history.membership_reward_tier',
        ],
    )
