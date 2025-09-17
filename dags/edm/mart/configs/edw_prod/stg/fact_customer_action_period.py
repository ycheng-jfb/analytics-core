from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator
from include.utils.snowflake import ForeignKey


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table='fact_customer_action_period',
        use_surrogate_key=False,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_fact_customer_action_period.sql'],
        column_list=[
            Column('customer_id', 'INT', uniqueness=True),
            Column('period_month_date', 'DATE', uniqueness=True),
            Column('period_name', 'VARCHAR'),
            Column('store_id', 'INT', foreign_key=ForeignKey('dim_store')),
            Column('store_name', 'VARCHAR'),
            Column('store_brand', 'VARCHAR'),
            Column('store_brand_abbr', 'VARCHAR'),
            Column('store_country', 'VARCHAR'),
            Column('store_region', 'VARCHAR'),
            Column('customer_action_category', 'VARCHAR'),
            Column('vip_cohort_month_date', 'DATE'),
        ],
        watermark_tables=[
            'edw_prod.stg.fact_membership_event',
            'edw_prod.stg.fact_customer_action',
        ],
    )
