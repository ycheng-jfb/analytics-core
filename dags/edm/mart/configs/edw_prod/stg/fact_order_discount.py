from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator
from include.utils.snowflake import ForeignKey


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table='fact_order_discount',
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_fact_order_discount.sql'],
        use_surrogate_key=False,
        column_list=[
            Column('order_discount_id', 'NUMBER(38,0)', uniqueness=True),
            Column('meta_original_order_discount_id', 'NUMBER(38,0)'),
            Column('order_id', 'NUMBER(38,0)', foreign_key=ForeignKey('fact_order')),
            Column(
                'promo_history_key', 'NUMBER(38,0)', foreign_key=ForeignKey('dim_promo_history')
            ),
            Column('promo_id', 'NUMBER(38,0)'),
            Column('discount_id', 'NUMBER(38,0)', foreign_key=ForeignKey('dim_discount')),
            Column('discount_type_id', 'NUMBER(38,0)', foreign_key=ForeignKey('dim_discount_type')),
            Column('order_discount_applied_to', 'VARCHAR(15)'),
            Column('order_discount_local_amount', 'NUMBER(19,4)'),
            Column('is_deleted', 'BOOLEAN'),
        ],
        watermark_tables=[
            'lake_consolidated.ultra_merchant.order_discount',
            'edw_prod.stg.dim_promo_history',
            'edw_prod.stg.fact_order',
        ],
    )
