from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator
from include.utils.snowflake import ForeignKey


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table='fact_order_product_cost',
        use_surrogate_key=False,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_fact_order_product_cost.sql'],
        column_list=[
            Column('order_id', 'INT', uniqueness=True),
            Column('meta_original_order_id', 'INT'),
            Column('store_id', 'INT', foreign_key=ForeignKey('dim_store')),
            Column('currency_key', 'INT'),
            Column('estimated_landed_cost_local_amount', 'NUMBER(19,4)'),
            Column('reporting_landed_cost_local_amount', 'NUMBER(38, 6)'),
            Column('is_actual_landed_cost', 'BOOLEAN'),
            Column('oracle_cost_local_amount', 'NUMBER(19,4)'),
            Column('lpn_po_cost_local_amount', 'NUMBER(19,4)'),
            Column('po_cost_local_amount', 'NUMBER(19,4)'),
            Column('misc_cogs_local_amount', 'NUMBER(19,4)'),
        ],
        watermark_tables=[
            'edw_prod.stg.fact_order_line',
            'edw_prod.stg.fact_order',
            'edw_prod.stg.fact_order_line_product_cost',
        ],
    )
