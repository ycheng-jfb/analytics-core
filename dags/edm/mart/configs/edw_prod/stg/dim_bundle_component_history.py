from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_scd import SnowflakeMartSCDOperator


def get_mart_operator():
    return SnowflakeMartSCDOperator(
        table='dim_bundle_component_history',
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_dim_bundle_component_history.sql'],
        use_surrogate_key=True,
        column_list=[
            Column('product_bundle_component_id', 'INT', uniqueness=True, type_2=True),
            Column('meta_original_product_bundle_component_id', 'INT', type_2=True),
            Column('bundle_product_id', 'INT', type_2=True),
            Column('bundle_component_product_id', 'INT', type_2=True),
            Column('bundle_name', 'VARCHAR(100)', type_2=True),
            Column('bundle_alias', 'VARCHAR(100)', type_2=True),
            Column('bundle_default_product_category', 'VARCHAR(100)', type_2=True),
            Column('bundle_component_default_product_category', 'VARCHAR(100)', type_2=True),
            Column('bundle_component_color', 'VARCHAR(500)', type_2=True),
            Column('bundle_component_name', 'VARCHAR(100)', type_2=True),
            Column('bundle_component_group_code', 'VARCHAR(50)', type_2=True),
            Column('bundle_price_contribution_percent', 'NUMBER(19,4)', type_2=True),
            Column('bundle_component_vip_unit_price', 'NUMBER(19,4)', type_2=True),
            Column('bundle_component_retail_unit_price', 'NUMBER(19,4)', type_2=True),
            Column('bundle_is_free', 'BOOLEAN', type_2=True),
            Column('is_deleted', 'BOOLEAN', type_2=True),
        ],
        watermark_tables=[
            'lake_consolidated.ultra_merchant_history.product_bundle_component',
            'lake_consolidated.ultra_merchant_history.product',
            'lake_consolidated.ultra_merchant_history.product_category',
            'lake_consolidated.ultra_merchant_history.pricing_option',
            'lake_consolidated.ultra_merchant.product_tag',
            'lake_consolidated.ultra_merchant.tag',
        ],
    )
