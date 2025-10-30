from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table='dim_store',
        use_surrogate_key=False,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_dim_store.sql'],
        column_list=[
            Column('store_id', 'INT', uniqueness=True),
            Column('store_group_id', 'INT'),
            Column('store_group', 'varchar(50)'),
            Column('store_name', 'varchar(50)'),
            Column('store_name_region', 'varchar(50)'),
            Column('store_full_name', 'varchar(50)'),
            Column('store_brand', 'varchar(20)'),
            Column('store_brand_abbr', 'varchar(10)'),
            Column('store_type', 'varchar(20)'),
            Column('store_sub_type', 'varchar(50)'),
            Column('is_core_store', 'boolean'),
            Column('store_country', 'varchar(50)'),
            Column('store_region', 'varchar(32)'),
            Column('store_division', 'varchar(32)'),
            Column('store_division_abbr', 'varchar(10)'),
            Column('store_currency', 'varchar(10)'),
            Column('store_time_zone', 'varchar(100)'),
            Column('store_retail_state', 'varchar(50)'),
            Column('store_retail_city', 'varchar(50)'),
            Column('store_retail_location', 'varchar(50)'),
            Column('store_retail_zip_code', 'varchar(10)'),
            Column('store_retail_location_code', 'varchar(50)'),
            Column('store_retail_status', 'varchar(50)'),
            Column('store_retail_region', 'varchar(50)'),
            Column('store_retail_district', 'varchar(50)'),
            Column('company_id', 'INT'),
        ],
        watermark_tables=[
            'lake_consolidated.ultra_merchant.store',
            'lake_consolidated.ultra_merchant.store_type',
            'lake_consolidated.ultra_merchant.store_group',
            'lake.ultra_warehouse.retail_location',
            'lake_consolidated.ultra_merchant.store_classification',
            'lake_view.sharepoint.fl_retail_store_detail',
            'edw_prod.reference.specialty_store_ids',
        ],
    )
