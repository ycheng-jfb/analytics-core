from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='product_image_test_config_history',
    schema='ultra_rollup',
    company_join_sql="""
    SELECT DISTINCT
        L.product_image_test_config_history_id,
        L.product_image_test_config_id,
        L.master_product_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{source_schema}.products_in_stock_master AS PISM
        ON DS.STORE_GROUP_ID = PISM.MASTER_STORE_GROUP_ID
    INNER JOIN {database}.{source_schema}.product_image_test_config_history AS L
        ON PISM.MASTER_PRODUCT_ID = L.MASTER_PRODUCT_ID
    """,
    column_list=[
        Column('product_image_test_config_history_id', 'INT', uniqueness=True, key=True),
        Column('product_image_test_config_id', 'INT', uniqueness=True),
        Column('master_product_id', 'INT', uniqueness=True),
        Column('image_test_flag', 'BOOLEAN'),
        Column('image_test_config', 'VARCHAR(16777216)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
        Column('effective_start_date', 'TIMESTAMP_NTZ(3)'),
        Column('effective_end_date', 'TIMESTAMP_NTZ(3)'),
        Column('is_current_record', 'BOOLEAN'),
        Column('evaluation_batch_id', 'NUMBER(38, 0)'),
        Column('master_test_number', 'NUMBER(38, 0)'),
        Column('evaluation_message', 'VARCHAR(50)'),
        Column('extra_params_json', 'VARCHAR(16777216)'),
        Column('deleted_flag', 'BOOLEAN'),
        Column('record_checksum', 'NUMBER(38, 0)'),
    ],
    watermark_column='rollup_datetime_modified',
)
