from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='return_batch',
    company_join_sql="""
    SELECT DISTINCT
        L.return_batch_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{source_schema}.return_batch AS L
        ON L.STORE_ID = DS.STORE_ID""",
    column_list=[
        Column('return_batch_id', 'INT', uniqueness=True, key=True),
        Column('label', 'VARCHAR(255)'),
        Column('warehouse_id', 'INT'),
        Column('store_id', 'INT'),
        Column('administrator_id_created', 'INT'),
        Column('administrator_id_approved', 'INT'),
        Column('statuscode', 'INT'),
        Column('date_added', 'TIMESTAMP_NTZ(3)'),
        Column('date_approved', 'TIMESTAMP_NTZ(3)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
