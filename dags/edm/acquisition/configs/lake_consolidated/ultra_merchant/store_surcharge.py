from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='store_surcharge',
    company_join_sql="""
            SELECT DISTINCT
                L.store_surcharge_id,
                ds.company_id
            FROM {database}.reference.dim_store AS ds
            INNER JOIN {database}.{source_schema}.store_surcharge AS L
            ON L.store_id=DS.store_id """,
    column_list=[
        Column('store_surcharge_id', 'INT', uniqueness=True),
        Column('store_id', 'INT'),
        Column('type', 'VARCHAR(25)'),
        Column('current_test_key', 'VARCHAR(50)'),
        Column('current_test_value_range_start', 'INT'),
        Column('rate', 'FLOAT'),
        Column('active', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
    ],
    watermark_column='datetime_modified',
)
