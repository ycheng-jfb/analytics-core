from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    schema='ultra_cms',
    table='test_framework_data',
    company_join_sql="""
        SELECT DISTINCT
            L.test_famework_data_id,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.test_framework AS TF
        ON DS.STORE_GROUP_ID = TF.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.test_framework_data AS L
        ON TF.test_framework_id = L.test_framework_id """,
    column_list=[
        Column('test_famework_data_id', 'INT', uniqueness=True, key=True),
        Column('test_framework_id', 'INT', key=True),
        Column('object', 'VARCHAR(50)'),
        Column('value', 'VARCHAR(50)'),
        Column('comments', 'VARCHAR(250)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
    ],
    watermark_column='datetime_modified',
)
