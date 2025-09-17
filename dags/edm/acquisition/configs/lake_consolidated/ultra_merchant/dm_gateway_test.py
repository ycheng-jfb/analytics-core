from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='dm_gateway_test',
    company_join_sql="""
     SELECT DISTINCT
         L.dm_gateway_test_id,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.dm_gateway AS dg
     ON DS.STORE_GROUP_ID = dg.STORE_GROUP_ID
     INNER JOIN {database}.{source_schema}.dm_gateway_test AS L
     ON L.dm_gateway_id= dg.dm_gateway_id """,
    column_list=[
        Column('dm_gateway_test_id', 'INT', uniqueness=True, key=True),
        Column('dm_gateway_id', 'INT', key=True),
        Column('label', 'VARCHAR(50)'),
        Column('description', 'VARCHAR'),
        Column('hypothesis', 'VARCHAR'),
        Column('results', 'VARCHAR'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_start', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_end', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
