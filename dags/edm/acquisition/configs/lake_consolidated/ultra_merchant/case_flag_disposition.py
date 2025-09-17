from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='case_flag_disposition',
    company_join_sql="""
     SELECT DISTINCT
         L.CASE_FLAG_DISPOSITION_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.case_flag_disposition AS L
     ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column('case_flag_disposition_id', 'INT', uniqueness=True),
        Column('case_disposition_type_id', 'INT'),
        Column('store_group_id', 'INT'),
        Column('label', 'VARCHAR(100)'),
        Column('statuscode', 'INT'),
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
