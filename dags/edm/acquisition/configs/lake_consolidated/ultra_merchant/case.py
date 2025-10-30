from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='case',
    company_join_sql="""
     SELECT DISTINCT
         L.CASE_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.case AS L
     ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column('case_id', 'INT', uniqueness=True, key=True),
        Column('code', 'VARCHAR(8)'),
        Column('foreign_case_number', 'VARCHAR(75)'),
        Column('case_source_id', 'INT'),
        Column('case_source_location_id', 'INT'),
        Column('store_group_id', 'INT'),
        Column('latest_case_assignment_id', 'INT'),
        Column('assigned_administrator_id', 'INT'),
        Column('closed_administrator_id', 'INT'),
        Column('created_administrator_id', 'INT'),
        Column('date_added', 'TIMESTAMP_NTZ(3)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_closed', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_wrapup', 'TIMESTAMP_NTZ(3)'),
        Column('qa_statuscode', 'INT'),
        Column('qa_customer_statuscode', 'INT'),
        Column('statuscode', 'INT'),
        Column('customer_search_object', 'VARCHAR(255)'),
        Column('customer_search_object_id', 'VARCHAR(255)'),
        Column('linked_case_id', 'INT', key=True),
    ],
    watermark_column='datetime_modified',
)
