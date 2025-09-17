from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='request_job',
    schema='gdpr',
    company_join_sql="""
        SELECT DISTINCT
            L.request_job_id,
            L.customer_id,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.ULTRA_MERCHANT.CUSTOMER AS C
            ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.request_job AS L
            ON L.CUSTOMER_ID = C.CUSTOMER_ID
    """,
    column_list=[
        Column('CUSTOMER_ID', 'INT', uniqueness=True, key=True),
        Column('REQUEST_JOB_ID', 'INT', uniqueness=True, key=True),
        Column('REQUEST_ID', 'INT'),
        Column('SYSTEM_ID', 'INT'),
        Column('DATETIME_ADDED', 'TIMESTAMP_NTZ'),
        Column('STATUSCODE', 'INT'),
        Column('SNOWFLAKE_UPDATED', 'INT'),
        Column('PUSHED_TO_MSSQL', 'INT'),
    ],
    watermark_column="DATETIME_ADDED",
)
