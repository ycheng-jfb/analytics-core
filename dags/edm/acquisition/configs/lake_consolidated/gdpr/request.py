from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='request',
    schema='gdpr',
    company_join_sql="""
        SELECT DISTINCT
            L.request_id,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.ULTRA_MERCHANT.CUSTOMER AS C
            ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.request AS L
            ON L.CUSTOMER_ID = C.CUSTOMER_ID """,
    column_list=[
        Column('request_id', 'INT', uniqueness=True, key=True),
        Column('customer_id', 'INT', key=True),
        Column('region_id', 'INT'),
        Column('request_type_id', 'INT'),
        Column('request_source_id', 'INT'),
        Column('request_administrator_id', 'INT'),
        Column('approval_administrator_id', 'INT'),
        Column('legal_administrator_id', 'INT'),
        Column('confirmation_code', 'VARCHAR(15)'),
        Column('email', 'VARCHAR(100)'),
        Column('firstname', 'VARCHAR(25)'),
        Column('lastname', 'VARCHAR(25)'),
        Column('comment', 'VARCHAR'),
        Column('is_escalated', 'INT'),
        Column('credit_forfeit_accepted', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_manager_approval', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_legal_approval', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_completed', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_cancelled', 'TIMESTAMP_NTZ(3)'),
        Column('date_requested', 'TIMESTAMP_NTZ(0)'),
        Column('date_due', 'TIMESTAMP_NTZ(0)'),
        Column('date_warranty_expires', 'TIMESTAMP_NTZ(0)'),
        Column('statuscode', 'INT'),
    ],
    watermark_column='datetime_modified',
)
