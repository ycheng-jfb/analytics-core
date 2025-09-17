from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='credit_transfer_queue',
    company_join_sql="""
     SELECT DISTINCT
         L.CREDIT_TRANSFER_QUEUE_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.credit_transfer_queue AS L
     ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column('credit_transfer_queue_id', 'INT', uniqueness=True, key=True),
        Column('store_group_id', 'INT'),
        Column('store_credit_id', 'INT', key=True),
        Column('gift_certificate_id', 'INT', key=True),
        Column('credit_transfer_transaction_type_id', 'INT'),
        Column('error_message', 'VARCHAR(255)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_scheduled', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column('membership_token_id', 'INT', key=True),
    ],
    watermark_column='datetime_modified',
)
