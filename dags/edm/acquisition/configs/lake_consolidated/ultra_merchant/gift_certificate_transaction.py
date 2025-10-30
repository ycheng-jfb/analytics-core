from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='gift_certificate_transaction',
    company_join_sql="""
        SELECT DISTINCT
            L.gift_certificate_transaction_id,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.GIFT_CERTIFICATE AS GC
            ON DS.STORE_GROUP_ID = GC.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.gift_certificate_transaction AS L
            ON L.GIFT_CERTIFICATE_ID=GC.GIFT_CERTIFICATE_ID""",
    column_list=[
        Column('gift_certificate_transaction_id', 'INT', uniqueness=True, key=True),
        Column('gift_certificate_id', 'INT', key=True),
        Column('gift_certificate_transaction_type_id', 'INT'),
        Column('gift_certificate_transaction_reason_id', 'INT'),
        Column('object', 'VARCHAR(50)'),
        Column('object_id', 'INT', key=True),
        Column('amount', 'DECIMAL(19,4)'),
        Column('balance', 'DECIMAL(19,4)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_transaction', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_local_transaction', 'TIMESTAMP_NTZ(3)'),
    ],
    watermark_column='datetime_modified',
)
