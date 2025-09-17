from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='fraud_customer',
    company_join_sql="""
        SELECT DISTINCT
            L.fraud_customer_id,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.CUSTOMER AS C
            ON DS.STORE_ID = C.STORE_ID
        INNER JOIN {database}.{source_schema}.fraud_customer AS L
            ON L.CUSTOMER_ID = C.CUSTOMER_ID """,
    column_list=[
        Column('fraud_customer_id', 'INT', uniqueness=True, key=True),
        Column('customer_id', 'INT', key=True),
        Column('fraud_customer_reason_id', 'INT'),
        Column('comment', 'VARCHAR(255)'),
        Column('administrator_id', 'INT'),
        Column('remove_comment', 'VARCHAR(255)'),
        Column('remove_administrator_id', 'INT'),
        Column('datetime_remove', 'TIMESTAMP_NTZ(3)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('statuscode', 'INT'),
    ],
    watermark_column='datetime_modified',
)
