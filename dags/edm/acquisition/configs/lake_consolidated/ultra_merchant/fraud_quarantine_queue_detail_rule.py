from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='fraud_quarantine_queue_detail_rule',
    company_join_sql="""
       SELECT DISTINCT
           L.FRAUD_QUARANTINE_QUEUE_DETAIL_RULE_ID,
           DS.COMPANY_ID
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}.CUSTOMER AS C
           ON C.STORE_GROUP_ID = DS.STORE_GROUP_ID
       INNER JOIN {database}.{schema}.FRAUD_QUARANTINE_QUEUE AS FQQ
           ON FQQ.CUSTOMER_ID = C.CUSTOMER_ID
       INNER JOIN {database}.{schema}.FRAUD_QUARANTINE_QUEUE_DETAIL AS FQQD
           ON FQQD.FRAUD_QUARANTINE_QUEUE_ID = FQQ.FRAUD_QUARANTINE_QUEUE_ID
       INNER JOIN {database}.{source_schema}.fraud_quarantine_queue_detail_rule AS L
           ON L.FRAUD_QUARANTINE_QUEUE_DETAIL_ID = FQQD.FRAUD_QUARANTINE_QUEUE_DETAIL_ID """,
    column_list=[
        Column('fraud_quarantine_queue_detail_rule_id', 'INT', uniqueness=True, key=True),
        Column('fraud_quarantine_queue_detail_id', 'INT', key=True),
        Column('fraud_rule_id', 'INT'),
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
