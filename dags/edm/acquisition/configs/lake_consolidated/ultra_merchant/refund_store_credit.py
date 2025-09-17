from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='refund_store_credit',
    company_join_sql="""
    SELECT DISTINCT
        L.REFUND_ID,
        L.STORE_CREDIT_ID,
        DS.COMPANY_ID
    FROM {database}.REFERENCE.DIM_STORE DS
    INNER JOIN {database}.{schema}."ORDER" O
        ON DS.STORE_ID=O.STORE_ID
    INNER JOIN {database}.{schema}.REFUND R
        ON O.ORDER_ID=R.ORDER_ID
    INNER JOIN {database}.{source_schema}.refund_store_credit L
        ON L.REFUND_ID=R.REFUND_ID""",
    column_list=[
        Column('refund_id', 'INT', uniqueness=True, key=True),
        Column('store_credit_id', 'INT', uniqueness=True, key=True),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
    ],
    watermark_column='datetime_modified',
)
