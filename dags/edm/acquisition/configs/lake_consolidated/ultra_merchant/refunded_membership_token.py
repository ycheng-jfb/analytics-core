from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='refunded_membership_token',
    company_join_sql="""
    SELECT DISTINCT
        L.refunded_membership_token_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}."ORDER" AS O
        ON O.STORE_ID = DS.STORE_ID
    INNER JOIN {database}.{schema}.REFUND AS R
        ON R.ORDER_ID = O.ORDER_ID
    INNER JOIN {database}.{source_schema}.refunded_membership_token AS L
        ON L.REFUND_ID = R.REFUND_ID""",
    column_list=[
        Column('refunded_membership_token_id', 'INT', uniqueness=True, key=True),
        Column('refund_id', 'INT', key=True),
        Column('membership_token_id', 'INT', key=True),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
    ],
    watermark_column='datetime_added',
)
