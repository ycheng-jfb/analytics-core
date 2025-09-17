from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table='gift_order',
    company_join_sql="""
        SELECT DISTINCT
            L.GIFT_ORDER_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{source_schema}.gift_order AS L
            ON L.ORDER_ID = O.ORDER_ID """,
    column_list=[
        Column('gift_order_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('sender_customer_id', 'INT', key=True),
        Column('sender_name', 'VARCHAR(50)'),
        Column('recipient_email', 'VARCHAR(75)'),
        Column('recipient_name', 'VARCHAR(50)'),
        Column('recipient_customer_id', 'INT', key=True),
        Column('membership_trial_id', 'INT', key=True),
        Column('gift_message', 'VARCHAR(16777216)'),
        Column('has_gift_message', 'INT'),
        Column('allow_trial', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
    ],
    watermark_column='datetime_added',
)
