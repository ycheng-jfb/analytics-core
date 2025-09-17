from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_reward_transaction',
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_REWARD_TRANSACTION_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS C
            ON DS.STORE_ID = C.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_reward_transaction AS L
            ON L.MEMBERSHIP_ID = C.MEMBERSHIP_ID """,
    column_list=[
        Column('membership_reward_transaction_id', 'INT', uniqueness=True, key=True),
        Column('membership_id', 'INT', key=True),
        Column('membership_reward_transaction_type_id', 'INT'),
        Column('membership_reward_plan_id', 'INT', key=True),
        Column('administrator_id', 'INT'),
        Column('object_id', 'INT'),
        Column('points', 'INT'),
        Column('comment', 'VARCHAR(512)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('statuscode', 'INT'),
        Column('date_expires', 'TIMESTAMP_NTZ(0)'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('builder_external_model_id', 'NUMBER(38,0)'),
    ],
    watermark_column='datetime_modified',
)
