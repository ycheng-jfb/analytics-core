from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_billing',
    company_join_sql="""
     SELECT DISTINCT
         L.membership_billing_id,
         DS.company_id
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.MEMBERSHIP_PLAN AS MP
        ON DS.STORE_ID = MP.STORE_ID
     INNER JOIN {database}.{source_schema}.membership_billing AS L
     ON L.MEMBERSHIP_PLAN_ID=MP.MEMBERSHIP_PLAN_ID """,
    column_list=[
        Column('membership_billing_id', 'INT', uniqueness=True, key=True),
        Column('membership_id', 'INT', key=True),
        Column('membership_plan_id', 'INT', key=True),
        Column('membership_type_id', 'INT'),
        Column('membership_billing_source_id', 'INT'),
        Column('membership_trial_id', 'INT', key=True),
        Column('activating_order_id', 'INT', key=True),
        Column('order_id', 'INT', key=True),
        Column('period_id', 'INT'),
        Column('period_type', 'VARCHAR(25)'),
        Column('error_message', 'VARCHAR(255)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_activated', 'TIMESTAMP_NTZ(3)'),
        Column('date_due', 'TIMESTAMP_NTZ(0)'),
        Column('date_period_start', 'TIMESTAMP_NTZ(0)'),
        Column('date_period_end', 'TIMESTAMP_NTZ(0)'),
        Column('statuscode', 'INT'),
    ],
    watermark_column='datetime_modified',
)
