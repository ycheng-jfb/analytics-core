from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_period',
    company_join_sql="""
     SELECT DISTINCT
         L.membership_period_id,
         DS.company_id
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.membership AS M
      ON DS.STORE_ID= M.STORE_ID
     INNER JOIN {database}.{source_schema}.membership_period AS L
     ON L.MEMBERSHIP_ID=M.MEMBERSHIP_ID""",
    column_list=[
        Column('membership_period_id', 'INT', uniqueness=True, key=True),
        Column('membership_id', 'INT', key=True),
        Column('period_id', 'INT'),
        Column('credit_order_id', 'INT', key=True),
        Column('error_message', 'VARCHAR(255)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('date_due', 'TIMESTAMP_NTZ(0)'),
        Column('statuscode', 'INT'),
        Column('membership_plan_id', 'INT', key=True),
        Column('priority', 'INT'),
        Column('datetime_activated', 'TIMESTAMP_NTZ(3)'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('date_start', 'TIMESTAMP_NTZ(0)'),
        Column('date_end', 'TIMESTAMP_NTZ(0)'),
    ],
    watermark_column='datetime_modified',
)
