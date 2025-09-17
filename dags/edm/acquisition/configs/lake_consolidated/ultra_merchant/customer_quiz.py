from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='customer_quiz',
    company_join_sql="""
      SELECT DISTINCT
          L.CUSTOMER_QUIZ_ID,
          COALESCE(DS1.COMPANY_ID, DS2.COMPANY_ID) AS COMPANY_ID
      FROM {database}.{source_schema}.customer_quiz AS L
      LEFT JOIN {database}.{schema}.session  AS S
        ON S.SESSION_ID = L.SESSION_ID
      LEFT JOIN {database}.REFERENCE.DIM_STORE AS DS1
        ON DS1.STORE_ID = S.STORE_ID
      LEFT JOIN {database}.{source_schema}.quiz AS Q
        ON Q.quiz_id = L.quiz_id
      LEFT JOIN {database}.REFERENCE.DIM_STORE AS DS2
        ON DS2.STORE_GROUP_ID = Q.STORE_GROUP_ID""",
    column_list=[
        Column('customer_quiz_id', 'INT', uniqueness=True, key=True),
        Column('session_id', 'INT', key=True),
        Column('quiz_id', 'INT', key=True),
        Column('customer_id', 'INT', key=True),
        Column('membership_id', 'INT', key=True),
        Column('page_number', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_completed', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
