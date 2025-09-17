from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.OBJECT_COLUMN_NULL_AND_NAME_VALUE,
    table='customer_quiz_detail',
    company_join_sql="""
      SELECT DISTINCT
          L.customer_quiz_detail_id,
          DS.COMPANY_ID
      FROM {database}.REFERENCE.DIM_STORE AS DS
      INNER JOIN {database}.{schema}.Session  AS S
      ON DS.STORE_ID = S.STORE_ID
      INNER JOIN {database}.{schema}.CUSTOMER_QUIZ AS CQ
      ON S.SESSION_ID = CQ.SESSION_ID
      INNER JOIN {database}.{source_schema}.customer_quiz_detail AS L
      ON CQ.CUSTOMER_QUIZ_ID = L.CUSTOMER_QUIZ_ID """,
    column_list=[
        Column('customer_quiz_detail_id', 'INT', uniqueness=True, key=True),
        Column('customer_quiz_id', 'INT', key=True),
        Column('object', 'VARCHAR(255)'),
        Column('object_id', 'INT'),
        Column('name', 'VARCHAR(50)'),
        Column('value', 'VARCHAR(255)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('quiz_question_id', 'INT', key=True),
    ],
    watermark_column='datetime_modified',
)
