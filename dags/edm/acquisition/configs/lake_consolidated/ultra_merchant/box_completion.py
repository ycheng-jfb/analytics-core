from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='box_completion',
    company_join_sql="""
      SELECT DISTINCT
          L.BOX_COMPLETION_ID,
          DS.COMPANY_ID
      FROM {database}.REFERENCE.DIM_STORE AS DS
      INNER JOIN {database}.{schema}.MEMBERSHIP_PLAN AS MP
          ON DS.STORE_GROUP_ID = MP.STORE_GROUP_ID
           INNER JOIN {database}.{schema}.MEMBERSHIP AS M
          ON MP.MEMBERSHIP_PLAN_ID = M.MEMBERSHIP_PLAN_ID
      INNER JOIN {database}.{schema}.BOX AS B
          ON B.MEMBERSHIP_ID = M.MEMBERSHIP_ID
        INNER JOIN {database}.{source_schema}.box_completion AS L
          ON B.BOX_ID = L.BOX_ID """,
    column_list=[
        Column('box_completion_id', 'INT', uniqueness=True, key=True),
        Column('box_id', 'INT', key=True),
        Column('box_completion_disposition_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('disposition_comment', 'VARCHAR(255)'),
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
