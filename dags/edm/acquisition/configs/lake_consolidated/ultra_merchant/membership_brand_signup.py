from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_brand_signup',
    company_join_sql="""
     SELECT DISTINCT
         L.membership_brand_signup_id,
         DS.company_id
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.membership AS M
      ON DS.STORE_ID= M.STORE_ID
     INNER JOIN {database}.{source_schema}.membership_brand_signup AS L
     ON L.MEMBERSHIP_ID=M.MEMBERSHIP_ID""",
    column_list=[
        Column('membership_brand_signup_id', 'INT', uniqueness=True, key=True),
        Column('membership_brand_id', 'INT'),
        Column('membership_id', 'INT', key=True),
        Column('membership_signup_id', 'INT', key=True),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_signup', 'TIMESTAMP_NTZ(3)'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
