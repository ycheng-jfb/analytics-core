from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='case_flag_type',
    company_join_sql="""
     SELECT DISTINCT
         L.CASE_FLAG_TYPE_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.case_flag_type AS L
     ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column('case_flag_type_id', 'INT', uniqueness=True),
        Column('parent_case_flag_type_id', 'INT', key=True),
        Column('store_group_id', 'INT'),
        Column('label', 'VARCHAR(500)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('statuscode', 'INT'),
        Column('sort', 'INT'),
        Column('instruction_html', 'VARCHAR'),
    ],
    watermark_column='datetime_modified',
)
