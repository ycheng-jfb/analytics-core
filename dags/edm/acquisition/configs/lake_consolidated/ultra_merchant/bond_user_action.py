from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='bond_user_action',
    company_join_sql="""
    SELECT DISTINCT
         L.bond_user_action_id,
         DS.company_id
     FROM {database}.reference.dim_store AS DS
     INNER JOIN {database}.{source_schema}.case AS C
        ON DS.store_group_id = C.store_group_id
     INNER JOIN {database}.{source_schema}.bond_user_action AS L
        ON C.case_id = L.case_id
    """,
    column_list=[
        Column('bond_user_action_id', 'INT', uniqueness=True, key=True),
        Column('case_id', 'INT', key=True),
        Column('customer_id', 'INT', key=True),
        Column('administrator_id', 'INT'),
        Column('bond_user_action_type_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
    ],
    watermark_column='datetime_modified',
)
