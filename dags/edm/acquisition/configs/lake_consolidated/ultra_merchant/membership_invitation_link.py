from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_invitation_link',
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_INVITATION_LINK_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_invitation_link AS L
            ON L.MEMBERSHIP_PLAN_ID = M.MEMBERSHIP_PLAN_ID """,
    column_list=[
        Column('membership_invitation_link_id', 'INT', uniqueness=True, key=True),
        Column('membership_plan_id', 'INT', key=True),
        Column('membership_id', 'INT', key=True),
        Column('code', 'VARCHAR(25)'),
        Column('label', 'VARCHAR(50)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('active', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
