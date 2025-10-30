from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_detail',
    table_type=TableType.NAME_VALUE_COLUMN,
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_DETAIL_ID,
            L.MEMBERSHIP_DETAIL_HASH_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_detail AS L
            ON L.MEMBERSHIP_ID = M.MEMBERSHIP_ID """,
    column_list=[
        Column('membership_detail_hash_id', 'INT', uniqueness=True),
        Column('membership_detail_id', 'INT', uniqueness=True, key=True),
        Column('membership_id', 'INT', key=True),
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
    ],
    watermark_column='datetime_modified',
)
