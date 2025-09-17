from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table='session_detail',
    table_type=TableType.NAME_VALUE_COLUMN,
    company_join_sql="""
        SELECT DISTINCT
            L.SESSION_DETAIL_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.SESSION AS S
            ON DS.STORE_ID = S.STORE_ID
        INNER JOIN {database}.{source_schema}.session_detail AS L
            ON L.SESSION_ID = S.SESSION_ID """,
    column_list=[
        Column('session_detail_hash_id', 'INT'),
        Column('session_detail_id', 'INT', uniqueness=True, key=True),
        Column('session_id', 'INT', key=True),
        Column('name', 'VARCHAR(50)'),
        Column('value', 'VARCHAR(4000)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('session_hash_id', 'INT'),
    ],
    watermark_column='repl_timestamp',
)
