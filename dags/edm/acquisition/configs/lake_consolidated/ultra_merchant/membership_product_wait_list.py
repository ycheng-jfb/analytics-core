from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_product_wait_list',
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_PRODUCT_WAIT_LIST_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS C
            ON DS.STORE_ID = C.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_product_wait_list AS L
            ON L.MEMBERSHIP_ID = C.MEMBERSHIP_ID """,
    column_list=[
        Column('membership_product_wait_list_id', 'INT', uniqueness=True, key=True),
        Column('membership_id', 'INT', key=True),
        Column('product_id', 'INT', key=True),
        Column('membership_product_wait_list_type_id', 'INT'),
        Column('action_attempts', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('date_expires', 'TIMESTAMP_NTZ(0)'),
        Column('datetime_last_action', 'TIMESTAMP_NTZ(3)'),
        Column('date_last_action', 'TIMESTAMP_NTZ(0)'),
        Column('statuscode', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('credit_allowed', 'BOOLEAN'),
    ],
    watermark_column='datetime_modified',
)
