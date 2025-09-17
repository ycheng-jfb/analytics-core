from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='refund_membership_token',
    company_join_sql="""
        SELECT DISTINCT
            L.REFUND_ID,
            L.MEMBERSHIP_TOKEN_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE DS
        INNER JOIN {database}.{schema}.MEMBERSHIP M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{schema}.MEMBERSHIP_TOKEN T
            ON T.MEMBERSHIP_ID = M.MEMBERSHIP_ID
        INNER JOIN {database}.{source_schema}.refund_membership_token L
            ON L.MEMBERSHIP_TOKEN_ID = T.MEMBERSHIP_TOKEN_ID""",
    column_list=[
        Column('refund_id', 'INT', uniqueness=True, key=True),
        Column('membership_token_id', 'INT', uniqueness=True, key=True),
        Column('order_line_id', 'INT', key=True),
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
