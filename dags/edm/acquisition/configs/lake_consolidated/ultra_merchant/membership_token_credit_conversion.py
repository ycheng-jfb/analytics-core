from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_token_credit_conversion',
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_TOKEN_CREDIT_CONVERSION_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_token AS T
            ON T.MEMBERSHIP_ID = M.MEMBERSHIP_ID
        INNER JOIN {database}.{source_schema}.membership_token_credit_conversion AS L
            ON L.ORIGINAL_MEMBERSHIP_TOKEN_ID = T.MEMBERSHIP_TOKEN_ID """,
    column_list=[
        Column('membership_token_credit_conversion_id', 'INT', uniqueness=True, key=True),
        Column('original_membership_token_id', 'INT', key=True),
        Column('original_store_credit_id', 'INT', key=True),
        Column('converted_store_credit_id', 'INT', key=True),
        Column('store_credit_conversion_type_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('comment', 'VARCHAR(255)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
    ],
    watermark_column='datetime_modified',
)
