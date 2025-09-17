from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='bounceback_endowment',
    company_join_sql="""
            SELECT DISTINCT
            L.bounceback_endowment_id,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.CUSTOMER AS C
            ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.bounceback_endowment AS L
            ON L.CUSTOMER_ID = C.CUSTOMER_ID
    """,
    column_list=[
        Column('bounceback_endowment_id', 'INT', uniqueness=True, key=True),
        Column('customer_id', 'INT', key=True),
        Column('bounceback_type_id', 'INT'),
        Column('order_id', 'INT', key=True),
        Column('order_line_id', 'INT', key=True),
        Column('membership_token_id', 'INT', key=True),
        Column('administrator_id', 'INT'),
        Column('amount', 'DECIMAL(19, 4)'),
        Column('object', 'VARCHAR(50)'),
        Column('object_id', 'INT', key=True),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
    ],
    watermark_column='datetime_modified',
)
