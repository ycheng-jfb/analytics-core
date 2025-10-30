from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='store_credit',
    company_join_sql="""
            SELECT DISTINCT
                L.store_credit_id,
                ds.company_id
            FROM {database}.reference.dim_store AS ds
            INNER JOIN {database}.{schema}.customer AS c
            ON ds.store_id = c.store_id
            INNER JOIN {database}.{source_schema}.store_credit AS L
            ON L.customer_id=c.customer_id """,
    column_list=[
        Column('store_credit_id', 'INT', uniqueness=True, key=True),
        Column('customer_id', 'INT', key=True),
        Column('administrator_id', 'INT'),
        Column('store_credit_reason_id', 'INT'),
        Column('reason_comment', 'VARCHAR(255)'),
        Column('code', 'VARCHAR(25)'),
        Column('amount', 'NUMBER(19, 4)'),
        Column('balance', 'NUMBER(19, 4)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('date_expires', 'TIMESTAMP_NTZ(0)'),
        Column('statuscode', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('currency_code', 'VARCHAR(3)'),
    ],
    watermark_column='datetime_modified',
)
