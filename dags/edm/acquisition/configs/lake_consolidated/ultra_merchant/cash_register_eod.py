from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='cash_register_eod',
    company_join_sql="""
     SELECT DISTINCT
         L.CASH_REGISTER_EOD_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.cash_register_eod AS L
     ON DS.STORE_ID = L.STORE_ID """,
    column_list=[
        Column('cash_register_eod_id', 'INT', uniqueness=True, key=True),
        Column('store_id', 'INT'),
        Column('expected_amount', 'NUMBER(19, 4)'),
        Column('reconciled_amount', 'NUMBER(19, 4)'),
        Column('open_administrator_id', 'INT'),
        Column('first_count_amount', 'NUMBER(19, 4)'),
        Column('first_count_variance_amount', 'NUMBER(19, 4)'),
        Column('first_count_administrator_id', 'INT'),
        Column('first_count_cash_register_disposition_id', 'INT'),
        Column('second_count_amount', 'NUMBER(19, 4)'),
        Column('second_count_variance_amount', 'NUMBER(19, 4)'),
        Column('second_count_administrator_id', 'INT'),
        Column('second_count_cash_register_disposition_id', 'INT'),
        Column('close_count', 'INT'),
        Column('close_amount', 'NUMBER(19, 4)'),
        Column('close_variance_amount', 'NUMBER(19, 4)'),
        Column('close_administrator_id', 'INT'),
        Column('close_cash_register_disposition_id', 'INT'),
        Column('close_comment', 'VARCHAR(8000)'),
        Column('deposit_amount', 'NUMBER(19, 4)'),
        Column('datetime_opened', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_first_count', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_second_count', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_closed', 'TIMESTAMP_NTZ(3)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('statuscode', 'INT'),
    ],
    watermark_column='datetime_modified',
)
