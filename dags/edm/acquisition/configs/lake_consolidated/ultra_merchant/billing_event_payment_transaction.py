from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.REGULAR,
    table='billing_event_payment_transaction',
    company_join_sql="""
            SELECT DISTINCT
            l.billing_event_id,
            l.payment_transaction_id,
            DS.company_id
            FROM {database}.{source_schema}.billing_event_payment_transaction l
                 JOIN {database}.{schema}.payment_transaction_creditcard p
                      ON p.payment_transaction_id = l.payment_transaction_id
                 JOIN {database}.{schema}."ORDER" o
                      ON o.order_id = p.order_id
                 JOIN {database}.REFERENCE.DIM_STORE ds
                      ON o.store_id = ds.store_id""",
    column_list=[
        Column('billing_event_id', 'INT', uniqueness=True, key=True),
        Column('payment_transaction_id', 'INT', uniqueness=True, key=True),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_added',
)
