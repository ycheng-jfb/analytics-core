from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='rma',
    company_join_sql="""SELECT DISTINCT l.rma_id, ds.company_id
             from {database}.REFERENCE.dim_store ds
             join {database}.{schema}."ORDER" o
                 on ds.store_id=o.store_id
             join {database}.{source_schema}.rma l
                on o.order_id=l.order_id""",
    column_list=[
        Column('rma_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('rma_source_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('return_action_request_id', 'INT'),
        Column('return_reason_id', 'INT'),
        Column('reason_comment', 'VARCHAR(255)'),
        Column('contains_unidentified_items', 'INT'),
        Column('return_days', 'INT'),
        Column('suppress_email', 'INT'),
        Column('product_refund', 'NUMBER(19, 4)'),
        Column('tax_refund', 'NUMBER(19, 4)'),
        Column('shipping_refund', 'NUMBER(19, 4)'),
        Column('billing_refund', 'NUMBER(19, 4)'),
        Column('date_added', 'TIMESTAMP_NTZ(0)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_resolved', 'TIMESTAMP_NTZ(3)'),
        Column('date_return_due', 'TIMESTAMP_NTZ(3)'),
        Column('date_expires', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_pickup_expected', 'TIMESTAMP_NTZ(3)'),
    ],
    watermark_column='datetime_modified',
)
