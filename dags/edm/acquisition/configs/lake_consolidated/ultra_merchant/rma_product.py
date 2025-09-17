from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='rma_product',
    company_join_sql="""SELECT DISTINCT l.rma_product_id, ds.company_id
              from {database}.REFERENCE.dim_store ds
             join {database}.{schema}."ORDER" o
                 on ds.store_id=o.store_id
             join {database}.{schema}.rma rma
                on o.order_id=rma.order_id
             JOIN {database}.{source_schema}.rma_product l
                ON l.rma_id = rma.rma_id""",
    column_list=[
        Column('rma_product_id', 'INT', uniqueness=True, key=True),
        Column('rma_id', 'INT', key=True),
        Column('order_line_id', 'INT', key=True),
        Column('product_id', 'INT', key=True),
        Column('item_id', 'INT'),
        Column('return_action_id', 'INT'),
        Column('expected_return_condition_id', 'INT'),
        Column('restocking_fee_id', 'INT'),
        Column('return_reason_id', 'INT'),
        Column('reason_comment', 'VARCHAR(255)'),
        Column('lpn_code', 'VARCHAR(25)'),
        Column('amount_paid', 'NUMBER(19, 4)'),
        Column('refund_amount', 'NUMBER(19, 4)'),
        Column('store_credit_amount', 'NUMBER(19, 4)'),
        Column('restocking_fee_amount', 'NUMBER(19, 4)'),
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
