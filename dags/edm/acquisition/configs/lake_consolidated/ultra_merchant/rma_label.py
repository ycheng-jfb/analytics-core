from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='rma_label',
    company_join_sql="""SELECT DISTINCT l.rma_label_id, ds.company_id
    from {database}.REFERENCE.dim_store ds
        join {database}.{schema}."ORDER" o
              on ds.store_id=o.store_id
        join {database}.{schema}.rma r
                 on r.order_id=o.order_id
        join {database}.{source_schema}.rma_label l
         on l.rma_id=r.rma_id""",
    column_list=[
        Column('rma_label_id', 'INT', uniqueness=True, key=True),
        Column('rma_id', 'INT', key=True),
        Column('rma_label_provider_id', 'INT'),
        Column('tracking_number', 'VARCHAR(50)'),
        Column('tracking_url', 'VARCHAR(255)'),
        Column('carrier_tracking_number', 'VARCHAR(50)'),
        Column('image_url', 'VARCHAR(255)'),
        Column('image_data', 'VARCHAR'),
        Column('carrier', 'VARCHAR(25)'),
        Column('billed_weight', 'DOUBLE'),
        Column('zone', 'INT'),
        Column('base_cost', 'NUMBER(19, 4)'),
        Column('total_fees', 'NUMBER(19, 4)'),
        Column('fuel_surcharge', 'NUMBER(19, 4)'),
        Column('total_cost', 'NUMBER(19, 4)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_first_scanned', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_last_scanned', 'TIMESTAMP_NTZ(3)'),
        Column('date_delivery_expected', 'TIMESTAMP_NTZ(0)'),
        Column('date_delivered', 'TIMESTAMP_NTZ(0)'),
        Column('statuscode', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('alternate_tracking_number', 'VARCHAR(50)'),
    ],
    watermark_column='datetime_modified',
)
