from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='carrier_billing',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('carrier_billing_id', 'INT', uniqueness=True),
        Column('carrier_id', 'INT'),
        Column('account_number', 'VARCHAR(75)'),
        Column('invoice_number', 'VARCHAR(75)'),
        Column('invoice_datetime', 'TIMESTAMP_NTZ(3)'),
        Column('tracking_number', 'VARCHAR(75)'),
        Column('invoice_box_id', 'INT'),
        Column('carrier_weight', 'DOUBLE'),
        Column('carrier_postage', 'NUMBER(19, 4)'),
        Column('carrier_surcharge', 'NUMBER(19, 4)'),
        Column('carrier_discount', 'NUMBER(19, 4)'),
        Column('carrier_transportation', 'NUMBER(19, 4)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
