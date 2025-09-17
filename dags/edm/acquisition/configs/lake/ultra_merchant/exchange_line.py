from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='exchange_line',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('exchange_line_id', 'INT', uniqueness=True),
        Column('exchange_id', 'INT'),
        Column('rma_product_id', 'INT'),
        Column('original_product_id', 'INT'),
        Column('exchange_product_id', 'INT'),
        Column('price_difference', 'NUMBER(19, 4)'),
        Column('price', 'NUMBER(19, 4)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
