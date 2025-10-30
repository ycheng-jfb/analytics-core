from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrarollup',
    schema='dbo',
    table='products_in_stock_spi_formula_ranking',
    watermark_column='rollup_datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('master_product_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('master_store_group_id', 'INT'),
        Column('product_type_id', 'INT'),
        Column('default_warehouse_id', 'INT'),
        Column('default_product_category_id', 'INT'),
        Column('group_code', 'VARCHAR(50)'),
        Column('spi_sales_1_day_formula_rank', 'INT'),
        Column('spi_sales_5_days_formula_rank', 'INT'),
        Column('spi_sales_10_days_formula_rank', 'INT'),
        Column('spi_conversions_1_day_formula_rank', 'INT'),
        Column('spi_conversions_5_days_formula_rank', 'INT'),
        Column('spi_conversions_10_days_formula_rank', 'INT'),
        Column('rollup_datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('rollup_datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('rollup_datetime_verified', 'TIMESTAMP_NTZ(3)'),
        Column('row_checksum', 'BINARY(32)'),
    ],
)
