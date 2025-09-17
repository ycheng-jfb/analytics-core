from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='chargeback_vendor',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('chargeback_vendor_id', 'INT', uniqueness=True),
        Column('erp_db_name', 'VARCHAR(255)'),
        Column('interid', 'VARCHAR(255)'),
        Column('vendor_id', 'VARCHAR(255)'),
        Column('category_reference', 'VARCHAR(255)'),
        Column('vendor_name', 'VARCHAR(255)'),
        Column('vendor_contact_name', 'VARCHAR(255)'),
        Column('email_contact', 'VARCHAR(255)'),
        Column('brand', 'VARCHAR(255)'),
        Column('active_since', 'DATE'),
        Column('undeliverable_list', 'VARCHAR(255)'),
        Column('is_active', 'BOOLEAN'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
