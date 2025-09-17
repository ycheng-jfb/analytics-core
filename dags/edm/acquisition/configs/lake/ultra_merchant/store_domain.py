from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='store_domain',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('store_domain_id', 'INT', uniqueness=True),
        Column('store_id', 'INT'),
        Column('store_domain_type_id', 'INT'),
        Column('domain_name', 'VARCHAR(64)'),
        Column('base_domain', 'VARCHAR(64)'),
        Column('http_base_url', 'VARCHAR(128)'),
        Column('https_base_url', 'VARCHAR(128)'),
        Column('environment', 'VARCHAR(15)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('http_media_url', 'VARCHAR(128)'),
        Column('https_media_url', 'VARCHAR(128)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
