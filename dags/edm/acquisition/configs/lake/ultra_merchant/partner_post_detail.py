from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='partner_post_detail',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('partner_post_detail_id', 'INT', uniqueness=True),
        Column('partner_post_id', 'INT'),
        Column('partner_campaign_id', 'INT'),
        Column('record', 'VARCHAR(8000)'),
        Column('record_public', 'VARCHAR(8000)'),
        Column('statuscode', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('code', 'VARCHAR(20)'),
    ],
)
