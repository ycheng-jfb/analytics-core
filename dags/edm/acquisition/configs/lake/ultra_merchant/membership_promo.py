from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_promo',
    watermark_column='datetime_modified',
    initial_load_value='2020-01-28 13:17:27.700',
    schema_version_prefix='v2',
    column_list=[
        Column('membership_promo_id', 'INT', uniqueness=True),
        Column('membership_id', 'INT'),
        Column('membership_promo_type_id', 'INT'),
        Column('promo_id', 'INT'),
        Column('membership_promo_trigger_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('date_start', 'TIMESTAMP_NTZ(0)'),
        Column('date_end', 'TIMESTAMP_NTZ(0)'),
        Column('statuscode', 'INT'),
        Column('allow_promos_of_same_type', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
