from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_profile_detail',
    watermark_column='datetime_modified',
    column_list=[
        Column('membership_profile_detail_id', 'INT', uniqueness=True),
        Column('membership_profile_id', 'INT'),
        Column('name', 'VARCHAR(50)'),
        Column('value', 'VARCHAR(255)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
