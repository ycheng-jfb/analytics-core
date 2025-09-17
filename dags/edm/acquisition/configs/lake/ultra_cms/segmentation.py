from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultracms',
    schema='dbo',
    table='segmentation',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('segmentation_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('label', 'VARCHAR(200)'),
        Column('statuscode', 'INT'),
        Column('member_choice_id', 'INT'),
        Column('registration_data_type', 'VARCHAR(50)'),
        Column('registration_date_min', 'DATE'),
        Column('registration_date_max', 'DATE'),
        Column('registration_days_min', 'INT'),
        Column('registration_days_max', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('gender', 'VARCHAR(10)'),
        Column('current_dm_gateway_code', 'VARCHAR(2048)'),
        Column('signup_dm_gateway_code', 'VARCHAR(2048)'),
        Column('exclude_member_choice_id', 'INT'),
        Column('registration_data_hours_min', 'INT'),
        Column('registration_data_hours_max', 'INT'),
        Column('registration_data_minutes_min', 'INT'),
        Column('registration_data_minutes_max', 'INT'),
    ],
)
