from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_profile',
    watermark_column='datetime_modified',
    column_list=[
        Column('membership_profile_id', 'INT', uniqueness=True),
        Column('membership_id', 'INT'),
        Column('customer_quiz_id', 'INT'),
        Column('primary_personality_type_tag_id', 'INT'),
        Column('name', 'VARCHAR(100)'),
        Column('age', 'INT'),
        Column('gender', 'VARCHAR(10)'),
        Column('is_current', 'INT'),
        Column('enable_autoship', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('active', 'INT'),
        Column('date_birthday', 'DATE'),
    ],
)
