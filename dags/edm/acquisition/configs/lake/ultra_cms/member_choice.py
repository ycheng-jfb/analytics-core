from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultracms',
    schema='dbo',
    table='member_choice',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('member_choice_id', 'INT', uniqueness=True),
        Column('member_choice', 'VARCHAR(50)'),
        Column('specific_member_option', 'VARCHAR(50)'),
        Column('specific_member_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
