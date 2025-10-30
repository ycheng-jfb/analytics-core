from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_invitation_link',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('membership_invitation_link_id', 'INT', uniqueness=True),
        Column('membership_plan_id', 'INT'),
        Column('membership_id', 'INT'),
        Column('code', 'VARCHAR(25)'),
        Column('label', 'VARCHAR(50)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('active', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
