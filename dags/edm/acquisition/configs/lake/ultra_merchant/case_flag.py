from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='case_flag',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('case_flag_id', 'INT', uniqueness=True),
        Column('parent_case_flag_id', 'INT'),
        Column('case_flag_type_id', 'INT'),
        Column('label', 'VARCHAR(250)'),
        Column('instruction_html', 'VARCHAR'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('sort', 'INT'),
        Column('statuscode', 'INT'),
    ],
)
