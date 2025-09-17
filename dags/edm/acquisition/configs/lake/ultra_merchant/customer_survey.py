from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='customer_survey',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('customer_survey_id', 'INT', uniqueness=True),
        Column('customer_id', 'INT'),
        Column('survey_id', 'INT'),
        Column('subscription_id', 'INT'),
        Column('session_id', 'INT'),
        Column('page_number', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_completed', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
