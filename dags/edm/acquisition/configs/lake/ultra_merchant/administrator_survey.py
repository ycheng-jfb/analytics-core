from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='administrator_survey',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('administrator_survey_id', 'INT', uniqueness=True),
        Column('case_administrator_id', 'INT'),
        Column('review_by_administrator_id', 'INT'),
        Column('survey_id', 'INT'),
        Column('case_id', 'INT'),
        Column('score', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_completed', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
