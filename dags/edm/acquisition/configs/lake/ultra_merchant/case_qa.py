from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='case_qa',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('case_qa_id', 'INT', uniqueness=True),
        Column('case_id', 'INT'),
        Column('case_qa_scorecard_id', 'INT'),
        Column('case_qa_scorecard_total_score', 'INT'),
        Column('customer_survey_id', 'INT'),
        Column('customer_survey_total_score', 'INT'),
        Column('customer_survey_rep_total_score', 'INT'),
        Column('customer_survey_infrastructure_total_score', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
