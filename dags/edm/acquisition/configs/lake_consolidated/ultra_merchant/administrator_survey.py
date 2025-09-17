from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='administrator_survey',
    column_list=[
        Column('administrator_survey_id', 'INT', uniqueness=True),
        Column('case_administrator_id', 'INT'),
        Column('review_by_administrator_id', 'INT'),
        Column('survey_id', 'INT'),
        Column('case_id', 'INT'),
        Column('score', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_completed', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
