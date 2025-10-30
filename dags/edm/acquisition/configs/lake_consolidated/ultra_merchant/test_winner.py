from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='test_winner',
    company_join_sql="""
        SELECT DISTINCT
            L.TEST_WINNER_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.TEST_METADATA AS TM
            ON DS.STORE_GROUP_ID = TM.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.test_winner AS L
            ON TM.TEST_METADATA_ID = L.TEST_METADATA_ID """,
    column_list=[
        Column('test_winner_id', 'INT', uniqueness=True, key=True),
        Column('test_metadata_id', 'INT', key=True),
        Column('variant_number', 'INT'),
        Column('statuscode', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
