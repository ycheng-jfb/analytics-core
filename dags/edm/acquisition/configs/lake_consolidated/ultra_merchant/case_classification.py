from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.REGULAR,
    table='case_classification',
    company_join_sql="""
    SELECT DISTINCT
        L.CASE_CLASSIFICATION_ID,
        DS.COMPANY_ID
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}.CUSTOMER AS C
        ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
    INNER JOIN {database}.{source_schema}.case_customer AS CC
        ON CC.CUSTOMER_ID = C.CUSTOMER_ID
    INNER JOIN {database}.{source_schema}.case_classification AS L
        ON L.CASE_ID = CC.CASE_ID""",
    column_list=[
        Column('case_classification_id', 'INT', uniqueness=True, key=True),
        Column('case_id', 'INT', key=True),
        Column('case_flag_id', 'INT'),
        Column('case_flag_type_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('comment', 'VARCHAR(100)'),
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
