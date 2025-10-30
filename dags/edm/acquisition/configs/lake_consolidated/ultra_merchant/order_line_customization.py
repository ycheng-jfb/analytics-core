from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='order_line_customization',
    company_join_sql="""
        SELECT DISTINCT
            L.order_line_customization_id,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.store_id = O.store_id
         INNER JOIN {database}.{source_schema}.order_line_customization AS L
         on L.order_id=O.order_id """,
    column_list=[
        Column('order_line_customization_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('order_line_id', 'INT', key=True),
        Column('customization_configuration_id', 'INT'),
        Column('error_message', 'VARCHAR(255)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('statuscode', 'INT'),
    ],
    watermark_column='datetime_modified',
)
