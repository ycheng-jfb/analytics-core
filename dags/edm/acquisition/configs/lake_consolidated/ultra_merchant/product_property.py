from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.VALUE_COLUMN,
    table='product_property',
    company_join_sql="""
        SELECT DISTINCT
            L.product_property_id,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.PRODUCT AS P
            ON DS.STORE_GROUP_ID = P.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.product_property AS L
            ON L.PRODUCT_ID = P.PRODUCT_ID """,
    column_list=[
        Column('product_property_id', 'INT', uniqueness=True, key=True),
        Column('product_id', 'INT', key=True),
        Column('product_property_type_id', 'INT', key=True),
        Column('label', 'VARCHAR(50)'),
        Column('value', 'VARCHAR(350)'),
        Column('sort', 'INT'),
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
