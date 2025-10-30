from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='product_property_type',
    company_join_sql="""
    SELECT DISTINCT
        L.product_property_type_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{source_schema}.product_property_type AS L
        ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column('product_property_type_id', 'INT', uniqueness=True, key=True),
        Column('store_group_id', 'INT'),
        Column('code', 'VARCHAR(50)'),
        Column('label', 'VARCHAR(100)'),
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
