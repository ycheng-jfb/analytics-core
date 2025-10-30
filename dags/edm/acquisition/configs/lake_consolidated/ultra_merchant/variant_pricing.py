from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='variant_pricing',
    company_join_sql="""
        SELECT DISTINCT
            L.VARIANT_PRICING_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.PRODUCT AS P
            ON DS.STORE_GROUP_ID = P.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.variant_pricing AS L
            ON P.PRODUCT_ID = L.PRODUCT_ID """,
    column_list=[
        Column('variant_pricing_id', 'INT', uniqueness=True, key=True),
        Column('test_metadata_id', 'INT', key=True),
        Column('product_id', 'INT', key=True),
        Column('pricing_id', 'INT', key=True),
        Column('control_pricing_id', 'INT', key=True),
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
