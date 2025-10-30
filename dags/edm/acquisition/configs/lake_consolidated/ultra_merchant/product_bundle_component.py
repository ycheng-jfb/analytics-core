from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='product_bundle_component',
    company_join_sql="""
      SELECT DISTINCT
         L.product_bundle_component_id,
         DS.company_id
         from
             {database}.REFERENCE.dim_store ds
            join {database}.{schema}.product p
                on ds.store_group_id=p.store_group_id
            join {database}.{source_schema}.product_bundle_component L
                         on L.bundle_product_id=p.product_id""",
    column_list=[
        Column('product_bundle_component_id', 'INT', uniqueness=True, key=True),
        Column('bundle_product_id', 'INT', key=True),
        Column('component_product_id', 'INT', key=True),
        Column('price_contribution_percentage', 'DOUBLE'),
        Column('is_free', 'INT'),
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
