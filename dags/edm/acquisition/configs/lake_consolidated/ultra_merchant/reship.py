from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='reship',
    company_join_sql="""SELECT DISTINCT l.reship_id, ds.company_id
              from {database}.REFERENCE.dim_store ds
               join {database}.{schema}."ORDER" o
                    on ds.STORE_ID=o.STORE_ID
                join {database}.{source_schema}.reship l
                    on l.reship_order_id=o.order_id""",
    column_list=[
        Column('reship_id', 'INT', uniqueness=True, key=True),
        Column('original_order_id', 'INT', key=True),
        Column('reship_order_id', 'INT', key=True),
        Column('reship_reason_id', 'INT'),
        Column('reason_comment', 'VARCHAR(255)'),
        Column('date_added', 'TIMESTAMP_NTZ(0)'),
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
