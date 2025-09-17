from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='partner_post',
    company_join_sql="""
    SELECT DISTINCT l.partner_post_id,
       ds.company_id
    FROM {database}.reference.dim_store AS ds
    JOIN {database}.{schema}.partner_campaign AS pc
        ON pc.store_id = ds.store_id
    JOIN {database}.{schema}.partner_post AS l
        ON pc.partner_id = l.partner_id""",
    column_list=[
        Column('partner_post_id', 'INT', uniqueness=True, key=True),
        Column('partner_id', 'INT', key=True),
        Column('partner_post_type_id', 'INT'),
        Column('label', 'VARCHAR(75)'),
        Column('record_format', 'VARCHAR(8000)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('store_group_id', 'INT'),
    ],
    watermark_column='datetime_modified',
)
