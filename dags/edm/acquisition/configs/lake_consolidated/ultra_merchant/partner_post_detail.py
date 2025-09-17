from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='partner_post_detail',
    company_join_sql="""SELECT DISTINCT l.partner_post_detail_id,
       ds.company_id
    FROM {database}.reference.dim_store AS ds
    JOIN {database}.{schema}.partner_campaign AS pc
        ON pc.store_id = ds.store_id
    JOIN {database}.{schema}.partner_post AS pp
        ON pc.partner_id = pp.partner_id
    JOIN {database}.{schema}.partner_post_detail AS l
        ON l.partner_post_id = pp.partner_post_id""",
    column_list=[
        Column('partner_post_detail_id', 'INT', uniqueness=True, key=True),
        Column('partner_post_id', 'INT', key=True),
        Column('partner_campaign_id', 'INT', key=True),
        Column('record', 'VARCHAR(8000)'),
        Column('record_public', 'VARCHAR(8000)'),
        Column('statuscode', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('code', 'VARCHAR(20)'),
    ],
    watermark_column='datetime_modified',
)
