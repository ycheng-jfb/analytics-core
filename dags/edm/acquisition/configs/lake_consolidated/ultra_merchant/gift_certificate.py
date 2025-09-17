from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='gift_certificate',
    company_join_sql="""
      SELECT DISTINCT
          L.gift_certificate_id,
          DS.company_id
      FROM {database}.REFERENCE.DIM_STORE AS DS
      INNER JOIN {database}.{source_schema}.gift_certificate AS L
       ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column('gift_certificate_id', 'INT', uniqueness=True, key=True),
        Column('store_group_id', 'INT'),
        Column('gift_certificate_batch_id', 'INT', key=True),
        Column('cross_promo_id', 'INT', key=True),
        Column('membership_plan_id', 'INT', key=True),
        Column('order_id', 'INT', key=True),
        Column('customer_id', 'INT', key=True),
        Column('gift_certificate_type_id', 'INT'),
        Column('recipient_email', 'VARCHAR(75)'),
        Column('recipient_firstname', 'VARCHAR(25)'),
        Column('recipient_lastname', 'VARCHAR(25)'),
        Column('recipient_address_id', 'INT', key=True),
        Column('code', 'VARCHAR(25)'),
        Column('amount', 'NUMBER(19, 4)'),
        Column('balance', 'NUMBER(19, 4)'),
        Column('shipping_allowed', 'INT'),
        Column('single_use', 'INT'),
        Column('min_subtotal', 'NUMBER(19, 4)'),
        Column('comment', 'VARCHAR(255)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('date_expires', 'TIMESTAMP_NTZ(0)'),
        Column('statuscode', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('currency_code', 'VARCHAR(3)'),
    ],
    watermark_column='datetime_modified',
)
