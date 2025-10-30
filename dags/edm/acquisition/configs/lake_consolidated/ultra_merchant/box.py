from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='box',
    company_join_sql="""
      SELECT DISTINCT
          L.BOX_ID,
          DS.COMPANY_ID
      FROM {database}.REFERENCE.DIM_STORE AS DS
      INNER JOIN {database}.{schema}.MEMBERSHIP_PLAN AS MP
          ON DS.STORE_GROUP_ID = MP.STORE_GROUP_ID
           INNER JOIN {database}.{schema}.MEMBERSHIP AS M
          ON MP.MEMBERSHIP_PLAN_ID = M.MEMBERSHIP_PLAN_ID
      INNER JOIN {database}.{source_schema}.box AS L
          ON L.MEMBERSHIP_ID = M.MEMBERSHIP_ID """,
    column_list=[
        Column('box_id', 'INT', uniqueness=True, key=True),
        Column('box_program_id', 'INT'),
        Column('box_subscription_id', 'INT'),
        Column('membership_id', 'INT', key=True),
        Column('box_type_id', 'INT'),
        Column('stylist_id', 'INT', key=True),
        Column('order_id', 'INT', key=True),
        Column('cart_id', 'INT', key=True),
        Column('rma_id', 'INT', key=True),
        Column('return_id', 'INT', key=True),
        Column('apply_fee', 'INT'),
        Column('fee_amount', 'NUMBER(19, 4)'),
        Column('items_liked', 'INT'),
        Column('items_sent', 'INT'),
        Column('items_kept', 'INT'),
        Column('score', 'DOUBLE'),
        Column('error_message', 'VARCHAR(255)'),
        Column('preview_sent', 'INT'),
        Column('preview_viewed', 'INT'),
        Column('preview_expired', 'INT'),
        Column('checkout_expired', 'INT'),
        Column('rma_label_scan_expired', 'INT'),
        Column('date_scheduled', 'TIMESTAMP_NTZ(0)'),
        Column('date_delivered', 'TIMESTAMP_NTZ(0)'),
        Column('date_checkout_due', 'TIMESTAMP_NTZ(0)'),
        Column('date_rma_label_scan_due', 'TIMESTAMP_NTZ(0)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_preview_expires', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_previewed', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_checkout', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_completed', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_cancelled', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column('preview_counter', 'INT'),
        Column('finalize_counter', 'INT'),
        Column('original_date_checkout_due', 'TIMESTAMP_NTZ(0)'),
        Column('disable_auto_billing', 'INT'),
        Column('keep_discount_id', 'INT', key=True),
        Column('datetime_released_to_queue', 'TIMESTAMP_NTZ(3)'),
        Column('is_autobox', 'INT'),
    ],
    watermark_column='datetime_modified',
)
