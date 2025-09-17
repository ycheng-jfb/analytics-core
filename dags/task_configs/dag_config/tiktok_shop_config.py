from dataclasses import dataclass

from include.airflow.operators.snowflake_load import Column

inventory_column_list = [
    Column('shop_name', 'VARCHAR', uniqueness=True),
    Column("product_id", "VARCHAR", uniqueness=True),
    Column("skus", "VARIANT"),
]

order_column_list = [
    Column('shop_name', 'VARCHAR', uniqueness=True),
    Column('buyer_email', 'VARCHAR'),
    Column('buyer_message', 'VARCHAR'),
    Column('cancel_order_sla_time', 'INT'),
    Column('cancel_reason', 'VARCHAR'),
    Column('cancel_time', 'INT'),
    Column('cancellation_initiator', 'VARCHAR'),
    Column('collection_due_time', 'INT'),
    Column('collection_time', 'INT'),
    Column('cpf', 'VARCHAR'),
    Column('create_time', 'INT'),
    Column('delivery_due_time', 'INT'),
    Column('delivery_option_id', 'VARCHAR'),
    Column('delivery_option_name', 'VARCHAR'),
    Column('delivery_option_required_delivery_time', 'INT'),
    Column('delivery_sla_time', 'INT'),
    Column('delivery_time', 'INT'),
    Column('delivery_type', 'VARCHAR'),
    Column('fast_dispatch_sla_time', 'INT'),
    Column('fulfillment_type', 'VARCHAR'),
    Column('has_updated_recipient_address', 'BOOLEAN'),
    Column('id', 'VARCHAR', uniqueness=True),
    Column('is_buyer_request_cancel', 'BOOLEAN'),
    Column('is_cod', 'BOOLEAN'),
    Column('is_on_hold_order', 'BOOLEAN'),
    Column('is_replacement_order', 'BOOLEAN'),
    Column('is_sample_order', 'VARCHAR'),
    Column('line_items', 'VARIANT'),
    Column('need_upload_invoice', 'VARCHAR'),
    Column('packages', 'VARIANT'),
    Column('paid_time', 'INT'),
    Column('payment', 'VARIANT'),
    Column('payment_method_name', 'VARCHAR'),
    Column('pick_up_cut_off_time', 'INT'),
    Column('recipient_address', 'VARIANT'),
    Column('replaced_order_id', 'VARCHAR'),
    Column('request_cancel_time', 'INT'),
    Column('rts_sla_time', 'INT'),
    Column('rts_time', 'INT'),
    Column('seller_note', 'VARCHAR'),
    Column('shipping_due_time', 'INT'),
    Column('shipping_provider', 'VARCHAR'),
    Column('shipping_provider_id', 'VARCHAR'),
    Column('shipping_type', 'VARCHAR'),
    Column('split_or_combine_tag', 'VARCHAR'),
    Column('status', 'VARCHAR'),
    Column('tracking_number', 'VARCHAR'),
    Column('tts_sla_time', 'INT'),
    Column('update_time', 'INT'),
    Column('user_id', 'VARCHAR'),
    Column('warehouse_id', 'VARCHAR'),
]

product_column_list = [
    Column('shop_name', 'VARCHAR', uniqueness=True),
    Column('audit_failed_reasons', 'VARIANT'),
    Column('brand', 'VARIANT'),
    Column('category_chains', 'VARIANT'),
    Column('certifications', 'VARIANT'),
    Column('create_time', 'INT'),
    Column('delivery_options', 'VARIANT'),
    Column('description', 'VARCHAR'),
    Column('external_product_id', 'VARCHAR'),
    Column('id', 'VARCHAR', uniqueness=True),
    Column('is_cod_allowed', 'BOOLEAN'),
    Column('is_not_for_sale', 'BOOLEAN'),
    Column('main_images', 'VARIANT'),
    Column('manufacturer', 'VARIANT'),
    Column('package_dimensions', 'VARIANT'),
    Column('package_weight', 'VARIANT'),
    Column('product_attributes', 'VARIANT'),
    Column('product_types', 'VARCHAR'),
    Column('recommended_categories', 'VARIANT'),
    Column('size_chart', 'VARIANT'),
    Column('skus', 'VARIANT'),
    Column('status', 'VARCHAR'),
    Column('title', 'VARCHAR'),
    Column('update_time', 'INT'),
]

refund_events_column_list = [
    Column('shop_name', 'VARCHAR', uniqueness=True),
    Column('arbitration_status', 'VARCHAR'),
    Column('can_buyer_keep_item', 'BOOLEAN'),
    Column('combined_return_id', 'VARCHAR'),
    Column('create_time', 'INT'),
    Column('discount_amount', 'VARIANT'),
    Column('handover_method', 'VARCHAR'),
    Column('is_combined_return', 'VARCHAR'),
    Column('next_return_id', 'VARCHAR'),
    Column('order_id', 'VARCHAR'),
    Column('pre_return_id', 'VARCHAR'),
    Column('refund_amount', 'VARIANT'),
    Column('return_id', 'VARCHAR', uniqueness=True),
    Column('return_line_items', 'VARIANT'),
    Column('return_method', 'VARCHAR'),
    Column('return_provider_id', 'VARCHAR'),
    Column('return_provider_name', 'VARCHAR'),
    Column('return_reason', 'VARCHAR'),
    Column('return_reason_text', 'VARCHAR'),
    Column('return_shipping_document_type', 'VARCHAR'),
    Column('return_status', 'VARCHAR'),
    Column('return_tracking_number', 'VARCHAR'),
    Column('return_type', 'VARCHAR'),
    Column('role', 'VARCHAR'),
    Column('seller_next_action_response', 'VARIANT'),
    Column('shipment_type', 'VARCHAR'),
    Column('shipping_fee_amount', 'VARIANT'),
    Column('update_time', 'INT'),
]


@dataclass
class TikTokShopConfig:
    table: str
    endpoint: str
    response_obj: str
    column_list: list
    transform_procedure: str
    database: str = 'lake'
    staging_database: str = 'lake_stg'
    schema: str = 'tiktok_shop'


tiktok_shop_config_list = [
    TikTokShopConfig(
        "orders",
        "/order/202309/orders/search",
        "orders",
        order_column_list,
        "tiktok_shop.order_report.sql",
    ),
    TikTokShopConfig(
        "refund_events",
        "/return_refund/202309/returns/search",
        "return_orders",
        refund_events_column_list,
        "tiktok_shop.refund_events_report.sql",
    ),
]


@dataclass
class TikTokShopConnection:
    name: str
    conn_id: str


tiktok_shop_connection_list = [
    TikTokShopConnection('fabletics', 'fab_tiktok_shop'),
    TikTokShopConnection('yitty', 'yty_tiktok_shop'),
]
