CREATE OR REPLACE VIEW lake_view.ultra_merchant."ORDER" COPY GRANTS AS
SELECT
    order_id,
    store_id,
    customer_id,
    order_source_id,
    order_tracking_id,
    shipping_address_id,
    billing_address_id,
    payment_method,
    auth_payment_transaction_id,
    capture_payment_transaction_id,
    master_order_id,
    session_id,
    payment_option_id,
    reserved_id,
    code,
    subtotal,
    shipping,
    tax,
    discount,
    credit,
    estimated_weight,
    ip,
    date_added,
    datetime_added,
    date_placed,
    datetime_processing_modified,
    datetime_payment_modified,
    date_shipped,
    processing_statuscode,
    payment_statuscode,
    datetime_modified,
    store_domain_id,
    membership_level_id,
    datetime_transaction,
    datetime_local_transaction,
    currency_code,
    store_group_id,
    membership_brand_id,
    datetime_local_shipped,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant."ORDER" s
WHERE NOT exists(
        SELECT
            1
        FROM lake.ultra_merchant.order_delete_log d
        WHERE d.order_id = s.order_id
    );
