SET low_watermark_ltz = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _order AS
SELECT
    *
FROM
    lake.tiktok_shop.orders o
    WHERE meta_update_datetime > $low_watermark_ltz;

CREATE OR REPLACE TEMP TABLE _line_items AS
SELECT
    id AS order_id,
    array_agg(c.value:id) WITHIN GROUP(ORDER BY NULL) AS order_item_ids,
    array_agg(c.value:sku_id) WITHIN GROUP(ORDER BY NULL) AS sku_ids,
    array_agg(c.value:seller_sku) WITHIN GROUP(ORDER BY NULL) AS seller_skus,
    array_agg(c.value:product_id) WITHIN GROUP(ORDER BY NULL) AS product_ids,
    array_agg(c.value:product_name) WITHIN GROUP(ORDER BY NULL) AS product_names,
    array_agg(c.value:sku_name) WITHIN GROUP(ORDER BY NULL) AS variations,
    count(*) AS quantity
from _order o,
    LATERAL FLATTEN(INPUT => o.LINE_ITEMS) c
group by id;


CREATE OR REPLACE TEMP TABLE _order_final AS
SELECT
    shop_name                                                               shop_name,
    id                                                                      order_id,
    status                                                                  order_status,
    CASE
    WHEN cancellation_initiator IS NOT NULL AND status = 'CANCELLED'
        THEN 'Cancel'
    WHEN cancellation_initiator IS NOT NULL AND status = 'COMPLETED'
        THEN 'Return/Refund'
    ELSE NULL
    END                                                                     cancellation_or_return_type,
    CASE
    WHEN cancellation_initiator IS NOT NULL AND
        (status = 'CANCELLED' OR status = 'COMPLETED') THEN 1
    ELSE 0
    END                                                                     sku_quantity_of_return,
    payment:currency::VARCHAR                                               currency,
    payment:original_shipping_fee                                           original_shipping_fee,
    payment:original_total_product_price                                    original_total_product_price,
    payment:platform_discount                                               platform_discount,
    COALESCE(payment:product_tax, 0)                                        product_tax,
    payment:seller_discount                                                 seller_discount,
    payment:shipping_fee                                                    shipping_fee,
    payment:shipping_fee_platform_discount                                  shipping_fee_platform_discount,
    payment:shipping_fee_seller_discount                                    shipping_fee_seller_discount,
    COALESCE(payment:shipping_fee_tax, 0)                                   shipping_fee_tax,
    payment:sub_total                                                       sub_total,
    payment:tax                                                             tax,
    payment:total_amount                                                    total_amount,
    CASE
    WHEN cancellation_initiator IS NOT NULL THEN payment:total_amount
    ELSE NULL END                                                           order_refund_amount,
    CONVERT_TIMEZONE('UTC', 'US/Pacific', to_timestamp(create_time))        created_time_pst,
    CONVERT_TIMEZONE('UTC', 'US/Pacific', to_timestamp(paid_time))          paid_time_pst,
    CONVERT_TIMEZONE('UTC', 'US/Pacific', to_timestamp(rts_time))           rts_time_pst,
    CONVERT_TIMEZONE('UTC', 'US/Pacific', to_timestamp(shipping_due_time))  shipped_time_pst,
    CONVERT_TIMEZONE('UTC', 'US/Pacific', to_timestamp(delivery_time))      delivered_time_pst,
    CONVERT_TIMEZONE('UTC', 'US/Pacific', to_timestamp(cancel_time))        cancelled_time_pst,
    cancellation_initiator                                                  cancel_by,
    cancel_reason                                                           cancel_reason,
    fulfillment_type                                                        fulfillment_type,
    tracking_number                                                         tracking_id,
    delivery_option_name                                                    delivery_option_type,
    shipping_provider                                                       shipping_provider_name,
    buyer_message                                                           buyer_message,
    user_id,
    is_on_hold_order,
    is_replacement_order,
    is_sample_order::boolean                                                is_sample_order,
    recipient_address:name                                                  recipient,
    recipient_address:phone_number                                          phone_number,
    recipient_address:district_info[0]:address_name                         country,
    recipient_address:district_info[1]:address_name                         state,
    recipient_address:district_info[3]:address_name                         city,
    recipient_address:postal_code                                           zipcode,
    recipient_address:address_line1                                         address_line1,
    recipient_address:address_line2                                         address_line2,
    recipient_address:delivery_preferences:drop_off_location                delivery_instruction,
    payment_method_name                                                     payment_method,
    packages[0]:id                                                          package_id,
    seller_note                                                             seller_note,
    recipient_address:full_address                                          shipping_information,
    order_item_ids,
    sku_ids,
    seller_skus,
    product_ids,
    product_names,
    variations,
    quantity
FROM _order o
    join _line_items li on li.order_id=o.id;


MERGE INTO lake.tiktok_shop.order_report AS t
    USING (SELECT *,
                  HASH(*)             meta_row_hash,
                  CURRENT_TIMESTAMP() meta_create_datetime,
                  CURRENT_TIMESTAMP() meta_update_datetime
           FROM _order_final) s
    ON EQUAL_NULL(t.order_id, s.order_id)
        AND equal_null(t.shop_name, s.shop_name)
    WHEN NOT MATCHED
        THEN INSERT (shop_name, order_id, order_status, cancellation_or_return_type, sku_quantity_of_return, currency,
                     original_shipping_fee, original_total_product_price, platform_discount, product_tax,
                     seller_discount, shipping_fee, shipping_fee_platform_discount, shipping_fee_seller_discount,
                     shipping_fee_tax, sub_total, tax, total_amount, order_refund_amount, created_time_pst,
                     paid_time_pst, rts_time_pst, shipped_time_pst, delivered_time_pst, cancelled_time_pst, cancel_by,
                     cancel_reason, fulfillment_type, tracking_id, delivery_option_type, shipping_provider_name,
                     buyer_message, user_id, is_on_hold_order, is_replacement_order,is_sample_order,recipient,
                     phone_number, country, state, city, zipcode, address_line1,
                     address_line2, delivery_instruction, payment_method, package_id, seller_note, shipping_information,
                     order_item_ids, sku_ids, seller_skus, product_ids, product_names, variations, quantity,
                     meta_row_hash, meta_create_datetime, meta_update_datetime)
        VALUES (shop_name, order_id, order_status, cancellation_or_return_type, sku_quantity_of_return, currency,
                original_shipping_fee, original_total_product_price, platform_discount, product_tax, seller_discount,
                shipping_fee, shipping_fee_platform_discount, shipping_fee_seller_discount, shipping_fee_tax, sub_total,
                tax, total_amount, order_refund_amount, created_time_pst, paid_time_pst, rts_time_pst, shipped_time_pst,
                delivered_time_pst, cancelled_time_pst, cancel_by, cancel_reason, fulfillment_type, tracking_id,
                delivery_option_type, shipping_provider_name, buyer_message, user_id, is_on_hold_order,
                is_replacement_order,is_sample_order, recipient, phone_number, country, state,
                city, zipcode, address_line1, address_line2, delivery_instruction, payment_method, package_id,
                seller_note, shipping_information, order_item_ids, sku_ids, seller_skus, product_ids, product_names,
                variations, quantity, meta_row_hash, meta_create_datetime, meta_update_datetime)
    WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
        THEN UPDATE
        SET
            t.order_status = s.order_status,
            t.cancellation_or_return_type = s.cancellation_or_return_type,
            t.sku_quantity_of_return = s.sku_quantity_of_return,
            t.currency = s.currency,
            t.original_shipping_fee = s.original_shipping_fee,
            t.original_total_product_price = s.original_total_product_price,
            t.platform_discount = s.platform_discount,
            t.product_tax = s.product_tax,
            t.seller_discount = s.seller_discount,
            t.shipping_fee = s.shipping_fee,
            t.shipping_fee_platform_discount = s.shipping_fee_platform_discount,
            t.shipping_fee_seller_discount = s.shipping_fee_seller_discount,
            t.shipping_fee_tax = s.shipping_fee_tax,
            t.sub_total = s.sub_total,
            t.tax = s.tax,
            t.total_amount = s.total_amount,
            t.order_refund_amount = s.order_refund_amount,
            t.created_time_pst = s.created_time_pst,
            t.paid_time_pst = s.paid_time_pst,
            t.rts_time_pst = s.rts_time_pst,
            t.shipped_time_pst = s.shipped_time_pst,
            t.delivered_time_pst = s.delivered_time_pst,
            t.cancelled_time_pst = s.cancelled_time_pst,
            t.cancel_by = s.cancel_by,
            t.cancel_reason = s.cancel_reason,
            t.fulfillment_type = s.fulfillment_type,
            t.tracking_id = s.tracking_id,
            t.delivery_option_type = s.delivery_option_type,
            t.shipping_provider_name = s.shipping_provider_name,
            t.buyer_message = s.buyer_message,
            t.user_id = s.user_id,
            t.is_on_hold_order = s.is_on_hold_order,
            t.is_replacement_order = s.is_replacement_order,
            t.is_sample_order = s.is_sample_order,
            t.recipient = s.recipient,
            t.phone_number = s.phone_number,
            t.country = s.country,
            t.state = s.state,
            t.city = s.city,
            t.zipcode = s.zipcode,
            t.address_line1 = s.address_line1,
            t.address_line2 = s.address_line2,
            t.delivery_instruction = s.delivery_instruction,
            t.payment_method = s.payment_method,
            t.package_id = s.package_id,
            t.seller_note = s.seller_note,
            t.shipping_information = s.shipping_information,
            t.order_item_ids = s.order_item_ids,
            t.sku_ids = s.sku_ids,
            t.seller_skus = s.seller_skus,
            t.product_ids = s.product_ids,
            t.product_names = s.product_names,
            t.variations = s.variations,
            t.quantity = s.quantity,
            t.meta_row_hash = s.meta_row_hash,
            t.meta_update_datetime = s.meta_update_datetime;

