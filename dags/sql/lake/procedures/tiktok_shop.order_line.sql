SET low_watermark_ltz = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _order_line AS
SELECT
    *
FROM
    lake.tiktok_shop.orders o
    WHERE meta_update_datetime > $low_watermark_ltz;

CREATE OR REPLACE TEMP TABLE _order_line_final AS
SELECT
    shop_name                                                               shop_name,
    id                                                                      order_id,
    c.value:id                                                              order_item_id,
    status                                                                  order_status,
    c.value:display_status                                                  order_substatus,
    CASE
    WHEN cancellation_initiator IS NOT NULL AND status = 'CANCELLED'
        THEN 'Cancel'
    WHEN cancellation_initiator IS NOT NULL AND status = 'COMPLETED'
        THEN 'Return/Refund'
    ELSE NULL
    END                                                                     cancellation_or_return_type,
    IFF(c.value:sku_type = 'UNKNOWN', null, c.value:sku_type)               normal_or_pre_order,
    c.value:sku_id                                                          sku_id,
    c.value:seller_sku                                                      seller_sku,
    c.value:product_id                                                      product_id,
    c.value:product_name                                                    product_name,
    c.value:sku_name                                                        variation,
    1                                                                       quantity,
    CASE
    WHEN cancellation_initiator IS NOT NULL AND
        (status = 'CANCELLED' OR status = 'COMPLETED') THEN 1
    ELSE 0
    END                                                                     sku_quantity_of_return,
    c.value:original_price                                                  sku_unit_original_price,
    c.value:original_price                                                  sku_subtotal_before_discount,
    COALESCE(c.value:platform_discount, 0)                                  sku_platform_discount,
    COALESCE(c.value:seller_discount, 0)                                   sku_seller_discount,
    c.value:original_price - (COALESCE(c.value:platform_discount, 0)
                        + COALESCE(c.value:seller_discount, 0))             sku_subtotal_after_discount,
    c.value:item_tax[0]:tax_amount                                          taxes,
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
    user_id                                                                 user_id,
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
FROM
    _order_line o,
    LATERAL FLATTEN(INPUT => o.LINE_ITEMS) c;

MERGE INTO lake.tiktok_shop.order_line AS t
USING (
    SELECT *,
           hash(*) meta_row_hash,
           current_timestamp() meta_create_datetime,
           current_timestamp() meta_update_datetime
    FROM _order_line_final
    ) s
ON equal_null(t.order_id, s.order_id)
    AND equal_null(t.order_item_id, s.order_item_id)
    AND equal_null(t.shop_name, s.shop_name)

WHEN NOT MATCHED
    THEN INSERT (shop_name,order_id,order_item_id,order_status,order_substatus,cancellation_or_return_type,normal_or_pre_order,sku_id,seller_sku,product_id,product_name,variation,quantity,
                 sku_quantity_of_return,sku_unit_original_price,sku_subtotal_before_discount,sku_platform_discount,sku_seller_discount,
                 sku_subtotal_after_discount,taxes,created_time_pst,paid_time_pst,rts_time_pst,
                 shipped_time_pst,delivered_time_pst,cancelled_time_pst,cancel_by,cancel_reason,fulfillment_type,tracking_id,delivery_option_type,
                 shipping_provider_name,buyer_message,user_id,recipient,phone_number,country,state,city,zipcode,address_line1,address_line2,delivery_instruction,
                 payment_method,package_id,seller_note,shipping_information,meta_row_hash,meta_create_datetime,meta_update_datetime)
         VALUES (shop_name,order_id,order_item_id,order_status,order_substatus,cancellation_or_return_type,normal_or_pre_order,sku_id,seller_sku,product_id,product_name,variation,quantity,
                 sku_quantity_of_return,sku_unit_original_price,sku_subtotal_before_discount,sku_platform_discount,sku_seller_discount,
                 sku_subtotal_after_discount,taxes,created_time_pst,paid_time_pst,rts_time_pst,
                 shipped_time_pst,delivered_time_pst,cancelled_time_pst,cancel_by,cancel_reason,fulfillment_type,tracking_id,delivery_option_type,
                 shipping_provider_name,buyer_message,user_id,recipient,phone_number,country,state,city,zipcode,address_line1,address_line2,delivery_instruction,
                 payment_method,package_id,seller_note,shipping_information,meta_row_hash,meta_create_datetime,meta_update_datetime)
WHEN MATCHED AND t.meta_row_hash!=s.meta_row_hash
    THEN UPDATE
    SET
        t.order_status=s.order_status,
        t.order_substatus=s.order_substatus,
        t.cancellation_or_return_type=s.cancellation_or_return_type,
        t.normal_or_pre_order=s.normal_or_pre_order,
        t.seller_sku=s.seller_sku,
        t.product_id=s.product_id,
        t.product_name=s.product_name,
        t.variation=s.variation,
        t.quantity=s.quantity,
        t.sku_quantity_of_return=s.sku_quantity_of_return,
        t.sku_unit_original_price=s.sku_unit_original_price,
        t.sku_subtotal_before_discount=s.sku_subtotal_before_discount,
        t.sku_platform_discount=s.sku_platform_discount,
        t.sku_seller_discount=s.sku_seller_discount,
        t.sku_subtotal_after_discount=s.sku_subtotal_after_discount,
        t.taxes=s.taxes,
        t.created_time_pst=s.created_time_pst,
        t.paid_time_pst=s.paid_time_pst,
        t.rts_time_pst=s.rts_time_pst,
        t.shipped_time_pst=s.shipped_time_pst,
        t.delivered_time_pst=s.delivered_time_pst,
        t.cancelled_time_pst=s.cancelled_time_pst,
        t.cancel_by=s.cancel_by,
        t.cancel_reason=s.cancel_reason,
        t.fulfillment_type=s.fulfillment_type,
        t.tracking_id=s.tracking_id,
        t.delivery_option_type=s.delivery_option_type,
        t.shipping_provider_name=s.shipping_provider_name,
        t.buyer_message=s.buyer_message,
        t.user_id=s.user_id,
        t.recipient=s.recipient,
        t.phone_number=s.phone_number,
        t.country=s.country,
        t.state=s.state,
        t.city=s.city,
        t.zipcode=s.zipcode,
        t.address_line1=s.address_line1,
        t.address_line2=s.address_line2,
        t.delivery_instruction=s.delivery_instruction,
        t.payment_method=s.payment_method,
        t.package_id=s.package_id,
        t.seller_note=s.seller_note,
        t.shipping_information=s.shipping_information,
        t.meta_row_hash=s.meta_row_hash,
        t.meta_update_datetime=s.meta_update_datetime;
