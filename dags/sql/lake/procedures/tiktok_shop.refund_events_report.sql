SET low_watermark_ltz = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _refund_events AS
SELECT
    *
FROM
    lake.tiktok_shop.refund_events o
WHERE meta_update_datetime > $low_watermark_ltz;

CREATE OR REPLACE TEMP TABLE _refund_events_final AS
SELECT
r.shop_name                                                          shop_name,
r.return_id                                                          return_order_id,
i.value:return_line_item_id                                          return_line_item_id,
r.order_id                                                           order_id,
i.value:order_line_item_id                                           order_line_item_id,
i.value:refund_amount:refund_total                                   refund_total,
i.value:sku_id                                                       sku_id,
i.value:seller_sku                                                   seller_sku,
i.value:product_name                                                 product_name,
i.value:sku_name                                                     sku_name,
r.return_type                                                        return_type,
CONVERT_TIMEZONE('UTC', 'US/Pacific', to_timestamp(create_time))     created_time_pst,
CONVERT_TIMEZONE('UTC', 'US/Pacific', to_timestamp(update_time))     updated_time_pst,
r.return_reason                                                      return_reason,
r.return_reason_text                                                 return_reason_text,
i.value:refund_amount:refund_shipping_fee                            refund_shipping_fee,
i.value:refund_amount:refund_tax                                     refund_tax,
i.value:refund_amount:refund_subtotal                                refund_subtotal,
1                                                                    return_quantity,
r.return_status                                                      return_status,
r.return_tracking_number                                             return_logistics_tracking_id,
FROM
    _refund_events r,
    LATERAL FLATTEN(input => r.RETURN_LINE_ITEMS) i;

MERGE INTO lake.tiktok_shop.refund_events_report as t
USING (
    SELECT *,
           hash(*) meta_row_hash,
           current_timestamp() meta_create_datetime,
           current_timestamp() meta_update_datetime
    FROM _refund_events_final
    ) s
ON equal_null(t.return_order_id, s.return_order_id)
    AND equal_null(t.return_line_item_id, s.return_line_item_id)
    AND equal_null(t.shop_name, s.shop_name)
WHEN NOT MATCHED
    THEN INSERT (shop_name,return_order_id,return_line_item_id,order_id,order_line_item_id,refund_total,sku_id,seller_sku,
                 product_name,sku_name,return_type,created_time_pst,updated_time_pst,return_reason,return_reason_text,
                 refund_shipping_fee,refund_tax,refund_subtotal,return_quantity,return_status,
                 return_logistics_tracking_id,meta_row_hash,meta_create_datetime,meta_update_datetime)
         VALUES (shop_name,return_order_id,return_line_item_id,order_id,order_line_item_id,refund_total,sku_id,seller_sku,
                 product_name,sku_name,return_type,created_time_pst,updated_time_pst,return_reason,return_reason_text,
                 refund_shipping_fee,refund_tax,refund_subtotal,return_quantity,return_status,
                 return_logistics_tracking_id,meta_row_hash,meta_create_datetime,meta_update_datetime)
WHEN MATCHED AND t.meta_row_hash!=s.meta_row_hash
    THEN UPDATE
    SET
        t.order_id=s.order_id,
        t.order_line_item_id=s.order_line_item_id,
        t.refund_total=s.refund_total,
        t.sku_id=s.sku_id,
        t.seller_sku=s.seller_sku,
        t.product_name=s.product_name,
        t.sku_name=s.sku_name,
        t.return_type=s.return_type,
        t.created_time_pst=s.created_time_pst,
        t.updated_time_pst=s.updated_time_pst,
        t.return_reason=s.return_reason,
        t.return_reason_text=s.return_reason_text,
        t.refund_shipping_fee=s.refund_shipping_fee,
        t.refund_tax=s.refund_tax,
        t.refund_subtotal=s.refund_subtotal,
        t.return_quantity=s.return_quantity,
        t.return_status=s.return_status,
        t.return_logistics_tracking_id=s.return_logistics_tracking_id,
        t.meta_row_hash=s.meta_row_hash,
        t.meta_update_datetime=s.meta_update_datetime;
