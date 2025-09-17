CREATE OR REPLACE VIEW data_model_jfb.fact_order_line_discount
(
    order_line_discount_key,
    order_line_discount_id,
    order_line_id,
    order_id,
    bundle_order_line_discount_id,
    promo_history_key,
    promo_id,
    discount_id,
    order_line_discount_local_amount,
    is_indirect_discount,
    --is_deleted,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    order_line_discount_key,
    order_line_discount_id,
    stg.udf_unconcat_brand(order_line_id) AS order_line_id,
    stg.udf_unconcat_brand(order_id) AS order_id,
    bundle_order_line_discount_id,
    promo_history_key,
    promo_id,
    discount_id,
    order_line_discount_local_amount,
    is_indirect_discount,
    --is_deleted,
    meta_create_datetime,
    meta_update_datetime
FROM stg.fact_order_line_discount
WHERE NOT is_deleted
    AND substring(order_line_id, -2) = '10';
