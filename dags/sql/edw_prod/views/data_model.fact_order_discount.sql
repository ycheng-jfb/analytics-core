CREATE OR REPLACE VIEW data_model.fact_order_discount
(
    order_discount_id,
    order_id,
    promo_history_key,
    promo_id,
    discount_id,
    discount_type_id,
    order_discount_applied_to,
    order_discount_local_amount,
    --is_deleted,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    order_discount_id,
    order_id,
    promo_history_key,
    promo_id,
    discount_id,
    discount_type_id,
    order_discount_applied_to,
    order_discount_local_amount,
    --is_deleted,
    meta_create_datetime,
    meta_update_datetime
FROM stg.fact_order_discount
WHERE NOT is_deleted;
