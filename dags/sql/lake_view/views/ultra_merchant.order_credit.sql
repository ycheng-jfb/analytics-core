CREATE OR REPLACE VIEW lake_view.ultra_merchant.order_credit COPY GRANTS AS
SELECT
    order_credit_id,
    order_id,
    gift_certificate_id,
    store_credit_id,
    membership_token_id,
    amount,
    datetime_added,
    datetime_modified,
    subtotal,
    shipping,
    tax,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant.order_credit s
WHERE NOT exists(
        SELECT
            1
        FROM lake.ultra_merchant.order_credit_delete_log d
        WHERE d.order_credit_id = s.order_credit_id
    )
;
