CREATE OR REPLACE VIEW data_model_jfb.dim_order_customer_selected_shipping
(
    order_customer_selected_shipping_key,
    customer_selected_shipping_type,
    customer_selected_shipping_service,
    customer_selected_shipping_description,
    customer_selected_shipping_price,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    order_customer_selected_shipping_key,
    customer_selected_shipping_type,
    customer_selected_shipping_service,
    customer_selected_shipping_description,
    customer_selected_shipping_price,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_order_customer_selected_shipping;
