TRUNCATE TABLE validation.warehouse_outlet_order_alert;

SET execution_start_datetime = current_timestamp;
INSERT INTO validation.warehouse_outlet_order_alert
SELECT DISTINCT
    order_id,
    'token redemption' AS alert_type,
    $execution_start_datetime AS meta_create_datetime,
    $execution_start_datetime AS meta_update_datetime
FROM data_model.fact_order AS fo
    JOIN data_model.dim_order_sales_channel AS osc
        ON osc.order_sales_channel_key = fo.order_sales_channel_key
WHERE osc.is_warehouse_outlet_order = true and token_local_amount > 0 AND order_id NOT IN (154861414020,156833281520)
UNION ALL
SELECT DISTINCT
    fo.order_id,
    'promo discount' AS alert_type,
    $execution_start_datetime,
    $execution_start_datetime
FROM data_model.fact_order AS fo
    JOIN data_model.dim_order_sales_channel AS osc
        ON osc.order_sales_channel_key = fo.order_sales_channel_key
    JOIN lake_consolidated.ultra_merchant.order_discount AS od
        ON od.order_id = fo.order_id
WHERE osc.is_warehouse_outlet_order = true;
