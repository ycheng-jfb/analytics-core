SET target_table = 'stg.dim_order_customer_selected_shipping';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

SET wm_lake_ultra_merchant_shipping_option = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.shipping_option'));
SET wm_lake_ultra_merchant_order_shipping = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.order_shipping'));

CREATE OR REPLACE TEMP TABLE _order_customer_selected_shipping__stg
(
    customer_selected_shipping_type VARCHAR,
    customer_selected_shipping_service VARCHAR,
    customer_selected_shipping_description VARCHAR,
    customer_selected_shipping_price NUMBER(38,4)
);

INSERT INTO _order_customer_selected_shipping__stg
SELECT
      COALESCE(INITCAP(so.type), 'Unknown') AS customer_selected_shipping_type,
      COALESCE(INITCAP(so.label), 'Unknown') AS customer_selected_shipping_service,
      COALESCE(INITCAP(so.description), 'Unknown') AS customer_selected_shipping_description,
      COALESCE(so.cost, -1) AS customer_selected_shipping_price
FROM lake_consolidated.ultra_merchant.shipping_option so
WHERE so.meta_update_datetime > $wm_lake_ultra_merchant_shipping_option

UNION

SELECT
      COALESCE(INITCAP(os.type), 'Unknown') AS customer_selected_shipping_type,
      'Unknown' AS customer_selected_shipping_service,
      'Unknown' AS customer_selected_shipping_description,
      -1 AS customer_selected_shipping_price
FROM lake_consolidated.ultra_merchant.order_shipping os
WHERE os.meta_update_datetime > $wm_lake_ultra_merchant_order_shipping

UNION

SELECT
    customer_selected_shipping_type,
    customer_selected_shipping_service,
    customer_selected_shipping_description,
    customer_selected_shipping_price
FROM excp.dim_order_customer_selected_shipping
WHERE meta_is_current_excp
AND meta_data_quality = 'error'

UNION

SELECT
    'Not Applicable' AS customer_selected_shipping_type,
    'Not Applicable' AS customer_selected_shipping_service,
    'Not Applicable' AS customer_selected_shipping_description,
    -2 AS customer_selected_shipping_price
;

INSERT INTO stg.dim_order_customer_selected_shipping_stg
(
    customer_selected_shipping_type,
    customer_selected_shipping_service,
    customer_selected_shipping_description,
    customer_selected_shipping_price,
    meta_create_datetime,
    meta_update_datetime
)

SELECT
    customer_selected_shipping_type,
    customer_selected_shipping_service,
    customer_selected_shipping_description,
    customer_selected_shipping_price,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _order_customer_selected_shipping__stg;
