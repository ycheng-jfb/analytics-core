------------------------------------------------------------------------
-- get the average item price (after discounts) of each loyalty item sold within the last 12 months
SET process_to_date = DATE_TRUNC(MONTH, CURRENT_DATE);
SET twelve_month_before = DATEADD(MONTH, -12, $process_to_date);

CREATE OR REPLACE TEMPORARY TABLE _ol_reward_item AS
SELECT ol.*,
       IFF(ol.order_line_status_key = 1, 'Cancelled', os.order_status) order_status,
       p.product_name,
       p.color,
       p.product_sku,
       p.vip_unit_price                                                vip_price
FROM edw_prod.data_model.dim_product p
         JOIN edw_prod.data_model.fact_order_line ol ON ol.product_id = p.product_id
         JOIN edw_prod.data_model.dim_product_type pt ON pt.product_type_key = ol.product_type_key
         JOIN edw_prod.data_model.fact_order fo ON fo.order_id = ol.order_id
         JOIN edw_prod.data_model.dim_order_status os ON ol.order_status_key = os.order_status_key
         JOIN edw_prod.data_model.dim_order_sales_channel doc
              ON doc.order_sales_channel_key = fo.order_sales_channel_key
WHERE pt.product_type_name = 'Membership Reward Points Item'
  AND doc.order_classification_l1 = 'Product Order';

CREATE OR REPLACE TEMPORARY TABLE _order_count_line AS
SELECT order_id, COUNT(*) order_line_count
FROM _ol_reward_item
GROUP BY 1;

CREATE OR REPLACE TEMPORARY TABLE _member_reward_item AS
SELECT business_unit,
       store_id,
       ori.order_id,
       DATE_TRUNC(MONTH, mrt.date_added) ::DATE redeemed_month,
       points / ori.order_line_count            points_per_line
FROM _order_count_line ori
         JOIN month_end.loyalty_points_detail mrt
              ON edw_prod.stg.udf_unconcat_brand(ori.order_id) = mrt.object_id
                  AND mrt.membership_reward_transaction_type_id = 130
                  AND mrt.object_id IS NOT NULL
--     AND DATEDIFF(MONTH, MRT.DATE_ADDED, $PROCESS_TO_DATE) <= 25
                  AND mrt.datetime_added >= DATEADD(MONTH, -25, $process_to_date)
                  AND mrt.datetime_added < $process_to_date;

-- get all the loyalty skus, then join to the non loyalty purchase of those skus
CREATE OR REPLACE TEMPORARY TABLE _sold_items AS
SELECT DISTINCT ol.store_id,
                p.product_sku
FROM edw_prod.data_model.fact_order_line ol
         JOIN edw_prod.data_model.dim_product p ON p.product_id = ol.product_id
         JOIN edw_prod.data_model.dim_product_type pt ON pt.product_type_key = ol.product_type_key
         JOIN lake_consolidated_view.ultra_merchant.membership_reward_transaction mrt
              ON edw_prod.stg.udf_unconcat_brand(ol.order_id) = mrt.object_id
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(mrt.datetime_added AS DATE)
    AND mrt.membership_reward_transaction_type_id = 130 ---- to get only swag items
WHERE pt.product_type_name = 'Membership Reward Points Item'
--   AND DATEDIFF(MONTH, OL.ORDER_LOCAL_DATETIME, $PROCESS_TO_DATE) < 12 -- purchased within last 12 months
  AND ol.order_local_datetime >= $twelve_month_before
  AND ol.order_local_datetime < $process_to_date;

CREATE OR REPLACE TEMPORARY TABLE _avg_loyalty_price_sold AS
SELECT ol.store_id,
       p.product_sku,
       SUM(item_quantity)                                                           AS items_sold,
       AVG(ol.subtotal_excl_tariff_local_amount - ol.product_discount_local_amount) AS avg_subtotal_after_discount
FROM edw_prod.data_model.fact_order_line ol
         JOIN edw_prod.data_model.dim_product p ON p.product_id = ol.product_id
         JOIN edw_prod.data_model.dim_product_type pt ON pt.product_type_key = ol.product_type_key
         JOIN _sold_items sku
              ON sku.product_sku = p.product_sku AND sku.store_id = ol.store_id
WHERE pt.product_type_name != 'Membership Reward Points Item' -- non loyalty purchase
--   AND DATEDIFF(MONTH, OL.ORDER_LOCAL_DATETIME, $PROCESS_TO_DATE) <= 12
  AND ol.order_local_datetime >= $twelve_month_before
  AND ol.order_local_datetime < $process_to_date
GROUP BY ol.store_id, p.product_sku;

CREATE OR REPLACE TRANSIENT TABLE month_end.loyalty_items_sold_price_per_point_detail AS
SELECT business_unit,
       CASE
           WHEN DATE_TRUNC(MONTH, ori.shipped_local_datetime)::DATE = '1900-01-01' THEN NULL
           WHEN ori.order_line_status_key = 1 THEN NULL
           ELSE DATE_TRUNC(MONTH, ori.shipped_local_datetime)::DATE
           END                                shipped_month,
       redeemed_month,
       ori.order_line_id,
       ori.order_id,
       ori.order_status,
       points_per_line                        points,
       ori.product_name,
       ori.color,
       ori.product_sku,
       ori.vip_price,
       alps.avg_subtotal_after_discount       avg_item_price_sold_after_discounts_last_12m,
       alps.items_sold                        count_items_sold_to_form_avg_price,
       ori.estimated_landed_cost_local_amount total_product_cost,
       ori.item_quantity                      units_shipped
FROM _member_reward_item mri
         JOIN _ol_reward_item ori ON mri.order_id = ori.order_id
         JOIN edw_prod.data_model.dim_order_line_status ols ON ols.order_line_status_key = ori.order_line_status_key
         LEFT JOIN _avg_loyalty_price_sold alps
                   ON mri.store_id = alps.store_id AND ori.product_sku = alps.product_sku;

ALTER TABLE month_end.loyalty_items_sold_price_per_point_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

INSERT INTO month_end.loyalty_items_sold_price_per_point_detail_snapshot
SELECT business_unit,
       shipped_month,
       redeemed_month,
       order_id,
       order_status,
       points,
       product_name,
       color,
       product_sku,
       vip_price,
       avg_item_price_sold_after_discounts_last_12m,
       count_items_sold_to_form_avg_price,
       total_product_cost,
       units_shipped,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM month_end.loyalty_items_sold_price_per_point_detail;

DELETE
FROM month_end.loyalty_items_sold_price_per_point_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
