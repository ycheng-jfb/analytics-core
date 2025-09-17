-- 61, 63, 79 are not matching, probably due to old data has been changing and it didnt update on the new ones

-- make code incremental -- append
SET target_date = (SELECT MAX(datetime_added)::DATE
                   FROM reporting_base_prod.shared.loyalty_points_detail);
SET current_date = CURRENT_DATE;

DELETE
FROM reporting_base_prod.shared.loyalty_points_detail
WHERE datetime_added >= $target_date;

CREATE OR REPLACE TEMPORARY TABLE _customer_male_gender AS
SELECT customer_id
FROM edw_prod.stg.dim_customer
WHERE gender = 'M'
  AND store_id NOT IN
      (SELECT store_id
       FROM lake_consolidated_view.ultra_merchant.store
       WHERE store_group_id < 9); -- Not considering customers from legacy stores

INSERT INTO reporting_base_prod.shared.loyalty_points_detail
SELECT c.store_id                                                                         AS store_id,
       CASE
           WHEN st.store_id IN (241, 24101) AND mrt.date_added < '2022-04-12' THEN 'Fabletics Womens'
           WHEN st.store_brand = 'Fabletics' AND dc.customer_id IS NOT NULL THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                                        AS store,
       store || ' ' || st.store_country                                                   AS business_unit,
       IFF(dc.customer_id IS NULL, 'F', 'M')                                              AS gender,
       mrt.membership_reward_plan_id                                                      AS membership_reward_plan_id,
       mrt.membership_reward_tier_id                                                      AS membership_reward_tier_id,
       mrt.membership_reward_tier                                                         AS membership_reward_tier,
       mrt.membership_reward_transaction_id                                               AS membership_reward_transaction_id,
       mrt.membership_id                                                                  AS membership_id,
       c.customer_id                                                                      AS customer_id,
       mrt.membership_reward_transaction_type_id                                          AS membership_reward_transaction_type_id,
       mrt.object_id                                                                      AS object_id,
       mrt.points                                                                         AS points,
       CAST(DATE_TRUNC(MONTH, mrt.datetime_added) AS DATE)                                AS activity_month_date,
       CAST(mrt.datetime_added AS DATE)                                                   AS date_added,
       mrt.datetime_added,
       IFF(mrt.points > 0, 'Accrual', 'Deducted')                                         AS point_direction,
       IFF(mrt.membership_reward_transaction_type_id = 130 AND mrt.object_id IS NULL, t.label || ' No Order Attached',
           t.label)                                                                       AS point_action,
       IFF(mrt.points > 0, 'pos Points ', 'neg Points ') || point_direction || ' - ' ||
       point_action                                                                       AS categories,
       -- promo outstanding waterwall
       DATEDIFF(DAY, mpromo.date_start, mpromo.date_end)                                  AS days_until_expiration,
       p.code                                                                             AS promotion_code,
       mpromo.membership_promo_id,
       mpromo.promo_id,
       NULL                                                                               AS transaction_statuscode,
       IFF(mpromo.statuscode = 4799, mrt.points * (-1), 0)                                AS redeemed,
       IFF(mpromo.statuscode = 4795, mrt.points * (-1), 0)                                AS cancelled,
       IFF(mpromo.statuscode = 4796 OR (mpromo.statuscode = 4790 AND mpromo.date_end < CURRENT_DATE()),
           mrt.points * (-1), 0)                                                          AS expired,
       SUM(mrt.points)
           OVER (PARTITION BY mrt.membership_id, c.store_id ORDER BY mrt.datetime_added)  AS rolling_points,
       MIN(mrt.datetime_added)
           OVER (PARTITION BY mrt.membership_id, c.store_id ORDER BY mrt.datetime_added ) AS redemption_issue_date
FROM reporting_prod.credit.loyalty_points mrt
         JOIN lake_consolidated_view.ultra_merchant.membership_reward_transaction_type t
              ON t.membership_reward_transaction_type_id = mrt.membership_reward_transaction_type_id
         JOIN lake_consolidated_view.ultra_merchant.membership c ON c.membership_id = mrt.membership_id
--     AND MRT.DATETIME_ADDED BETWEEN C.EFFECTIVE_START_DATETIME AND C.EFFECTIVE_END_DATETIME
         LEFT JOIN _customer_male_gender dc ON dc.customer_id = c.customer_id
         JOIN edw_prod.data_model.dim_date AS d ON d.full_date = CAST(mrt.datetime_added AS DATE)
         JOIN edw_prod.data_model.dim_store st ON st.store_id = c.store_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.membership_promo mpromo
                   ON mrt.object_id = edw_prod.stg.UDF_UNCONCAT_BRAND(mpromo.membership_promo_id)
                       AND mrt.membership_reward_transaction_type_id = 150
         LEFT JOIN lake_consolidated_view.ultra_merchant.promo p
                   ON p.promo_id = mpromo.promo_id
WHERE mrt.points <> 0
  AND mrt.datetime_added >= $target_date
  AND mrt.datetime_added < $current_date;

CREATE OR REPLACE TEMPORARY TABLE _customer_female_gender AS
SELECT customer_id
FROM edw_prod.stg.dim_customer
WHERE NOT gender = 'M'
  AND store_id NOT IN
      (SELECT store_id
       FROM lake_consolidated_view.ultra_merchant.store
       WHERE store_group_id < 9); -- Not considering customers from legacy stores

UPDATE reporting_base_prod.shared.loyalty_points_detail
SET gender = 'M'
WHERE customer_id IN (SELECT customer_id FROM _customer_male_gender);

UPDATE reporting_base_prod.shared.loyalty_points_detail
SET gender = 'F'
WHERE customer_id IN (SELECT customer_id FROM _customer_female_gender);

UPDATE reporting_base_prod.shared.loyalty_points_detail
SET store         = REPLACE(store, 'Mens', 'Womens'),
    business_unit = REPLACE(business_unit, 'Mens', 'Womens')
WHERE gender = 'F'
  AND business_unit LIKE '%Mens%';

UPDATE reporting_base_prod.shared.loyalty_points_detail
SET store         = REPLACE(store, 'Womens', 'Mens'),
    business_unit = REPLACE(business_unit, 'Womens', 'Mens')
WHERE gender = 'M'
  AND business_unit LIKE '%Womens%';

CREATE OR REPLACE TEMP TABLE _yitty_to_fabletics AS
SELECT lp.membership_reward_transaction_id
FROM reporting_base_prod.shared.loyalty_points_detail lp
         JOIN lake_consolidated_view.ultra_merchant_history.membership m ON lp.membership_id = m.membership_id
              AND lp.datetime_added BETWEEN m.effective_start_datetime AND m.effective_end_datetime
WHERE lp.store_id = 241
  AND m.store_id = 52
  AND DATE_TRUNC('month', lp.date_added) >= '2022-04-01';

CREATE OR REPLACE TEMP TABLE _fabletics_to_yitty AS
SELECT lp.membership_reward_transaction_id
FROM reporting_base_prod.shared.loyalty_points_detail lp
         JOIN lake_consolidated_view.ultra_merchant_history.membership m ON lp.membership_id = m.membership_id
              AND lp.datetime_added BETWEEN m.effective_start_datetime AND m.effective_end_datetime
WHERE lp.store_id = 52
  AND m.store_id = 241
  AND DATE_TRUNC('month', lp.date_added) >= '2022-04-01';

UPDATE reporting_base_prod.shared.loyalty_points_detail
SET store_id=52,
    business_unit = IFF(gender = 'F', 'Fabletics Womens US', 'Fabletics Mens US')
WHERE membership_reward_transaction_id IN (SELECT membership_reward_transaction_id FROM _yitty_to_fabletics);

UPDATE reporting_base_prod.shared.loyalty_points_detail
SET business_unit='Yitty US',
    store_id=241
WHERE membership_reward_transaction_id IN (SELECT membership_reward_transaction_id FROM _fabletics_to_yitty);

UPDATE reporting_base_prod.shared.loyalty_points_detail
SET store_id = 26
WHERE customer_id in (SELECT DISTINCT customer_id FROM reporting_base_prod.shared.loyalty_points_detail WHERE store_id = 41);

UPDATE reporting_base_prod.shared.loyalty_points_detail
SET business_unit = 'JustFab US'
WHERE store_id = 26;

-- item sold cost per point -- price per point
CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.loyalty_points_discount AS
SELECT promo_id AS                                                                                 promo_id,
       DATE_TRUNC(MONTH, CONVERT_TIMEZONE('America/Los_Angeles', fo.shipped_local_datetime))::DATE month_date,
       fo.store_id,
       dc.gender,
       SUM(od.amount)                                                                              discount_amount,
       COUNT(*)                                                                                    order_count
FROM edw_prod.data_model.fact_order fo
         JOIN lake_consolidated_view.ultra_merchant.order_discount od ON od.order_id = fo.order_id
         JOIN edw_prod.data_model.dim_customer dc
              ON dc.customer_id = fo.customer_id
WHERE fo.order_status_key = 1 --'Success'
  AND promo_id IS NOT NULL
GROUP BY 1, 2, 3, 4;

ALTER TABLE reporting_base_prod.shared.loyalty_points_discount
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

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
         JOIN reporting_base_prod.shared.loyalty_points_detail mrt
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

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.loyalty_items_sold_price_per_point_detail AS
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

ALTER TABLE reporting_base_prod.shared.loyalty_items_sold_price_per_point_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

INSERT INTO reporting_base_prod.shared.loyalty_items_sold_price_per_point_detail_snapshot
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
FROM reporting_base_prod.shared.loyalty_items_sold_price_per_point_detail;

DELETE
FROM reporting_base_prod.shared.loyalty_items_sold_price_per_point_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());

INSERT INTO reporting_base_prod.shared.loyalty_points_detail_snapshot
SELECT store_id,
       store,
       business_unit,
       gender,
       membership_reward_plan_id,
       membership_reward_transaction_id,
       membership_id,
       customer_id,
       membership_reward_transaction_type_id,
       object_id,
       points,
       activity_month_date,
       date_added,
       datetime_added,
       point_direction,
       point_action,
       categories,
       days_until_expiration,
       promotion_code,
       membership_promo_id,
       promo_id,
       transaction_statuscode,
       redeemed,
       cancelled,
       expired,
       rolling_points,
       redemption_issue_date,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM reporting_base_prod.shared.loyalty_points_detail;

DELETE
FROM reporting_base_prod.shared.loyalty_points_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
