SET is_empty = (SELECT NOT EXISTS(SELECT 1 FROM month_end.loyalty_points));
SET current_date_hq = CONVERT_TIMEZONE('America/Los_Angeles', CURRENT_TIMESTAMP) :: DATE;
SET current_month_date_hq = DATE_TRUNC(MONTH, $current_date_hq);
SET process_from_month_date_hq = (SELECT IFF($is_empty, '2010-01-01', DATEADD(MONTH, -1, $current_month_date_hq)));
SET execution_time = CURRENT_TIMESTAMP()::TIMESTAMP_LTZ;

DELETE
FROM month_end.loyalty_points
WHERE date_added >= $process_from_month_date_hq;

-- Mapping lake product_id with product_sku's
CREATE OR REPLACE TEMP TABLE _dim_product__item AS
SELECT p.product_id,
       mp.product_id                                           AS master_product_id,
       TRIM(i.item_number)                                     AS item_number,
       dsku.product_sku,
       dsku.base_sku,
       TRIM(iw.wms_class)                                      AS wms_class,
       COALESCE(p.membership_brand_id, mp.membership_brand_id) AS membership_brand_id
FROM lake_consolidated.ultra_merchant.product AS p
         JOIN lake_consolidated.ultra_merchant.product AS mp
              ON mp.product_id = COALESCE(p.master_product_id, p.product_id) /* The attributes in lake_consolidated.ultra_merchant.product are assigned to the master_product_id */
         LEFT JOIN lake_consolidated.ultra_merchant.item AS i
                   ON i.item_id = COALESCE(mp.item_id, p.item_id)
         LEFT JOIN lake.ultra_warehouse.item AS iw
                   ON TRIM(i.item_number) = TRIM(iw.item_number)
         LEFT JOIN edw_prod.stg.dim_sku AS dsku
                   ON TRIM(i.item_number) = dsku.sku
QUALIFY ROW_NUMBER() OVER (PARTITION BY p.product_id ORDER BY iw.item_id) = 1
ORDER BY p.product_id;

-- Identifies misclassified orders with reward points redemption
CREATE OR REPLACE TEMP TABLE _misclassified_orders AS
SELECT edw_prod.stg.udf_unconcat_brand(fol.order_id) AS order_id
FROM lake_consolidated_view.ultra_merchant.order_line fol
         JOIN lake_consolidated_view.ultra_merchant.product dp
              ON fol.product_id = dp.product_id
         JOIN _dim_product__item psm
              ON dp.product_id = psm.product_id
WHERE psm.product_sku IN
      (SELECT product_sku FROM edw_prod.reference.misclassified_loyalty_skus);

INSERT INTO month_end.loyalty_points
(store_id,
 membership_reward_plan_id,
 membership_reward_tier_id,
 membership_reward_tier,
 membership_reward_transaction_id,
 membership_id,
 customer_id,
 membership_reward_transaction_type_id,
 object_id,
 points,
 datetime_added,
 date_added,
 meta_create_datetime,
 meta_update_datetime)
SELECT iff(c.store_id = 41, 26, c.store_id)                                     AS store_id,
       mrt.membership_reward_plan_id,
       mrtt.membership_reward_tier_id,
       iff(mrtt.effective_start_datetime = '1900-01-01 00:00:00.000 -08:00'
               AND mrtt.membership_reward_plan_id IN (1420, 820)
               AND mrtt.label IN ('Gold'), 'Legacy ' || mrtt.label, mrtt.label) AS membership_reward_tier,
       mrt.membership_reward_transaction_id,
       mrt.membership_id,
       c.customer_id,
       mrt.membership_reward_transaction_type_id,
       mrt.object_id,
       mrt.points,
       mrt.datetime_added,
       mrt.datetime_added::DATE                                      AS date_added,
       $execution_time                                                          AS meta_create_datetime,
       $execution_time                                                          AS meta_update_datetime
FROM lake_consolidated_view.ultra_merchant.membership_reward_transaction mrt
         LEFT JOIN lake_consolidated_view.ultra_merchant_history.membership c ON c.membership_id = mrt.membership_id
    AND mrt.datetime_added BETWEEN c.effective_start_datetime AND c.effective_end_datetime
         LEFT JOIN lake_consolidated_view.ultra_merchant.membership_reward_plan mp
                   ON edw_prod.stg.udf_unconcat_brand(mp.membership_reward_plan_id) =
                      edw_prod.stg.udf_unconcat_brand(mrt.membership_reward_plan_id)
         LEFT JOIN lake_consolidated_view.ultra_merchant.membership_plan m
                   ON edw_prod.stg.udf_unconcat_brand(m.membership_plan_id) =
                      edw_prod.stg.udf_unconcat_brand(mp.membership_plan_id)
         LEFT JOIN lake_consolidated_view.ultra_merchant_history.membership_reward_tier mrtt
                   ON c.membership_reward_plan_id = mrtt.membership_reward_plan_id
                       AND c.membership_reward_tier_id = mrtt.membership_reward_tier_id
                       AND mrt.datetime_added BETWEEN mrtt.effective_start_datetime AND mrtt.effective_end_datetime
         LEFT JOIN month_end.loyalty_points lp
                   ON lp.membership_reward_transaction_id = mrt.membership_reward_transaction_id
WHERE mrt.statuscode = 3995
  AND lp.membership_reward_transaction_id IS NULL
  AND mrt.points <> 0
  AND mrt.datetime_added >= $process_from_month_date_hq;

-- Expiring the points redeemed in ORDERS without loyalty items or promo, BUG FROM THE SOURCE
UPDATE month_end.loyalty_points
SET membership_reward_transaction_type_id = 140
WHERE object_id IN (SELECT order_id FROM _misclassified_orders)
  AND membership_reward_transaction_type_id = 130;
