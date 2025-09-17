-- 61, 63, 79 are not matching, probably due to old data has been changing and it didn't update on the new ones

-- make code incremental -- append
SET target_date = IFF((SELECT COUNT(*) FROM month_end.loyalty_points_detail)=0,
                        (SELECT MIN(datetime_added)::DATE FROM month_end.loyalty_points),
                        (SELECT MAX(datetime_added)::DATE FROM month_end.loyalty_points_detail));
SET current_date = CURRENT_DATE;

DELETE
FROM month_end.loyalty_points_detail
WHERE datetime_added >= $target_date;

CREATE OR REPLACE TEMPORARY TABLE _customer_male_gender AS
SELECT customer_id
FROM edw_prod.stg.dim_customer
WHERE gender = 'M'
  AND store_id NOT IN
      (SELECT store_id
       FROM lake_consolidated_view.ultra_merchant.store
       WHERE store_group_id < 9); -- Not considering customers from legacy stores

INSERT INTO month_end.loyalty_points_detail
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
FROM month_end.loyalty_points mrt
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

UPDATE month_end.loyalty_points_detail
SET gender = 'M'
WHERE customer_id IN (SELECT customer_id FROM _customer_male_gender);

UPDATE month_end.loyalty_points_detail
SET gender = 'F'
WHERE customer_id IN (SELECT customer_id FROM _customer_female_gender);

UPDATE month_end.loyalty_points_detail
SET store         = REPLACE(store, 'Mens', 'Womens'),
    business_unit = REPLACE(business_unit, 'Mens', 'Womens')
WHERE gender = 'F'
  AND business_unit LIKE '%Mens%';

UPDATE month_end.loyalty_points_detail
SET store         = REPLACE(store, 'Womens', 'Mens'),
    business_unit = REPLACE(business_unit, 'Womens', 'Mens')
WHERE gender = 'M'
  AND business_unit LIKE '%Womens%';

CREATE OR REPLACE TEMP TABLE _yitty_to_fabletics AS
SELECT lp.membership_reward_transaction_id
FROM month_end.loyalty_points_detail lp
         JOIN lake_consolidated_view.ultra_merchant_history.membership m ON lp.membership_id = m.membership_id
              AND lp.datetime_added BETWEEN m.effective_start_datetime AND m.effective_end_datetime
WHERE lp.store_id = 241
  AND m.store_id = 52
  AND DATE_TRUNC('month', lp.date_added) >= '2022-04-01';

CREATE OR REPLACE TEMP TABLE _fabletics_to_yitty AS
SELECT lp.membership_reward_transaction_id
FROM month_end.loyalty_points_detail lp
         JOIN lake_consolidated_view.ultra_merchant_history.membership m ON lp.membership_id = m.membership_id
              AND lp.datetime_added BETWEEN m.effective_start_datetime AND m.effective_end_datetime
WHERE lp.store_id = 52
  AND m.store_id = 241
  AND DATE_TRUNC('month', lp.date_added) >= '2022-04-01';

UPDATE month_end.loyalty_points_detail
SET store_id=52,
    business_unit = IFF(gender = 'F', 'Fabletics Womens US', 'Fabletics Mens US')
WHERE membership_reward_transaction_id IN (SELECT membership_reward_transaction_id FROM _yitty_to_fabletics);

UPDATE month_end.loyalty_points_detail
SET business_unit='Yitty US',
    store_id=241
WHERE membership_reward_transaction_id IN (SELECT membership_reward_transaction_id FROM _fabletics_to_yitty);

UPDATE month_end.loyalty_points_detail
SET store_id = 26
WHERE customer_id in (SELECT DISTINCT customer_id FROM month_end.loyalty_points_detail WHERE store_id = 41);

UPDATE month_end.loyalty_points_detail
SET business_unit = 'JustFab US'
WHERE store_id = 26;

INSERT INTO month_end.loyalty_points_detail_snapshot
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
FROM month_end.loyalty_points_detail;

DELETE
FROM month_end.loyalty_points_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
