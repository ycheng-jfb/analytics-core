CREATE OR REPLACE TEMP TABLE _loyalty_points AS
SELECT lp.mrt_key      AS   mrt_key,
       lp.store_id,
       lp.membership_id,
       lp.membership_reward_plan_id,
       lp.fixed_points AS   points,
       lp.membership_reward_transaction_type_id,
       lp.datetime_added,
       SUM(lp.fixed_points) over (partition BY lp.membership_id ORDER BY lp.mrt_key ASC) AS raw_balbal,
       row_number() over (partition BY lp.membership_id ORDER BY lp.mrt_key ASC)         AS rnk
FROM month_end.loyalty_points_balance lp;

UPDATE _loyalty_points
SET points     = points - raw_balbal,
    raw_balbal = 0
WHERE membership_reward_transaction_type_id = 160
  AND raw_balbal > 0;

CREATE OR REPLACE TEMP TABLE _issued AS
SELECT mrt_key,
       store_id,
       membership_id,
       membership_reward_plan_id,
       points,
       membership_reward_transaction_type_id,
       datetime_added,
       SUM(iff(points > 0, points, 0)) over (partition BY membership_id ORDER BY mrt_key) AS issues
FROM _loyalty_points
WHERE points > 0;

CREATE OR REPLACE TEMP TABLE _redeemed AS
SELECT mrt_key,
       store_id,
       membership_id,
       membership_reward_plan_id,
       points,
       membership_reward_transaction_type_id,
       datetime_added,
       SUM(iff(points < 0, abs(points), 0)) over (partition BY membership_id ORDER BY mrt_key) AS redeems
FROM _loyalty_points
WHERE points < 0;

CREATE OR REPLACE TEMP TABLE _scaffold AS
SELECT is_.mrt_key,
       re_.mrt_key              AS redemption_key,
       is_.issues - re_.redeems AS bal,
       is_.points,
       re_.points               AS redeemed,
       is_.issues,
       re_.redeems,
       is_.membership_id
FROM _issued is_
         LEFT JOIN _redeemed re_ ON is_.mrt_key < re_.mrt_key
    AND is_.membership_id = re_.membership_id
ORDER BY 1, 2;

--find if different issued points are used by more than on redemptions
CREATE OR REPLACE TEMP TABLE _partitial_redeems AS
SELECT mrt_key,
       redemption_key,
       membership_id,
       iff((s.points - bal) < -redeemed, (s.points - bal), -redeemed) AS partitial_redeemed
FROM _scaffold s
WHERE bal > 0
  AND (s.issues - s.redeems) < s.points
UNION ALL
SELECT mrt_key, MIN(redemption_key) AS redemption_key, membership_id, NULL
FROM _scaffold
WHERE bal <= 0
GROUP BY mrt_key, membership_id
ORDER BY 1, 2;

--calculate no. of points redeemed in each issuance and redemption combination
CREATE OR REPLACE TEMP TABLE _redeems_per_redemption AS
SELECT is_.mrt_key,
       pr_.redemption_key,
       is_.membership_id,
       iff(pr_.redemption_key IS NOT NULL,
           COALESCE(pr_.partitial_redeemed, is_.points - COALESCE(
               SUM(pr_.partitial_redeemed) over(partition BY is_.mrt_key ORDER BY redemption_key), 0)),
           NULL)                                      AS redeemed_per_redemption,
       iff(pr_.redemption_key IS NULL, is_.points, 0) AS unredeemed_points
FROM _issued is_
         LEFT JOIN _partitial_redeems pr_ ON pr_.mrt_key = is_.mrt_key
    AND pr_.membership_id = is_.membership_id
ORDER BY 1, 2;

--insert issued points which are partially redeemed and calculate unredeemed amounts
--at most only one record exists for each membership_id with this case
INSERT INTO _redeems_per_redemption
SELECT redeemed_till_now_.mrt_key,
       NULL,
       is_.membership_id,
       NULL,
       (is_.points - redeemed_till_now_.redeemed_till_now) unredeemed
FROM (SELECT mrt_key, SUM(redeemed_per_redemption) AS redeemed_till_now
      FROM _redeems_per_redemption
      GROUP BY mrt_key) redeemed_till_now_
         JOIN _issued is_ ON is_.mrt_key = redeemed_till_now_.mrt_key
    AND (is_.points - redeemed_till_now_.redeemed_till_now) > 0;


CREATE OR REPLACE TEMP TABLE _loyalty_points_results_ammended AS
SELECT DISTINCT lpr.mrt_key,
                CAST(lp2.datetime_added AS DATE)                                          AS date_issued,
                lpr.membership_id,
                CASE WHEN IFNULL(IFF(m.store_id = 118, 52, m.store_id), lp2.store_id) = 41
                     THEN 26
                     ELSE IFNULL(IFF(m.store_id = 118, 52, m.store_id), lp2.store_id) END AS store_id,
                lpr2.membership_reward_plan_id,
                lpr.redemption_key,
                lp.datetime_added                                                         AS redemption_date,
                CASE
                    WHEN lp.membership_reward_transaction_type_id = 140 THEN 'Point Expiration'
                    WHEN lp.membership_reward_transaction_type_id = 160 THEN 'Membership Cancellation'
                    WHEN lp.membership_reward_transaction_type_id = 161 THEN 'Non-VIP Point Adjustment'
                    WHEN lp.membership_reward_transaction_type_id = 110 THEN 'Refund - Cash'
                    --WHEN lp.membership_reward_transaction_type_id = 120 THEN 'Manual Debit'
                    ELSE NULL
                    END                                                                   AS redemption_type,
                lpr.redeemed_per_redemption                                               AS points_redeemed,
                lpr.unredeemed_points,
                datediff(MONTH, lp2.datetime_added, lp.datetime_added) + 1                AS months_until_redemption
FROM _redeems_per_redemption lpr
         JOIN _loyalty_points lpr2 ON lpr.mrt_key = lpr2.mrt_key
         JOIN _loyalty_points lp2 ON lp2.mrt_key = lpr.mrt_key
         LEFT JOIN _loyalty_points lp ON lpr.redemption_key = lp.mrt_key
         LEFT JOIN (SELECT DISTINCT customer_id,
                                    membership_id,
                                    store_id,
                                    effective_start_datetime,
                                    effective_end_datetime
                    FROM lake_consolidated_view.ultra_merchant_history.membership) m
                   ON m.membership_id = lpr.membership_id
                       AND
                      lp2.datetime_added
                          BETWEEN m.effective_start_datetime AND m.effective_end_datetime;

-- fix for the old data with activities date before issue dates, only effecting records from 2011 - 2014
UPDATE _loyalty_points_results_ammended
SET months_until_redemption = 1
WHERE months_until_redemption < 1;

CREATE
OR REPLACE TEMPORARY TABLE _customer_male_gender AS
SELECT customer_id
FROM edw_prod.stg.dim_customer
WHERE gender = 'M'
  AND store_id NOT IN
      (SELECT store_id
       FROM lake_consolidated_view.ultra_merchant.store
       WHERE store_group_id < 9); -- Not considering customers from legacy stores


CREATE OR REPLACE TRANSIENT TABLE month_end.loyalty_points_accrual_to_redemption_no_refund_with_loop
    (store_set VARCHAR(50),
     store VARCHAR(50),
     store_abbreviation VARCHAR(25),
     month_issued DATE,
     membership_reward_tier VARCHAR(55),
     redeemed VARCHAR(55),
     gender VARCHAR(5),
     points BIGINT
        )
AS
SELECT CASE
           WHEN st.store_id IN (241, 24101) AND mrt.date_added < '2022-04-12' THEN 'Fabletics Womens'
           WHEN st.store_brand = 'Fabletics' AND dc.customer_id IS NOT NULL THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                                           AS store_set,
       store_set || ' ' || st.store_country                                                  AS store,
       iff(r.store_id IN (241, 24101) AND r.date_issued < '2022-04-12', 'Fabletics US',
           concat_ws(' ', st.store_brand, st.store_country))                                 AS store_abbreviation,
       d.month_date                                                                          AS month_issued,
       mrt.membership_reward_tier,
       CASE
           WHEN redemption_type IN
                ('Point Expiration', 'Membership Cancellation', 'Non-VIP Point Adjustment', 'Refund - Cash')
               THEN redemption_type
           WHEN months_until_redemption IS NOT NULL THEN concat('M', CAST(months_until_redemption AS VARCHAR(55)))
           ELSE redemption_type
           END                                                                               AS redeemed,
       iff(dc.customer_id IS NULL, 'F', 'M')                                                 AS gender,
       SUM(COALESCE(CAST(r.points_redeemed AS BIGINT), CAST(r.unredeemed_points AS BIGINT))) AS points
FROM _loyalty_points_results_ammended r
         JOIN month_end.loyalty_points mrt ON mrt.membership_reward_transaction_id = r.mrt_key
         LEFT JOIN _customer_male_gender dc ON mrt.customer_id = dc.customer_id
         JOIN edw_prod.data_model.dim_date d ON d.full_date = DATE (mrt.datetime_added)
-- 		    should join membership and get it from membership
    JOIN edw_prod.data_model.dim_store AS st
ON st.store_id = r.store_id
GROUP BY store_set,
         store,
         store_abbreviation,
         month_issued,
         mrt.membership_reward_tier,
         redeemed,
         gender;

UPDATE month_end.loyalty_points_accrual_to_redemption_no_refund_with_loop
SET redeemed = CASE
                   WHEN redeemed = 'M1' THEN 'M01'
                   WHEN redeemed = 'M2' THEN 'M02'
                   WHEN redeemed = 'M3' THEN 'M03'
                   WHEN redeemed = 'M4' THEN 'M04'
                   WHEN redeemed = 'M5' THEN 'M05'
                   WHEN redeemed = 'M6' THEN 'M06'
                   WHEN redeemed = 'M7' THEN 'M07'
                   WHEN redeemed = 'M8' THEN 'M08'
                   WHEN redeemed = 'M9' THEN 'M09'
                   WHEN redeemed IS NULL THEN 'Unredeemed'
    END
WHERE redeemed IS NULL
   OR redeemed IN ('M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9');
