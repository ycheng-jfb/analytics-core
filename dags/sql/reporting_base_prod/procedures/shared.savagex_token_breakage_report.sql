SET month_thru = DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE));


CREATE OR REPLACE TEMP TABLE _extended_key_original AS
SELECT dcf.original_credit_key
FROM lake_consolidated_view.ultra_merchant.membership_token mt
         JOIN shared.dim_credit dcf
              ON mt.membership_token_id = dcf.credit_id AND dcf.source_credit_id_type = 'Token'
WHERE mt.extension_months > 0;


CREATE OR REPLACE TEMPORARY TABLE _original_credit_activity AS
SELECT CASE
           WHEN store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
           WHEN store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE store_brand END                                             AS brand,
       store_country                                                        AS country,
       store_region                                                         AS region,
       IFF(ek.original_credit_key IS NOT NULL, TRUE, FALSE)                 AS is_extended,
       credit_activity_type,
       original_credit_tender,
       original_credit_type,
       original_credit_reason,
       DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       SUM(fce.credit_activity_gross_vat_local_amount)                      AS activity_amount
FROM shared.dim_credit dcf -- new credit key
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
         LEFT JOIN _extended_key_original ek
                   ON dcf.original_credit_key = ek.original_credit_key
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled', 'Expired')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand = 'Savage X'
  AND st.store_country = 'US'
GROUP BY brand,
         country,
         region,
         IFF(ek.original_credit_key IS NOT NULL, TRUE, FALSE),
         credit_activity_type,
         original_credit_tender,
         original_credit_type,
         original_credit_reason,
         DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE),
         DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE);


CREATE OR REPLACE TEMPORARY TABLE _pivot AS
WITH _issuance AS (SELECT *
                   FROM _original_credit_activity c
                            JOIN (SELECT month_date calendar_month
                                  FROM edw_prod.data_model.dim_date) dm
                                 ON dm.calendar_month >= c.original_issued_month
                                     AND dm.calendar_month <= CURRENT_DATE()
                   WHERE credit_activity_type = 'Issued')
SELECT DISTINCT i.brand,
                i.region,
                i.country,
                i.is_extended,
                i.original_credit_tender,
                i.original_credit_type,
                i.original_credit_reason,
                i.original_issued_month,
                i.activity_amount                                  original_issued_amount,
                COALESCE(p.activity_month::DATE, i.calendar_month) activity_month,
                NVL(p."'Issued'", 0)                               issued,
                NVL(p."'Redeemed'", 0)                             redeemed,
                NVL(p."'Cancelled'", 0)                            cancelled,
                NVL(p."'Expired'", 0)                              expired
FROM _issuance i
         LEFT JOIN _original_credit_activity
    PIVOT (SUM(activity_amount)
    FOR credit_activity_type IN ('Issued','Redeemed', 'Cancelled', 'Expired')) p
                   ON i.brand = p.brand
                       AND i.region = p.region
                       AND i.country = p.country
                       AND i.is_extended = p.is_extended
                       AND i.original_credit_tender = p.original_credit_tender
                       AND i.original_credit_type = p.original_credit_type
                       AND i.original_credit_reason = p.original_credit_reason
                       AND i.activity_month = p.original_issued_month
                       AND i.calendar_month = p.activity_month;

CREATE OR REPLACE TEMPORARY TABLE _sx_base AS
SELECT brand,
       region,
       country,
       original_credit_tender,
       original_credit_reason,
       original_credit_type,
       original_issued_month,
       activity_month,
       DATEDIFF(MONTH, original_issued_month, activity_month) + 1 credit_tenure,
       SUM(original_issued_amount)                                original_issued_amount,
       SUM(issued)                                                issued,
       SUM(redeemed)                                              redeemed,
       SUM(cancelled)                                             cancelled,
       SUM(expired)                                               expired
FROM _pivot
WHERE original_credit_tender = 'Cash'
  AND ((original_credit_type = 'Fixed Credit' AND original_credit_reason = 'Membership Credit')
    OR (original_credit_type = 'Token' AND original_credit_reason = 'Token Billing'))
GROUP BY brand,
         region,
         country,
         original_credit_tender,
         original_credit_reason,
         original_credit_type,
         original_issued_month,
         activity_month,
         DATEDIFF(MONTH, original_issued_month, activity_month) + 1;


CREATE OR REPLACE TEMPORARY TABLE _sx_base_extended_only AS
SELECT brand,
       region,
       country,
       original_credit_tender,
       original_credit_reason,
       original_credit_type,
       original_issued_month,
       activity_month,
       DATEDIFF(MONTH, original_issued_month, activity_month) + 1 credit_tenure,
       SUM(original_issued_amount)                                original_issued_amount,
       SUM(issued)                                                issued,
       SUM(redeemed)                                              redeemed,
       SUM(cancelled)                                             cancelled,
       SUM(expired)                                               expired
FROM _pivot
WHERE original_credit_tender = 'Cash'
  AND ((original_credit_type = 'Fixed Credit' AND original_credit_reason = 'Membership Credit')
    OR (original_credit_type = 'Token' AND original_credit_reason = 'Token Billing'))
  AND is_extended = TRUE
GROUP BY brand,
         region,
         country,
         original_credit_tender,
         original_credit_reason,
         original_credit_type,
         original_issued_month,
         activity_month;


CREATE OR REPLACE TEMPORARY TABLE _extended_activities AS
WITH issuance_amount AS (SELECT brand,
                                region,
                                country,
                                original_credit_tender,
                                original_credit_reason,
                                original_credit_type,
                                original_issued_month,
                                SUM(issued) issued_to_date
                         FROM _sx_base_extended_only
                         WHERE original_credit_reason = 'Token Billing'
                           AND original_issued_month >= '2020-01-01'
                           AND activity_month <= $month_thru
                           AND original_issued_month <= $month_thru
                         GROUP BY brand,
                                  region,
                                  country,
                                  original_credit_tender,
                                  original_credit_reason,
                                  original_credit_type,
                                  original_issued_month)
   , _activity_amount_after_m14 AS (SELECT *
                                    FROM _sx_base_extended_only
                                    WHERE original_credit_reason = 'Token Billing'
                                      AND original_issued_month >= '2020-01-01'
                                      AND activity_month <= $month_thru
                                      AND original_issued_month <= $month_thru
                                      AND credit_tenure >= 14)
   , _expected_unredeem_rate_m19
    AS (SELECT brand,
               cumulative_unredeem_percent
                   - CASE
                         WHEN $month_thru < '2023-07-01' THEN 0.01
                         WHEN $month_thru = '2023-07-01' THEN 0.005
                         ELSE 0 END expected_unrd,
               cumulative_unredeem_amount
        FROM (SELECT brand,
                     credit_tenure,
                     SUM(unredeemed_percent)
                         OVER (PARTITION BY brand ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_unredeem_percent,
                     SUM(unredeemed_amount)
                         OVER (PARTITION BY brand ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_unredeem_amount
              FROM (SELECT brand,
                           credit_tenure,
                           SUM(original_issued_amount),
                           SUM(redeemed),
                           SUM(cancelled),
                           NVL(SUM(issued) - SUM(redeemed) - SUM(cancelled), 0) unredeemed_amount,
                           NVL((SUM(issued) - SUM(redeemed) - SUM(cancelled)), 0) /
                           SUM(original_issued_amount) AS                       unredeemed_percent
                    FROM _sx_base_extended_only
                    WHERE activity_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                      AND original_issued_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                    GROUP BY brand, credit_tenure))
        WHERE credit_tenure = 19)
   , _expected_unredeem_rate_m13
    AS (SELECT brand,
               cumulative_unredeem_percent
                   - CASE
                         WHEN $month_thru < '2023-07-01' THEN 0.01
                         WHEN $month_thru = '2023-07-01' THEN 0.005
                         ELSE 0 END expected_unrd,
               cumulative_unredeem_amount
        FROM (SELECT brand,
                     credit_tenure,
                     SUM(unredeemed_percent)
                         OVER (PARTITION BY brand ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_unredeem_percent,
                     SUM(unredeemed_amount)
                         OVER (PARTITION BY brand ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_unredeem_amount
              FROM (SELECT brand,
                           credit_tenure,
                           SUM(original_issued_amount),
                           SUM(redeemed),
                           SUM(cancelled),
                           NVL((SUM(issued) - SUM(redeemed) - SUM(cancelled)), 0) unredeemed_amount,
                           NVL((SUM(issued) - SUM(redeemed) - SUM(cancelled)), 0) /
                           SUM(original_issued_amount) AS                         unredeemed_percent
                    FROM _sx_base_extended_only
                    WHERE activity_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                      AND original_issued_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                    GROUP BY brand, credit_tenure))
        WHERE credit_tenure = 13)
SELECT i.brand,
       i.region,
       i.country,
       i.original_credit_tender,
       i.original_credit_reason,
       i.original_credit_type,
       i.original_issued_month,
       $month_thru                                                  activity_month,
       DATEDIFF(MONTH, i.original_issued_month, $month_thru) + 1    credit_tenure,
       ur19.expected_unrd                                           expected_unredeemed_rate_m19,
       ur13.expected_unrd                                           expected_unredeemed_rate_m13,
       ur13.expected_unrd - ur19.expected_unrd                      expected_additional_unredeemed_rate,
       i.issued_to_date                                             issued_for_extended,
       SUM(a.redeemed)                                              redeemed_after_m14,
       SUM(a.cancelled)                                             cancelled_after_m14,
       SUM(a.expired)                                               expired_after_m14,
       issued_to_date * 0.1378889919                                expected_activity_amount_m14to19,
       NVL(redeemed_after_m14
               + cancelled_after_m14,
           0)                                                       activity_amount_after_m14,
       expected_activity_amount_m14to19 - activity_amount_after_m14 expected_additional_activity_amount_m14to19,
       IFF(expected_activity_amount_m14to19 - activity_amount_after_m14 < 0 OR
           DATEDIFF(MONTH, i.original_issued_month, $month_thru) + 1 > 19, 0,
           expected_activity_amount_m14to19 -
           activity_amount_after_m14)                               expected_additional_activity_amount_m14to19_adjusted

FROM issuance_amount i
         LEFT JOIN _activity_amount_after_m14 a
                   ON i.brand = a.brand
                       AND i.original_issued_month = a.original_issued_month
                       AND i.original_credit_reason = a.original_credit_reason
                       AND i.original_credit_type = a.original_credit_type
                       AND i.original_credit_tender = a.original_credit_tender
                       AND i.region = a.region
                       AND i.country = a.country
         LEFT JOIN _expected_unredeem_rate_m19 ur19 ON i.brand = ur19.brand
         LEFT JOIN _expected_unredeem_rate_m13 ur13 ON i.brand = ur13.brand
GROUP BY i.brand,
         i.region,
         i.country,
         i.original_credit_tender,
         i.original_credit_reason,
         i.original_credit_type,
         i.original_issued_month,
         $month_thru,
         DATEDIFF(MONTH, i.original_issued_month, $month_thru) + 1,
         expected_unredeemed_rate_m19,
         expected_unredeemed_rate_m13,
         expected_additional_unredeemed_rate,
         issued_for_extended;


CREATE OR REPLACE TEMPORARY TABLE _append_sx_token AS
WITH _activity_amount AS (SELECT *
                          FROM _sx_base
                          WHERE original_credit_reason = 'Token Billing'
                            AND original_issued_month >= '2021-07-01'
                            AND activity_month <= $month_thru)
   , _expected_redemption_rate AS (SELECT credit_tenure,
                                          SUM(redeemed_percent)
                                              OVER (ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_redeemed_percent
                                   FROM (SELECT credit_tenure,
                                                NVL(SUM(redeemed)
                                                        / SUM(original_issued_amount), 0) AS redeemed_percent
                                         FROM _sx_base
                                         WHERE activity_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                           AND original_issued_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                         GROUP BY credit_tenure))
   , _expected_redemption_rate_19 AS (SELECT credit_tenure,
                                             SUM(redeemed_percent)
                                                 OVER (ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                                 +
                                             CASE
                                                 WHEN $month_thru < '2023-07-01' THEN 0.01
                                                 WHEN $month_thru = '2023-07-01' THEN 0.005
                                                 ELSE 0 END cumulative_redeemed_percent
                                      FROM (SELECT credit_tenure,
                                                   NVL(SUM(redeemed) / SUM(original_issued_amount), 0) AS redeemed_percent
                                            FROM _sx_base
                                            WHERE activity_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                              AND original_issued_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                            GROUP BY credit_tenure))
   , _expected_unredeem_rate
    AS (SELECT credit_tenure,
               SUM(unredeemed_percent)
                   OVER (ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                   -
               CASE
                   WHEN $month_thru < '2023-07-01' THEN 0.01
                   WHEN $month_thru = '2023-07-01' THEN 0.005
                   ELSE 0 END cumulative_unredeem_percent
        FROM (SELECT credit_tenure,
                     NVL((SUM(issued) - SUM(redeemed) - SUM(cancelled)), 0) /
                     SUM(original_issued_amount) AS unredeemed_percent
              FROM _sx_base
              WHERE activity_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                AND original_issued_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
              GROUP BY credit_tenure))
SELECT a.brand,
       a.region,
       a.country,
       a.original_credit_tender,
       a.original_credit_reason,
       a.original_credit_type,
       a.original_issued_month,
       a.original_issued_amount,
       $month_thru                                                                    activity_month,
       DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1                      credit_tenure,
       r12.cumulative_redeemed_percent                                                expected_redeemed_rate,
       ur.cumulative_unredeem_percent                                                 expected_unredeemed_rate,
       r19.cumulative_redeemed_percent                                                expected_redeemed_rate_max,

       SUM(issued)                                                                    issued_to_date,
       SUM(redeemed)                                                                  redeemed_to_date,
       SUM(cancelled)                                                                 cancelled_to_date,
       SUM(expired)                                                                   expired_to_date,
       issued_to_date - redeemed_to_date - cancelled_to_date                          unredeemed_to_date,
       redeemed_to_date / NULLIF(issued_to_date, 0)                                   redeemed_rate_to_date,

       redeemed_rate_to_date / r12.cumulative_redeemed_percent                        actual_redeemed_rate_vs_expected,

       SUM(ea.expected_additional_activity_amount_m14to19_adjusted)                   expected_additional_activity_amount_m14to19,

       case when datediff(MONTH, a.original_issued_month, $month_thru) + 1 > 13 then unredeemed_to_date - nvl(SUM(ea.expected_additional_activity_amount_m14to19_adjusted), 0)
            else iff(unredeemed_to_date < issued_to_date * expected_unredeemed_rate,unredeemed_to_date, issued_to_date * expected_unredeemed_rate)
               - nvl(SUM(ea.expected_additional_activity_amount_m14to19_adjusted), 0) end     expected_breakage_to_record,

        case when datediff(MONTH, a.original_issued_month, $month_thru) + 1 > 13 then 0
             when unredeemed_to_date < expected_unredeemed_rate * issued_to_date then 0
             else (expected_redeemed_rate_max * issued_to_date - redeemed_to_date) end         expected_additional_redeemed,

        case when datediff(MONTH, a.original_issued_month, $month_thru) + 1 > 13 then 0
             when unredeemed_to_date < expected_unredeemed_rate * issued_to_date then 0
             else ((1 - expected_unredeemed_rate - expected_redeemed_rate_max) * issued_to_date - cancelled_to_date) end expected_additional_cancelled,

       case when datediff(MONTH, a.original_issued_month, $month_thru) + 1 > 13 then expected_breakage_to_record
            else expected_breakage_to_record *
                iff(actual_redeemed_rate_vs_expected > 1, 1, actual_redeemed_rate_vs_expected) end breakage_recorded
FROM _activity_amount a
         JOIN _expected_redemption_rate r12 ON r12.credit_tenure = 12
         JOIN _expected_redemption_rate_19 r19 ON r19.credit_tenure = 13
         JOIN _expected_unredeem_rate ur ON ur.credit_tenure = 13
         LEFT JOIN _extended_activities ea
                   ON a.country = ea.country
                       AND a.region = ea.region
                       AND a.brand = ea.brand
                       AND a.original_issued_month = ea.original_issued_month
                       AND a.activity_month = ea.activity_month
                       AND a.original_credit_tender = ea.original_credit_tender
                       AND a.original_credit_reason = ea.original_credit_reason
                       AND a.original_credit_type = ea.original_credit_type
GROUP BY a.brand,
         a.region,
         a.country,
         a.original_credit_tender,
         a.original_credit_reason,
         a.original_credit_type,
         a.original_issued_month,
         a.original_issued_amount,
         $month_thru,
         DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1,
         expected_redeemed_rate,
         expected_unredeemed_rate,
         expected_redeemed_rate_max;

MERGE INTO shared.savagex_token_breakage_report t
    USING _append_sx_token a
    ON t.brand = a.brand AND
       t.region = a.region AND
       t.country = a.country AND
       t.original_credit_tender = a.original_credit_tender AND
       t.original_credit_reason = a.original_credit_reason AND
       t.original_credit_type = a.original_credit_type AND
       t.original_issued_month = a.original_issued_month AND
       t.activity_month = a.activity_month AND
       t.credit_tenure = a.credit_tenure
    WHEN NOT MATCHED THEN
        INSERT (brand, region, country, original_credit_tender, original_credit_reason, original_credit_type,
                original_issued_amount,
                original_issued_month, activity_month,
                credit_tenure, issued_to_date, redeemed_to_date, cancelled_to_date, expired_to_date, unredeemed_to_date,
                redeemed_rate_to_date,
                expected_redeemed_rate, expected_unredeemed_rate, actual_redeemed_rate_vs_expected,
                expected_breakage_to_record, breakage_recorded, expected_redeemed_rate_max,
                expected_additional_redeemed, expected_additional_cancelled,
                expected_additional_activity_amount_m14to19)
            VALUES (brand, region, country, original_credit_tender, original_credit_reason, original_credit_type,
                    original_issued_amount,
                    original_issued_month, activity_month,
                    credit_tenure, issued_to_date, redeemed_to_date, cancelled_to_date, expired_to_date,
                    unredeemed_to_date, redeemed_rate_to_date,
                    expected_redeemed_rate, expected_unredeemed_rate, actual_redeemed_rate_vs_expected,
                    expected_breakage_to_record, breakage_recorded, expected_redeemed_rate_max,
                    expected_additional_redeemed, expected_additional_cancelled,
                    expected_additional_activity_amount_m14to19)
    WHEN MATCHED AND
        (NOT EQUAL_NULL(t.issued_to_date, a.issued_to_date) OR
         NOT EQUAL_NULL(t.redeemed_to_date, a.redeemed_to_date) OR
         NOT EQUAL_NULL(t.cancelled_to_date, a.cancelled_to_date) OR
         NOT EQUAL_NULL(t.expired_to_date, a.expired_to_date) OR
         NOT EQUAL_NULL(t.unredeemed_to_date, a.unredeemed_to_date) OR
         NOT EQUAL_NULL(t.expected_redeemed_rate, a.expected_redeemed_rate) OR
         NOT EQUAL_NULL(t.expected_unredeemed_rate, a.expected_unredeemed_rate) OR
         NOT EQUAL_NULL(t.redeemed_rate_to_date, a.redeemed_rate_to_date) OR
         NOT EQUAL_NULL(t.actual_redeemed_rate_vs_expected, a.actual_redeemed_rate_vs_expected) OR
         NOT EQUAL_NULL(t.expected_breakage_to_record, a.expected_breakage_to_record) OR
         NOT EQUAL_NULL(t.breakage_recorded, a.breakage_recorded) OR
         NOT EQUAL_NULL(t.expected_redeemed_rate_max, a.expected_redeemed_rate_max) OR
         NOT EQUAL_NULL(t.expected_additional_redeemed, a.expected_additional_redeemed) OR
         NOT EQUAL_NULL(t.expected_additional_cancelled, a.expected_additional_cancelled) OR
         NOT EQUAL_NULL(t.expected_additional_activity_amount_m14to19, a.expected_additional_activity_amount_m14to19))
        THEN
        UPDATE
            SET t.issued_to_date = a.issued_to_date,
                t.redeemed_to_date = a.redeemed_to_date,
                t.cancelled_to_date = a.cancelled_to_date,
                t.expired_to_date = a.expired_to_date,
                t.unredeemed_to_date = a.unredeemed_to_date,
                t.redeemed_rate_to_date = a.redeemed_rate_to_date,
                t.expected_redeemed_rate = a.expected_redeemed_rate,
                t.expected_unredeemed_rate = a.expected_unredeemed_rate,
                t.actual_redeemed_rate_vs_expected = a.actual_redeemed_rate_vs_expected,
                t.expected_breakage_to_record = a.expected_breakage_to_record,
                t.breakage_recorded = a.breakage_recorded,
                t.expected_redeemed_rate_max = a.expected_redeemed_rate_max,
                t.expected_additional_redeemed = a.expected_additional_redeemed,
                t.expected_additional_cancelled = a.expected_additional_cancelled,
                t.expected_additional_activity_amount_m14to19 = a.expected_additional_activity_amount_m14to19;
