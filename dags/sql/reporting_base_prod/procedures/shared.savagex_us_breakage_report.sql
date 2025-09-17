SET
    month_thru = DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE));

CREATE
    OR REPLACE TEMPORARY TABLE _original_credit_activity AS
SELECT st.store_brand                                                       AS brand,
       st.store_country                                                     AS country,
       st.store_region                                                      AS region,
       fce.credit_activity_type,
       dcf.original_credit_tender,
       dcf.original_credit_type,
       dcf.original_credit_reason,
       iff(upper(dcf2.credit_state) in ('DC','ME'), 'Cant Recognize', dcf2.deferred_recognition_label_credit) as deferred_recognition_label_credit,
       DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       SUM(fce.credit_activity_gross_vat_local_amount)                      AS activity_amount
FROM shared.dim_credit dcf
         JOIN shared.dim_credit dcf2 ON dcf.original_credit_key = dcf2.credit_key
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled', 'Expired')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand = 'Savage X'
  AND st.store_country = 'US'
GROUP BY st.store_brand,
         st.store_country,
         st.store_region,
         fce.credit_activity_type,
         dcf.original_credit_tender,
         dcf.original_credit_type,
         dcf.original_credit_reason,
         iff(upper(dcf2.credit_state) in ('DC','ME'), 'Cant Recognize', dcf2.deferred_recognition_label_credit),
         DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE),
         DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE);


CREATE
    OR REPLACE TEMPORARY TABLE _pivot AS
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
                i.original_credit_tender,
                i.original_credit_type,
                i.original_credit_reason,
                i.deferred_recognition_label_credit,
                i.original_issued_month,
                i.activity_amount                                  original_issued_amount,
                COALESCE(p.activity_month::DATE, i.calendar_month) activity_month,
                NVL(p."'Issued'", 0)                               issued,
                NVL(p."'Redeemed'", 0)                             redeemed,
                NVL(p."'Cancelled'", 0)                            cancelled,
                NVL(p."'Expired'", 0)                              expired
FROM _issuance i
         LEFT JOIN _original_credit_activity
    PIVOT (SUM(activity_amount) FOR credit_activity_type IN ('Issued','Redeemed', 'Cancelled', 'Expired')) p
                   ON i.brand = p.brand
                       AND i.region = p.region
                       AND i.country = p.country
                       AND i.original_credit_tender = p.original_credit_tender
                       AND i.original_credit_type = p.original_credit_type
                       AND i.original_credit_reason = p.original_credit_reason
                       AND i.deferred_recognition_label_credit = p.deferred_recognition_label_credit
                       AND i.activity_month = p.original_issued_month
                       AND i.calendar_month = p.activity_month;



CREATE
    OR REPLACE TEMPORARY TABLE _sx_base AS
SELECT brand,
       region,
       country,
       original_credit_tender,
       'Membership Credit or Token Billing with Refund' AS        original_credit_reason,
       deferred_recognition_label_credit,
       'Fixed Credit or Token'                          AS        original_credit_type,
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
    OR (original_credit_type = 'Token' AND original_credit_reason = 'Token Billing')
    OR (original_credit_type = 'Token' AND original_credit_reason = 'Refund'))
GROUP BY brand,
         region,
         country,
         original_credit_tender,
         'Membership Credit or Token Billing with Refund',
         deferred_recognition_label_credit,
         'Fixed Credit or Token',
         original_issued_month,
         activity_month,
         DATEDIFF(MONTH, original_issued_month, activity_month) + 1;


CREATE
    OR REPLACE TEMPORARY TABLE _append_sx_us AS
WITH _activity_amount AS (SELECT *
                          FROM _sx_base
                          WHERE original_issued_month >= '2019-02-01'
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
   , _expected_redemption_rate_24 AS (SELECT credit_tenure,
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
       a.deferred_recognition_label_credit,
       a.original_credit_type,
       a.original_issued_month,
       a.original_issued_amount,
       $month_thru                                               activity_month,
       DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 credit_tenure,

       r12.cumulative_redeemed_percent                           expected_redeemed_rate,
       ur.cumulative_unredeem_percent                            expected_unredeemed_rate,
       r24.cumulative_redeemed_percent                           expected_redeemed_rate_max,

       SUM(a.issued)                                             issued_to_date,
       SUM(a.redeemed)                                           redeemed_to_date,
       SUM(a.cancelled)                                          cancelled_to_date,
       SUM(a.expired)                                            expired_to_date,

       issued_to_date - redeemed_to_date - cancelled_to_date     unredeemed_to_date,
       redeemed_to_date / NULLIF(issued_to_date, 0)              redeemed_rate_to_date,
       redeemed_rate_to_date / r12.cumulative_redeemed_percent   actual_redeemed_rate_vs_expected,

       0                                         AS              expected_additional_activity_amount_m14to19,

       issued_to_date * expected_unredeemed_rate AS              expected_unredeemed,

       CASE
           WHEN a.deferred_recognition_label_credit = 'Recognize 40%' AND
                DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 24
               THEN 0.6 * (unredeemed_to_date - expired_to_date)
           WHEN a.deferred_recognition_label_credit = 'Cant Recognize' AND
                DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 24
               THEN (unredeemed_to_date - expired_to_date)
           WHEN a.deferred_recognition_label_credit = 'Recognize 40%'
               THEN IFF((unredeemed_to_date - expired_to_date) < issued_to_date * expected_unredeemed_rate,
                        0.6 * (unredeemed_to_date - expired_to_date), 0.6 * (issued_to_date * expected_unredeemed_rate))
           WHEN a.deferred_recognition_label_credit = 'Cant Recognize'
               THEN (IFF((unredeemed_to_date - expired_to_date) < issued_to_date * expected_unredeemed_rate,
                         (unredeemed_to_date - expired_to_date), (issued_to_date * expected_unredeemed_rate)))
           ELSE 0 END
                                                                 expected_cannot_recognize,

       CASE
           WHEN a.deferred_recognition_label_credit = 'Recognize' AND
                DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 24 THEN unredeemed_to_date
           WHEN a.deferred_recognition_label_credit = 'Recognize 40%' AND
                DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 24
               THEN (.4 * (unredeemed_to_date - expired_to_date)) + expired_to_date
           WHEN a.deferred_recognition_label_credit = 'Recognize' THEN
                   (IFF((unredeemed_to_date - expired_to_date) < issued_to_date * expected_unredeemed_rate,
                        (unredeemed_to_date - expired_to_date), issued_to_date * expected_unredeemed_rate)) +
                   expired_to_date
           WHEN a.deferred_recognition_label_credit = 'Recognize 40%' THEN (0.4 * (IFF(
                   (unredeemed_to_date - expired_to_date) < issued_to_date * expected_unredeemed_rate,
                   (unredeemed_to_date - expired_to_date), issued_to_date * expected_unredeemed_rate)) +
                                                                            expired_to_date)
           ELSE expired_to_date END                              expected_breakage_to_record,

       CASE
           WHEN DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 24 THEN 0
           WHEN (unredeemed_to_date - expired_to_date) < expected_unredeemed_rate * issued_to_date THEN 0
           ELSE (expected_redeemed_rate_max * issued_to_date - redeemed_to_date -
                 expired_to_date) END                            expected_additional_redeemed,

       CASE
           WHEN DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 24 THEN 0
           WHEN (unredeemed_to_date - expired_to_date) < expected_unredeemed_rate * issued_to_date THEN 0
           ELSE ((1 - expected_unredeemed_rate - expected_redeemed_rate_max) * issued_to_date -
                 cancelled_to_date) END                          expected_additional_cancelled,

       CASE
           WHEN DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 24 THEN expected_breakage_to_record
           WHEN a.deferred_recognition_label_credit = 'Recognize' THEN
                       (IFF((unredeemed_to_date - expired_to_date) < issued_to_date * expected_unredeemed_rate,
                            (unredeemed_to_date - expired_to_date), issued_to_date * expected_unredeemed_rate)) *
                       IFF(actual_redeemed_rate_vs_expected > 1, 1, actual_redeemed_rate_vs_expected) + expired_to_date
           WHEN a.deferred_recognition_label_credit = 'Recognize 40%' THEN .4 * (IFF(
                   (unredeemed_to_date - expired_to_date) < issued_to_date * expected_unredeemed_rate,
                   (unredeemed_to_date - expired_to_date), issued_to_date * expected_unredeemed_rate)) *
                                                                           IFF(actual_redeemed_rate_vs_expected > 1, 1,
                                                                               actual_redeemed_rate_vs_expected) +
                                                                           expired_to_date
           ELSE expected_breakage_to_record END                  breakage_recorded,

       NULL                                      AS              projected_redeem_breakage_tenure_cmc_legacy,
       NULL                                      AS              projected_unredeem_breakage_tenure_cmc_legacy

FROM _activity_amount a
         LEFT JOIN _expected_redemption_rate r12 ON r12.credit_tenure = 12
         LEFT JOIN _expected_redemption_rate_24 r24 ON r24.credit_tenure = 24
         LEFT JOIN _expected_unredeem_rate ur ON ur.credit_tenure = 24
GROUP BY a.brand,
         a.region,
         a.country,
         a.original_credit_tender,
         a.original_credit_reason,
         a.deferred_recognition_label_credit,
         a.original_credit_type,
         a.original_issued_month,
         a.original_issued_amount,
         $month_thru,
         DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1,
         expected_redeemed_rate,
         expected_unredeemed_rate,
         expected_redeemed_rate_max;

DELETE FROM shared.savagex_us_breakage_report
where activity_month = $month_thru;


INSERT INTO shared.savagex_us_breakage_report
SELECT brand,
       region,
       country,
       original_credit_tender,
       original_credit_reason,
       original_credit_type,
       original_issued_month,
       original_issued_amount,
       activity_month,
       credit_tenure,
       issued_to_date,
       redeemed_to_date,
       cancelled_to_date,
       expired_to_date,
       unredeemed_to_date,
       redeemed_rate_to_date,
       expected_redeemed_rate,
       expected_unredeemed_rate,
       expected_redeemed_rate_max,
       actual_redeemed_rate_vs_expected,
       expected_additional_redeemed,
       expected_additional_cancelled,
       expected_additional_activity_amount_m14to19,
       expected_breakage_to_record,
       breakage_recorded,
       projected_redeem_breakage_tenure_cmc_legacy,
       projected_unredeem_breakage_tenure_cmc_legacy,
       deferred_recognition_label_credit,
       expected_unredeemed,
       expected_cannot_recognize
FROM _append_sx_us;
