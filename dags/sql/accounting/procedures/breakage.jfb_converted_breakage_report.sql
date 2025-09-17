SET month_thru = DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE));

CREATE OR REPLACE TEMPORARY TABLE _original_credit_activity AS
SELECT CASE
           WHEN store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
           WHEN store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE store_brand END                                             AS brand,
       store_country                                                        AS country,
       store_region                                                         AS region,
       dcf.giftco_transfer_status,
       credit_activity_type,
       original_credit_tender,
       original_credit_type,
       original_credit_reason,
       DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       SUM(fce.credit_activity_gross_vat_local_amount)                      AS activity_amount
FROM reporting_base_prod.shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN reporting_base_prod.shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled', 'Expired')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand IN ('JustFab', 'FabKids', 'ShoeDazzle')
  AND st.store_country IN ('US')
  AND dcf.giftco_transfer_status <> 'Membership Credit to Giftco'
GROUP BY brand,
         country,
         region,
         dcf.giftco_transfer_status,
         credit_activity_type,
         original_credit_tender,
         original_credit_type,
         original_credit_reason,
         original_issued_month,
         activity_month;

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
                i.giftco_transfer_status,
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
    PIVOT (SUM(activity_amount) FOR credit_activity_type IN ('Issued','Redeemed', 'Cancelled', 'Expired')) p
                   ON i.brand = p.brand
                       AND i.region = p.region
                       AND i.country = p.country
                       AND i.giftco_transfer_status = p.giftco_transfer_status
                       AND i.original_credit_tender = p.original_credit_tender
                       AND i.original_credit_type = p.original_credit_type
                       AND i.original_credit_reason = p.original_credit_reason
                       AND i.activity_month = p.original_issued_month
                       AND i.calendar_month = p.activity_month;

CREATE OR REPLACE TEMPORARY TABLE _jfb_converted_base AS
SELECT brand,
       region,
       country,
       giftco_transfer_status,
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
  AND original_credit_type = 'Fixed Credit'
  AND original_credit_reason = 'Membership Credit'
GROUP BY brand,
         region,
         country,
         giftco_transfer_status,
         original_credit_tender,
         original_credit_reason,
         original_credit_type,
         original_issued_month,
         activity_month,
         DATEDIFF(MONTH, original_issued_month, activity_month) + 1;

CREATE OR REPLACE TEMPORARY TABLE _append_jfb_converted AS
WITH _activity_amount AS (SELECT *
                          FROM _jfb_converted_base
                          WHERE activity_month <= $month_thru),
     _expected_redemption_rate AS (SELECT brand,
                                          country,
                                          credit_tenure,
                                          SUM(redeemed_percent)
                                              OVER (PARTITION BY brand, country ORDER BY credit_tenure
                                                  ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_redeemed_percent
                                   FROM (SELECT brand,
                                                country,
                                                credit_tenure,
                                                NVL(SUM(redeemed)
                                                        / SUM(original_issued_amount), 0) AS redeemed_percent
                                         FROM _jfb_converted_base
                                         WHERE DECODE(brand || ' ' || country, 'JustFab US',
                                                      activity_month BETWEEN '2020-08-01' AND $month_thru,
                                                      activity_month BETWEEN '2020-10-01' AND $month_thru)
                                           AND DECODE(brand || ' ' || country, 'JustFab US',
                                                      original_issued_month BETWEEN '2020-08-01' AND '2023-10-01',
                                                      original_issued_month BETWEEN '2020-10-01' AND '2023-09-01')
                                         GROUP BY brand, country, credit_tenure)),
     _expected_redemption_rate_max AS (SELECT brand,
                                              country,
                                              credit_tenure,
                                              SUM(redeemed_percent)
                                                  OVER (PARTITION BY brand, country ORDER BY credit_tenure
                                                      ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_redeemed_percent
                                       FROM (SELECT brand,
                                                    country,
                                                    credit_tenure,
                                                    NVL(SUM(redeemed) / SUM(original_issued_amount), 0) AS redeemed_percent
                                             FROM (SELECT brand,
                                                          country,
                                                          original_issued_month,
                                                          original_issued_amount,
                                                          activity_month,
                                                          credit_tenure,
                                                          redeemed
                                                   FROM _jfb_converted_base)
                                             WHERE DECODE(brand || ' ' || country, 'JustFab US',
                                                          activity_month BETWEEN '2020-08-01' AND $month_thru,
                                                          activity_month BETWEEN '2020-10-01' AND $month_thru)
                                               AND DECODE(brand || ' ' || country, 'JustFab US',
                                                          original_issued_month BETWEEN '2020-08-01' AND '2023-10-01',
                                                          original_issued_month BETWEEN '2020-10-01' AND '2023-09-01')
                                             GROUP BY brand, country, credit_tenure)),
     _expected_unredeem_rate AS (SELECT brand,
                                        country,
                                        credit_tenure,
                                        SUM(unredeemed_percent)
                                            OVER (PARTITION BY brand, country ORDER BY credit_tenure
                                                ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  cumulative_unredeem_percent
                                 FROM (SELECT brand,
                                              country,
                                              credit_tenure,
                                              NVL((SUM(issued) - SUM(redeemed) - SUM(cancelled)), 0) /
                                              SUM(original_issued_amount) AS unredeemed_percent
                                       FROM _jfb_converted_base
                                       WHERE DECODE(brand || ' ' || country, 'JustFab US',
                                                    activity_month BETWEEN '2020-08-01' AND $month_thru,
                                                    activity_month BETWEEN '2020-10-01' AND $month_thru)
                                         AND DECODE(brand || ' ' || country, 'JustFab US',
                                                    original_issued_month BETWEEN '2020-08-01' AND '2023-10-01',
                                                    original_issued_month BETWEEN '2020-10-01' AND '2023-09-01')
                                       GROUP BY brand, country, credit_tenure))
SELECT iff(giftco_transfer_status='None', a.brand || ' ' || a.country,
       a.brand || ' ' || a.country || ' ' || a.giftco_transfer_status)                     business_unit,
       a.brand,
       a.region,
       a.country,
       a.original_credit_tender,
       a.original_credit_reason                                                            store_credit_reason,
       a.original_credit_type,
       a.original_issued_month                                                             issued_month,
       'Recognize'                                                                         deferred_recognition_label,
       'Local Gross VAT'                                                                   currency,
       DATEDIFF(MONTH, original_issued_month, $month_thru) + 1                             credit_tenure,
       $month_thru                                                                         recognition_booking_month,
       a.original_issued_amount,
       IFF(DATEDIFF(MONTH, original_issued_month, $month_thru) + 1 > 24, NULL,
           r12.cumulative_redeemed_percent)                                                expected_redeemed_rate,
       IFF(DATEDIFF(MONTH, original_issued_month, $month_thru) + 1 > 24, NULL,
           IFF((a.brand || ' ' || a.country = 'JustFab US' AND DATEDIFF(MONTH, original_issued_month, '2023-10-01') < 9)
                   OR
               (a.brand || ' ' || a.country <> 'JustFab US' AND
                DATEDIFF(MONTH, original_issued_month, '2023-09-01') < 6),
               ur24.cumulative_unredeem_percent,
               ur36.cumulative_unredeem_percent))                                          expected_unredeemed_rate,
       IFF(DATEDIFF(MONTH, original_issued_month, $month_thru) + 1 > 24, NULL,
           IFF((a.brand || ' ' || a.country = 'JustFab US' AND DATEDIFF(MONTH, original_issued_month, '2023-10-01') < 9)
                   OR
               (a.brand || ' ' || a.country <> 'JustFab US' AND
                DATEDIFF(MONTH, original_issued_month, '2023-09-01') < 6),
               r24.cumulative_redeemed_percent,
               r36.cumulative_redeemed_percent))                                           expected_redeemed_rate_max,
       SUM(issued)                                                                         issued_to_date,
       SUM(redeemed)                                                                       redeemed_to_date,
       SUM(cancelled)                                                                      cancelled_to_date,
       SUM(expired)                                                                        expired_to_date,
       issued_to_date - redeemed_to_date - cancelled_to_date                               unredeemed_to_date,
       redeemed_to_date / NULLIF(issued_to_date, 0)                                        redeemed_rate_to_date,
       cancelled_to_date / NULLIF(issued_to_date, 0)                                       cancelled_to_date_pct,
       unredeemed_to_date / NULLIF(issued_to_date, 0)                                      unredeemed_to_date_pct,
       redeemed_rate_to_date /
       NULLIF(expected_redeemed_rate,
              0)                                                                           actual_redeemed_rate_vs_expected,
       1 - expected_redeemed_rate_max - expected_unredeemed_rate                           assumption_cumulative_cancelled_pct,
       IFF(DATEDIFF(MONTH, original_issued_month, $month_thru) + 1 <= 24,
           IFF(unredeemed_to_date < expected_unredeemed_rate * issued_to_date, 0,
               expected_redeemed_rate_max * issued_to_date - redeemed_to_date),
           0)                                                                              expected_additional_redeemed,
       IFF(DATEDIFF(MONTH, original_issued_month, $month_thru) + 1 <= 24,
           IFF(unredeemed_to_date < expected_unredeemed_rate * issued_to_date, 0,
               (1 - expected_unredeemed_rate - expected_redeemed_rate_max) * issued_to_date -
               cancelled_to_date),
           0)                                                                              expected_additional_cancelled,
       IFF((DATEDIFF(MONTH, original_issued_month, $month_thru) + 1) > 24, unredeemed_to_date,
           IFF(unredeemed_to_date < issued_to_date * expected_unredeemed_rate,
               unredeemed_to_date, issued_to_date *
                                   expected_unredeemed_rate))                              expected_breakage,
       IFF((DATEDIFF(MONTH, original_issued_month, $month_thru) + 1) > 24,
           unredeemed_to_date,
           expected_breakage *
           IFF(actual_redeemed_rate_vs_expected > 1, 1, actual_redeemed_rate_vs_expected)) breakage_to_record,
       0                                                                                   change_in_breakage_to_record,
       'Converted Credits'                                                                 breakage_type
FROM _activity_amount a
         JOIN _expected_redemption_rate r12
              ON r12.credit_tenure = 12 AND a.brand = r12.brand AND a.country = r12.country
         JOIN _expected_redemption_rate_max r36
              ON r36.credit_tenure = 36 AND a.brand = r36.brand AND a.country = r36.country
         JOIN _expected_redemption_rate_max r24
              ON r24.credit_tenure = 24 AND a.brand = r24.brand AND a.country = r24.country
         JOIN _expected_unredeem_rate ur36
              ON ur36.credit_tenure = 36 AND a.brand = ur36.brand AND a.country = ur36.country
         JOIN _expected_unredeem_rate ur24
              ON ur24.credit_tenure = 24 AND a.brand = ur24.brand AND a.country = ur24.country
GROUP BY iff(giftco_transfer_status='None', a.brand || ' ' || a.country,
         a.brand || ' ' || a.country || ' ' || a.giftco_transfer_status),
         a.brand,
         a.region,
         a.country,
         a.original_credit_tender,
         a.original_credit_reason,
         a.original_credit_type,
         a.original_issued_month,
         $month_thru,
         DATEDIFF(MONTH, original_issued_month, $month_thru) + 1,
         a.original_issued_amount,
         expected_redeemed_rate,
         expected_unredeemed_rate,
         expected_redeemed_rate_max
;

DELETE FROM breakage.jfb_converted_breakage_report
where recognition_booking_month = $month_thru;

INSERT INTO breakage.jfb_converted_breakage_report
SELECT business_unit,
       brand,
       region,
       country,
       original_credit_tender,
       store_credit_reason,
       original_credit_type,
       issued_month,
       deferred_recognition_label,
       currency,
       original_issued_amount,
       credit_tenure,
       recognition_booking_month,
       issued_to_date,
       redeemed_to_date,
       cancelled_to_date,
       expired_to_date,
       unredeemed_to_date,
       redeemed_rate_to_date,
       cancelled_to_date_pct,
       unredeemed_to_date_pct,
       expected_redeemed_rate,
       expected_unredeemed_rate,
       expected_redeemed_rate_max,
       actual_redeemed_rate_vs_expected,
       assumption_cumulative_cancelled_pct,
       expected_additional_redeemed,
       expected_additional_cancelled,
       expected_breakage,
       breakage_to_record,
       change_in_breakage_to_record,
       breakage_type
FROM _append_jfb_converted;

INSERT INTO breakage.jfb_converted_breakage_report_snapshot
SELECT business_unit,
       brand,
       region,
       country,
       original_credit_tender,
       store_credit_reason,
       original_credit_type,
       issued_month,
       deferred_recognition_label,
       currency,
       original_issued_amount,
       credit_tenure,
       recognition_booking_month,
       issued_to_date,
       redeemed_to_date,
       cancelled_to_date,
       expired_to_date,
       unredeemed_to_date,
       redeemed_rate_to_date,
       cancelled_to_date_pct,
       unredeemed_to_date_pct,
       expected_redeemed_rate,
       expected_unredeemed_rate,
       expected_redeemed_rate_max,
       actual_redeemed_rate_vs_expected,
       assumption_cumulative_cancelled_pct,
       expected_additional_redeemed,
       expected_additional_cancelled,
       expected_breakage,
       breakage_to_record,
       change_in_breakage_to_record,
       breakage_type,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM breakage.jfb_converted_breakage_report;

DELETE
FROM breakage.jfb_converted_breakage_report_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
