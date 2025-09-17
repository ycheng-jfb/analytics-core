-- SHOPIFY_TEST.PUBLIC.
CREATE OR REPLACE TEMPORARY TABLE SHOPIFY_TEST.PUBLIC.token_original_credit_activity AS
SELECT CASE
           WHEN store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
           WHEN store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE store_brand END                                             AS brand,
       store_country                                                        AS country,
       store_region                                                         AS region,
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
  AND st.store_brand IN ('JustFab','FabKids','ShoeDazzle')
  AND st.store_country IN ('US')
GROUP BY brand,
         country,
         region,
         credit_activity_type,
         original_credit_tender,
         original_credit_type,
         original_credit_reason,
         DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE),
         DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE);


CREATE OR REPLACE TABLE SHOPIFY_TEST.PUBLIC.token_pivot AS
SELECT DISTINCT i.brand,
                i.region,
                i.country,
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
FROM (SELECT *
                   FROM SHOPIFY_TEST.PUBLIC.token_original_credit_activity c
                            JOIN (SELECT month_date calendar_month
                                  FROM EDW_PROD.DATA_MODEL_JFB.DIM_DATE) dm
                                 ON dm.calendar_month >= c.original_issued_month
                                     AND dm.calendar_month <= CURRENT_DATE()
                   WHERE credit_activity_type = 'Issued') i
         LEFT JOIN SHOPIFY_TEST.PUBLIC.token_original_credit_activity
    PIVOT (SUM(activity_amount)
    FOR credit_activity_type IN ('Issued','Redeemed', 'Cancelled', 'Expired')) p
                   ON i.brand = p.brand
                       AND i.region = p.region
                       AND i.country = p.country
                       AND i.original_credit_tender = p.original_credit_tender
                       AND i.original_credit_type = p.original_credit_type
                       AND i.original_credit_reason = p.original_credit_reason
                       AND i.activity_month = p.original_issued_month
                       AND i.calendar_month = p.activity_month;

CREATE OR REPLACE TEMPORARY TABLE SHOPIFY_TEST.PUBLIC.token_jfb_token_base AS
SELECT brand,
       region,
       country,
       original_credit_tender,
       original_credit_reason,
       original_credit_type,
       original_issued_month,
       original_issued_amount,
       activity_month,
       DATEDIFF(MONTH, original_issued_month, activity_month) + 1 credit_tenure,
       SUM(issued)                                                issued,
       SUM(redeemed)                                              redeemed,
       SUM(cancelled)                                             cancelled,
       SUM(expired)                                               expired
FROM SHOPIFY_TEST.PUBLIC.token_pivot
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
         original_issued_amount,
         activity_month,
         DATEDIFF(MONTH, original_issued_month, activity_month) + 1;

CREATE OR REPLACE TEMPORARY TABLE SHOPIFY_TEST.PUBLIC.token_append_jfb_token AS
SELECT a.brand || ' ' || a.country                                                 business_unit,
       a.brand,
       a.region,
       a.country,
       a.original_credit_tender,
       a.original_credit_reason                                                    store_credit_reason,
       a.original_credit_type,
       a.original_issued_month                                                     issued_month,
       'Recognize'                                                                 deferred_recognition_label,
       'Local Gross VAT'                                                           currency,
       a.original_issued_amount,
       DATEDIFF(MONTH, original_issued_month, DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))) + 1                     credit_tenure,
       DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))                                                                 recognition_booking_month,
       SUM(issued)                                                                 issued_to_date,
       SUM(redeemed)                                                               redeemed_to_date,
       SUM(cancelled)                                                              cancelled_to_date,
       SUM(expired)                                                                expired_to_date,
       issued_to_date - redeemed_to_date - cancelled_to_date                       unredeemed_to_date,
       redeemed_to_date / NULLIF(issued_to_date, 0)                                redeemed_rate_to_date,
       cancelled_to_date / NULLIF(issued_to_date, 0)                               cancelled_to_date_pct,
       unredeemed_to_date / NULLIF(issued_to_date, 0)                              unredeemed_to_date_pct,
       r12.cumulative_redeemed_percent                                             expected_redeemed_rate,
       ur.cumulative_unredeem_percent                                              expected_unredeemed_rate,
       r13.cumulative_redeemed_percent                                             expected_redeemed_rate_max,
       redeemed_rate_to_date / r12.cumulative_redeemed_percent                     actual_redeemed_rate_vs_expected,
       1 - expected_redeemed_rate_max - expected_unredeemed_rate                   assumption_cumulative_cancelled_pct,
       IFF(DATEDIFF(MONTH, original_issued_month, DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))) + 1 <= 13,
           IFF(unredeemed_to_date < expected_unredeemed_rate * issued_to_date, 0,
               expected_redeemed_rate_max * issued_to_date - redeemed_to_date), 0) expected_additional_redeemed,
       IFF(DATEDIFF(MONTH, original_issued_month, DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))) + 1 <= 13,
           IFF(unredeemed_to_date < expected_unredeemed_rate * issued_to_date, 0,
               (1 - expected_unredeemed_rate - expected_redeemed_rate_max)
                   * issued_to_date - cancelled_to_date), 0)                       expected_additional_cancelled,

       IFF(DATEDIFF(MONTH, original_issued_month, DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))) + 1 <= 13,
           IFF(unredeemed_to_date < issued_to_date * expected_unredeemed_rate,
               unredeemed_to_date,
               issued_to_date * expected_unredeemed_rate), unredeemed_to_date)     expected_breakage,
       IFF(DATEDIFF(MONTH, original_issued_month, DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))) + 1 <= 13,
           expected_breakage *
           IFF(actual_redeemed_rate_vs_expected > 1, 1, actual_redeemed_rate_vs_expected),
           expected_breakage)                                                      breakage_to_record,
       IFF(RANK() OVER (PARTITION BY a.brand,
           region,
           a.country,
           original_credit_tender,
           store_credit_reason,
           original_credit_type,
           issued_month
           ORDER BY recognition_booking_month) = 1, breakage_to_record,
           (breakage_to_record - LAG(breakage_to_record)
                                     OVER (PARTITION BY a.brand,
                                         region,
                                         a.country,
                                         original_credit_tender,
                                         store_credit_reason,
                                         original_credit_type,
                                         issued_month
                                         ORDER BY recognition_booking_month)))     change_in_breakage_to_record,
       'Token'                                                                     breakage_type
FROM (SELECT *
                          FROM SHOPIFY_TEST.PUBLIC.token_jfb_token_base
                          WHERE original_credit_reason = 'Token Billing'
                            AND activity_month <= DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))) a
         JOIN (SELECT brand,
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
                                         FROM SHOPIFY_TEST.PUBLIC.token_jfb_token_base
                                         WHERE activity_month BETWEEN DATEADD(MONTH, -23, DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))) AND DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))
                                           AND original_issued_month BETWEEN DATEADD(MONTH, -23, DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))) AND DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))
                                         GROUP BY brand, country, credit_tenure)) r12 ON r12.credit_tenure = 12 and a.brand = r12.brand AND a.country = r12.country
         JOIN (SELECT brand,
                                             country,
                                             credit_tenure,
                                             SUM(redeemed_percent)
                                                 OVER (PARTITION BY brand, country ORDER BY credit_tenure
                                                     ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  cumulative_redeemed_percent
                                      FROM (SELECT brand,
                                                   country,
                                                   credit_tenure,
                                                   NVL(SUM(redeemed) / SUM(original_issued_amount), 0) AS redeemed_percent
                                            FROM SHOPIFY_TEST.PUBLIC.token_jfb_token_base
                                            WHERE activity_month BETWEEN DATEADD(MONTH, -23, DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))) AND DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))
                                              AND original_issued_month BETWEEN DATEADD(MONTH, -23, DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))) AND DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))
                                            GROUP BY brand, country, credit_tenure))  r13 ON r13.credit_tenure = 13  and a.brand = r13.brand AND a.country = r13.country
         JOIN (SELECT brand,
                                        country,
                                        credit_tenure,
                                        SUM(unredeemed_percent)
                                            OVER (PARTITION BY brand, country ORDER BY credit_tenure
                                                ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_unredeem_percent
                                 FROM (SELECT brand,
                                              country,
                                              credit_tenure,
                                              NVL((SUM(issued) - SUM(redeemed) - SUM(cancelled)), 0) /
                                              SUM(original_issued_amount) AS unredeemed_percent
                                       FROM SHOPIFY_TEST.PUBLIC.token_jfb_token_base
                                       WHERE activity_month BETWEEN DATEADD(MONTH, -23, DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))) AND DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))
                                         AND original_issued_month BETWEEN DATEADD(MONTH, -23, DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))) AND DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))
                                       GROUP BY brand, country, credit_tenure)) ur ON ur.credit_tenure = 13  and a.brand = ur.brand AND a.country = ur.country
GROUP BY a.brand,
         a.region,
         a.country,
         a.original_credit_tender,
         a.original_credit_reason,
         a.original_credit_type,
         a.original_issued_month,
         a.original_issued_amount,
         DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE)),
         DATEDIFF(MONTH, original_issued_month, DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE))) + 1,
         expected_redeemed_rate,
         expected_unredeemed_rate,
         expected_redeemed_rate_max
;

TRUNCATE TABLE SHOPIFY_TEST.PUBLIC.JFB_TOKEN_BREAKAGE_REPORT;

INSERT INTO    SHOPIFY_TEST.PUBLIC.JFB_TOKEN_BREAKAGE_REPORT
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
FROM SHOPIFY_TEST.PUBLIC.token_append_jfb_token;


TRUNCATE TABLE SHOPIFY_TEST.PUBLIC.jfb_token_breakage_report_snapshot;


INSERT INTO SHOPIFY_TEST.PUBLIC.jfb_token_breakage_report_snapshot
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
       null AS snapshot_timestamp
FROM SHOPIFY_TEST.PUBLIC.jfb_token_breakage_report;