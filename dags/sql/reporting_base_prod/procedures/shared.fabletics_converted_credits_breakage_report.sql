SET month_thru = DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE));

CREATE OR REPLACE TEMP TABLE _original_credit_activity AS
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
       'Local Gross VAT'                                                    AS currency,
       DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       DATE_TRUNC('month', fce.credit_activity_local_datetime)::DATE        AS activity_month,
       SUM(fce.credit_activity_gross_vat_local_amount)                      AS activity_amount
FROM shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled', 'Expired')
  AND fce.original_credit_activity_type_action = 'Include'
  AND brand = 'Fabletics Womens'
  AND st.store_country IN ('US', 'CA', 'DK', 'ES', 'FR', 'NL', 'SE', 'UK', 'DE')
GROUP BY brand,
         country,
         region,
         credit_activity_type,
         original_credit_tender,
         original_credit_type,
         original_credit_reason,
         currency,
         original_issued_month,
         activity_month

UNION

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
       'Local Net VAT'                                                      AS currency,
       DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       DATE_TRUNC('month', fce.credit_activity_local_datetime)::DATE        AS activity_month,
       SUM(fce.credit_activity_local_amount_issuance_vat)                   AS activity_amount
FROM shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled', 'Expired')
  AND fce.original_credit_activity_type_action = 'Include'
  AND brand = 'Fabletics Womens'
  AND st.store_country IN ('DK', 'ES', 'FR', 'NL', 'SE', 'UK', 'DE')
GROUP BY brand,
         country,
         region,
         credit_activity_type,
         original_credit_tender,
         original_credit_type,
         original_credit_reason,
         currency,
         original_issued_month,
         activity_month
;

CREATE OR REPLACE TEMP TABLE _pivot AS
WITH _month AS (SELECT month_date                                       calendar_month
                FROM edw_prod.data_model.dim_date),
     _issuance AS (SELECT *
                   FROM _original_credit_activity c
                            JOIN _month dm
                                 ON dm.calendar_month >= c.original_issued_month
                                     AND dm.calendar_month <= DATEADD(MONTH, 24, CURRENT_DATE())
                   WHERE credit_activity_type = 'Issued')
SELECT DISTINCT i.brand,
                i.region,
                i.country,
                i.original_credit_tender,
                i.original_credit_type,
                i.original_credit_reason,
                i.original_issued_month,
                i.currency,
                i.activity_amount                                  original_issued_amount,
                COALESCE(p.activity_month::DATE, i.calendar_month) activity_month,
                NVL(p."'Issued'", 0)                               issued,
                NVL(p."'Redeemed'", 0)                             redeemed,
                NVL(p."'Cancelled'", 0)                            cancelled,
                NVL(p."'Expired'", 0)                              expired
FROM _issuance i
         LEFT JOIN (SELECT DISTINCT brand,
                                    country,
                                    region,
                                    original_credit_tender,
                                    original_credit_type,
                                    credit_activity_type,
                                    original_credit_reason,
                                    original_issued_month,
                                    currency,
                                    activity_month,
                                    activity_amount
                    FROM _original_credit_activity)
    PIVOT (SUM(activity_amount)
    FOR credit_activity_type IN ('Issued','Redeemed', 'Cancelled', 'Expired')) p
                   ON
                               i.brand = p.brand
                           AND i.region = p.region
                           AND i.country = p.country
                           AND i.original_credit_tender = p.original_credit_tender
                           AND i.original_credit_type = p.original_credit_type
                           AND i.original_credit_reason = p.original_credit_reason
                           AND i.activity_month = p.original_issued_month
                           AND i.currency = p.currency
                           AND i.calendar_month = p.activity_month;

CREATE OR REPLACE TEMPORARY TABLE _fl_base AS
SELECT brand,
       region,
       country,
       original_credit_tender,
       original_credit_reason,
       original_credit_type,
       original_issued_month,
       currency,
       original_issued_amount,
       activity_month,
       DATEDIFF(MONTH, original_issued_month, activity_month) + 1 credit_tenure,
       SUM(NVL(issued, 0))                                        issued,
       SUM(NVL(redeemed, 0))                                      redeemed,
       SUM(NVL(cancelled, 0))                                     cancelled,
       SUM(NVL(expired, 0))                                       expired
FROM _pivot
WHERE original_credit_tender = 'Cash'
  AND ((original_credit_type = 'Fixed Credit' AND
        original_credit_reason = 'Membership Credit'))
GROUP BY brand,
         region,
         country,
         original_credit_tender,
         original_credit_reason,
         original_credit_type,
         original_issued_month,
         currency,
         original_issued_amount,
         activity_month;

CREATE OR REPLACE TEMPORARY TABLE _issued AS
SELECT brand,
       region,
       country,
       currency,
       original_credit_tender,
       original_credit_reason,
       original_credit_type,
       original_issued_month,
       original_issued_amount,
       $month_thru                                             activity_month,
       DATEDIFF(MONTH, original_issued_month, $month_thru) + 1 credit_tenure,
       DATEDIFF(MONTH, original_issued_month,
                DECODE(country, 'US', '2021-01-01', 'CA', '2021-08-01', '2021-10-01')) + 1 +
       DECODE(country, 'CA', 17, 'UK', 17, 23)                 projected_unredeem_breakage_tenure, -- tenure until the FIRST TOKEN MONTH (e.g., US: 2022-01-01) adds the projected months (18 months)
       DATEDIFF(MONTH, original_issued_month,
                DECODE(country, 'US', '2021-01-01', 'CA', '2021-08-01', '2021-10-01')) + 1 +
       11                                                      projected_redeem_breakage_tenure,
       DATEDIFF(MONTH, original_issued_month,
                DECODE(country, 'US', '2021-01-01', 'CA', '2021-08-01', '2021-10-01')) + 1 +
       DECODE(country, 'CA', 17, 'UK', 17, 23)/*23*/           projected_redeem_breakage_tenure_m24,

       NVL(SUM(issued), 0)                                     i,
       NVL(SUM(redeemed), 0)                                   r,
       NVL(SUM(cancelled), 0)                                  c,
       NVL(SUM(expired), 0)                                    e,
       i - r - c                                               unrd
FROM _fl_base b
WHERE DECODE(country, 'UK', original_issued_month <= '2021-09-01',
             'DE', original_issued_month <= '2021-09-01',
             'CA', original_issued_month <= '2021-07-01',
             'US', original_issued_month BETWEEN '2018-12-01' AND '2020-12-01',
             original_issued_month <= '2021-10-01')
  AND activity_month <= $month_thru
  AND original_credit_reason = 'Membership Credit'
  AND original_credit_type = 'Fixed Credit'
GROUP BY brand,
         region,
         country,
         currency,
         original_credit_tender,
         original_credit_reason,
         original_credit_type,
         original_issued_month,
         original_issued_amount;


CREATE OR REPLACE TEMPORARY TABLE _append_fl_converted AS
SELECT i.brand,
       i.region,
       i.country,
       i.currency,
       i.original_credit_tender,
       i.original_credit_reason,
       i.original_credit_type,
       i.original_issued_month,
       i.original_issued_amount,
       i.activity_month,
       i.credit_tenure,
       i.projected_redeem_breakage_tenure,
       i.projected_unredeem_breakage_tenure,
       i.i                                          issued_to_date,
       i.r                                          redeemed_to_date,
       i.c                                          cancelled_to_date,
       i.e                                          expired_to_date,
       i.unrd                                       unredeemed_to_date,
       redeemed_to_date / NULLIF(issued_to_date, 0) redeemed_rate_to_date,
       NULL                                         expected_redeemed_rate,
       NULL                                         actual_redeemed_rate_vs_expected,
       NULL                                         expected_unredeemed_rate,
       i.unrd                                       expected_breakage_to_record,
       i.unrd                                       breakage_recorded,
       NULL                                         expected_redeemed_rate_m24,
       0                                            expected_additional_redeemed,
       0                                            expected_additional_cancelled
FROM _issued i;


MERGE INTO shared.fabletics_converted_credits_breakage_report t
    USING _append_fl_converted a
    ON t.brand = a.brand AND
       t.region = a.region AND
       t.country = a.country AND
       t.currency = a.currency AND
       t.original_credit_tender = a.original_credit_tender AND
       t.original_credit_reason = a.original_credit_reason AND
       t.original_credit_type = a.original_credit_type AND
       t.original_issued_month = a.original_issued_month AND
       t.activity_month = a.activity_month AND
       t.credit_tenure = a.credit_tenure
    WHEN NOT MATCHED THEN
        INSERT (brand, region, country, currency, original_credit_tender, original_credit_reason, original_credit_type,
                original_issued_amount,
                original_issued_month, activity_month,
                credit_tenure, issued_to_date, redeemed_to_date, cancelled_to_date, expired_to_date, unredeemed_to_date,
                redeemed_rate_to_date,
                expected_redeemed_rate, expected_unredeemed_rate, actual_redeemed_rate_vs_expected,
                expected_breakage_to_record, breakage_recorded, expected_redeemed_rate_m24,
                expected_additional_redeemed, expected_additional_cancelled
            )
            VALUES (brand, region, country, currency, original_credit_tender, original_credit_reason,
                    original_credit_type,
                    original_issued_amount, original_issued_month, activity_month, credit_tenure, issued_to_date,
                    redeemed_to_date, cancelled_to_date, expired_to_date, unredeemed_to_date,
                    redeemed_rate_to_date, expected_redeemed_rate, expected_unredeemed_rate,
                    actual_redeemed_rate_vs_expected, expected_breakage_to_record, breakage_recorded,
                    expected_redeemed_rate_m24,
                    expected_additional_redeemed, expected_additional_cancelled)
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
         NOT EQUAL_NULL(t.expected_redeemed_rate_m24, a.expected_redeemed_rate_m24) OR
         NOT EQUAL_NULL(t.expected_additional_redeemed, a.expected_additional_redeemed) OR
         NOT EQUAL_NULL(t.expected_additional_cancelled, a.expected_additional_cancelled)
            )
        THEN
        UPDATE
            SET t.original_issued_amount = a.original_issued_amount,
                t.issued_to_date = a.issued_to_date,
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
                t.expected_redeemed_rate_m24 = a.expected_redeemed_rate_m24,
                t.expected_additional_redeemed = a.expected_additional_redeemed,
                t.expected_additional_cancelled = a.expected_additional_cancelled
;


