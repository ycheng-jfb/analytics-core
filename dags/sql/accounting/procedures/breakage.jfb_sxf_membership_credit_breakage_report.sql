SET month_thru = DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE));

CREATE OR REPLACE TEMPORARY TABLE _original_credit_activity AS
-- Gross VAT
SELECT CASE
           WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                          AS brand,
       st.store_country                                                     AS country,
       st.store_region                                                      AS region,
       dcf.giftco_transfer_status,
       fce.credit_activity_type,
       dcf.original_credit_tender,
       dcf.original_credit_type,
       dcf.original_credit_reason,
       date_trunc('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       date_trunc('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       dcf2.deferred_recognition_label_credit,
       'Local Gross VAT'                                                    AS currency,
       round(SUM(fce.credit_activity_gross_vat_local_amount), 2)            AS activity_amount
FROM reporting_base_prod.shared.dim_credit dcf
         JOIN reporting_base_prod.shared.dim_credit dcf2 ON dcf.original_credit_key = dcf2.credit_key
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf2.customer_id
         JOIN reporting_base_prod.shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf2.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand IN ('JustFab', 'ShoeDazzle', 'FabKids', 'Savage X')
  AND st.store_country IN ('DE', 'DK', 'ES', 'FR', 'NL', 'SE', 'UK', 'EUREM')
GROUP BY brand,
         store_country,
         store_region,
         dcf.giftco_transfer_status,
         credit_activity_type,
         dcf.original_credit_tender,
         dcf.original_credit_type,
         dcf.original_credit_reason,
         date_trunc('month', dcf.original_credit_issued_local_datetime::DATE),
         date_trunc('month', fce.credit_activity_local_datetime::DATE),
         dcf2.deferred_recognition_label_credit,
         currency

UNION

--Net VAT
SELECT CASE
           WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                          AS brand,
       st.store_country,
       st.store_region,
       dcf.giftco_transfer_status,
       fce.credit_activity_type,
       dcf.original_credit_tender,
       dcf.original_credit_type,
       dcf.original_credit_reason,
       date_trunc('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       date_trunc('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       dcf2.deferred_recognition_label_credit,
       'Local Net VAT'                                                      AS currency,
       SUM(round(fce.credit_activity_local_amount_issuance_vat, 2))         AS activity_amount
FROM reporting_base_prod.shared.dim_credit dcf
         JOIN reporting_base_prod.shared.dim_credit dcf2 ON dcf.original_credit_key = dcf2.credit_key
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf2.customer_id
         JOIN reporting_base_prod.shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf2.store_id
WHERE fce.credit_activity_type IN ('Issued', 'Redeemed', 'Cancelled')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand IN ('JustFab', 'ShoeDazzle', 'FabKids', 'Savage X')
  AND st.store_country IN ('DE', 'DK', 'ES', 'FR', 'NL', 'SE', 'UK', 'EUREM')
GROUP BY brand,
         store_country,
         store_region,
         dcf.giftco_transfer_status,
         credit_activity_type,
         dcf.original_credit_tender,
         dcf.original_credit_type,
         dcf.original_credit_reason,
         date_trunc('month', dcf.original_credit_issued_local_datetime::DATE),
         date_trunc('month', fce.credit_activity_local_datetime::DATE),
         dcf2.deferred_recognition_label_credit,
         currency

UNION

-- Getting the giftco data from all brands in the US
SELECT CASE
           WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                          AS brand,
       st.store_country                                                     AS country,
       st.store_region                                                      AS region,
       dcf.giftco_transfer_status,
       fce.credit_activity_type,
       dcf.original_credit_tender,
       dcf.original_credit_type,
       dcf.original_credit_reason,
       date_trunc('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       date_trunc('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       dcf2.deferred_recognition_label_credit,
       'Local Gross VAT'                                                    AS currency,
       round(SUM(fce.credit_activity_gross_vat_local_amount), 2)            AS activity_amount
FROM reporting_base_prod.shared.dim_credit dcf
         JOIN reporting_base_prod.shared.dim_credit dcf2 ON dcf.original_credit_key = dcf2.credit_key
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf2.customer_id
         JOIN reporting_base_prod.shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf2.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand IN ('JustFab', 'ShoeDazzle', 'FabKids', 'Fabletics')
  AND st.store_country IN ('US')
  AND dcf2.giftco_transfer_status IN ('Membership Credit to Giftco')
  AND dcf.original_credit_type = 'Fixed Credit'
  AND dcf.original_credit_reason = 'Membership Credit'
GROUP BY brand,
         store_country,
         store_region,
         dcf.giftco_transfer_status,
         credit_activity_type,
         dcf.original_credit_tender,
         dcf.original_credit_type,
         dcf.original_credit_reason,
         date_trunc('month', dcf.original_credit_issued_local_datetime::DATE),
         date_trunc('month', fce.credit_activity_local_datetime::DATE),
         dcf2.deferred_recognition_label_credit,
         currency

UNION

-- Getting data of FL refund credits
SELECT CASE
           WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                          AS brand,
       st.store_country                                                     AS country,
       st.store_region                                                      AS region,
       dcf.giftco_transfer_status,
       fce.credit_activity_type,
       dcf.original_credit_tender,
       dcf.original_credit_type,
       dcf.original_credit_reason,
       date_trunc('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       date_trunc('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       dcf2.deferred_recognition_label_credit                               AS deferred_recognition_label_credit,
       'Local Gross VAT'                                                    AS currency,
       round(SUM(fce.credit_activity_gross_vat_local_amount), 2)            AS activity_amount
FROM reporting_base_prod.shared.dim_credit dcf
         JOIN reporting_base_prod.shared.dim_credit dcf2 ON dcf.original_credit_key = dcf2.credit_key
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf2.customer_id
         JOIN reporting_base_prod.shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf2.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled', 'Expired')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand in ('Fabletics', 'Yitty')
  AND dcf.original_credit_type = 'Variable Credit'
  AND dcf.original_credit_reason = 'Refund'
  AND dcf.giftco_transfer_status <> 'Membership Credit to Giftco'
GROUP BY brand,
         store_country,
         store_region,
         dcf.giftco_transfer_status,
         credit_activity_type,
         dcf.original_credit_tender,
         dcf.original_credit_type,
         dcf.original_credit_reason,
         date_trunc('month', dcf.original_credit_issued_local_datetime::DATE),
         date_trunc('month', fce.credit_activity_local_datetime::DATE),
         dcf2.deferred_recognition_label_credit,
         currency


UNION

--Net VAT
SELECT CASE
           WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                          AS brand,
       st.store_country,
       st.store_region,
       dcf.giftco_transfer_status,
       fce.credit_activity_type,
       dcf.original_credit_tender,
       dcf.original_credit_type,
       dcf.original_credit_reason,
       date_trunc('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       date_trunc('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       dcf2.deferred_recognition_label_credit,
       'Local Net VAT'                                                      AS currency,
       SUM(round(fce.credit_activity_local_amount_issuance_vat, 2))         AS activity_amount
FROM reporting_base_prod.shared.dim_credit dcf
         JOIN reporting_base_prod.shared.dim_credit dcf2 ON dcf.original_credit_key = dcf2.credit_key
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf2.customer_id
         JOIN reporting_base_prod.shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf2.store_id
WHERE fce.credit_activity_type IN ('Issued', 'Redeemed', 'Cancelled', 'Expired')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand = 'Fabletics'
  AND st.store_region <> 'NA'
  AND dcf.original_credit_type = 'Variable Credit'
  AND dcf.original_credit_reason = 'Refund'
  AND dcf.giftco_transfer_status <> 'Membership Credit to Giftco'

GROUP BY brand,
         store_country,
         store_region,
         dcf.giftco_transfer_status,
         credit_activity_type,
         dcf.original_credit_tender,
         dcf.original_credit_type,
         dcf.original_credit_reason,
         date_trunc('month', dcf.original_credit_issued_local_datetime::DATE),
         date_trunc('month', fce.credit_activity_local_datetime::DATE),
         dcf2.deferred_recognition_label_credit,
         currency

UNION

-- Getting data of FL Cash Gift Cards
SELECT CASE
           WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                          AS brand,
       st.store_country                                                     AS country,
       st.store_region                                                      AS region,
       dcf.giftco_transfer_status,
       fce.credit_activity_type,
       dcf.original_credit_tender,
       dcf.original_credit_type,
       dcf.original_credit_reason,
       date_trunc('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       date_trunc('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       dcf2.deferred_recognition_label_credit                               AS deferred_recognition_label_credit,
       'Local Gross VAT'                                                    AS currency,
       round(SUM(fce.credit_activity_gross_vat_local_amount), 2)            AS activity_amount
FROM reporting_base_prod.shared.dim_credit dcf
         JOIN reporting_base_prod.shared.dim_credit dcf2 ON dcf.original_credit_key = dcf2.credit_key
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf2.customer_id
         JOIN reporting_base_prod.shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf2.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled', 'Expired')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand in ('Fabletics')
  AND st.store_country in ('US')
  AND dcf.original_credit_tender = 'Cash'
  AND dcf.original_credit_type = 'Giftcard' AND dcf.original_credit_reason = 'Gift Card - Paid'
GROUP BY brand,
         store_country,
         store_region,
         dcf.giftco_transfer_status,
         credit_activity_type,
         dcf.original_credit_tender,
         dcf.original_credit_type,
         dcf.original_credit_reason,
         date_trunc('month', dcf.original_credit_issued_local_datetime::DATE),
         date_trunc('month', fce.credit_activity_local_datetime::DATE),
         dcf2.deferred_recognition_label_credit,
         currency
;

CREATE OR REPLACE TEMPORARY TABLE _pivot AS
WITH _issuance AS (SELECT *
                   FROM _original_credit_activity C
                            JOIN (SELECT month_date calendar_month
                                  FROM edw_prod.data_model.dim_date) dm
                                 ON dm.calendar_month >= C.original_issued_month
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
                COALESCE(p.activity_month::date, i.calendar_month) activity_month,
                i.deferred_recognition_label_credit,
                i.currency,
                nvl(p."'Issued'", 0)                               issued,
                nvl(p."'Redeemed'", 0)                             redeemed,
                nvl(p."'Cancelled'", 0)                            cancelled,
                nvl(p."'Expired'", 0)                              expired
FROM _issuance i
         LEFT JOIN _original_credit_activity
    pivot(SUM(activity_amount) FOR credit_activity_type IN ('Issued','Redeemed', 'Cancelled', 'Expired')) p
                   ON i.brand = p.brand
                       AND i.region = p.region
                       AND i.country = p.country
                       AND i.giftco_transfer_status = p.giftco_transfer_status
                       AND i.original_credit_tender = p.original_credit_tender
                       AND i.original_credit_type = p.original_credit_type
                       AND i.original_credit_reason = p.original_credit_reason
                       AND i.activity_month = p.original_issued_month
                       AND i.calendar_month = p.activity_month
                       AND i.deferred_recognition_label_credit = p.deferred_recognition_label_credit
                       AND i.currency = p.currency;

CREATE OR REPLACE TEMPORARY TABLE _jfb_sxf_base AS
SELECT brand,
       region,
       country,
       giftco_transfer_status,
       original_credit_tender,
       original_credit_reason,
       original_credit_type,
       original_issued_month,
       original_issued_amount,
       activity_month,
       deferred_recognition_label_credit,
       currency,
       datediff(MONTH, original_issued_month, activity_month) + 1 credit_tenure,
       SUM(issued)                                                issued,
       SUM(redeemed)                                              redeemed,
       SUM(cancelled)                                             cancelled,
       SUM(expired)                                               expired
FROM _pivot
WHERE original_credit_tender = 'Cash'
  AND iff(brand in ('Fabletics Mens','Fabletics Womens','Yitty'),
          ((original_credit_type = 'Fixed Credit' AND original_credit_reason = 'Membership Credit') OR
           (original_credit_type = 'Variable Credit' AND original_credit_reason = 'Refund') OR
           (original_credit_type = 'Giftcard' AND original_credit_reason = 'Gift Card - Paid')),
           (original_credit_type = 'Fixed Credit' AND original_credit_reason = 'Membership Credit'))
GROUP BY brand,
         region,
         country,
         giftco_transfer_status,
         original_credit_tender,
         original_credit_reason,
         original_credit_type,
         original_issued_month,
         original_issued_amount,
         activity_month,
         deferred_recognition_label_credit,
         currency,
         datediff(MONTH, original_issued_month, activity_month) + 1;

CREATE OR REPLACE TEMPORARY TABLE _append_jfb_sxf_credits AS
WITH _activity_amount AS (SELECT *
                          FROM _jfb_sxf_base
                          WHERE original_credit_reason IN ('Membership Credit','Refund','Gift Card - Paid')
                            AND original_issued_month <= $month_thru
                            AND activity_month <= $month_thru),
     _expected_redemption_rate AS (SELECT brand,
                                          country,
                                          currency,
                                          original_credit_reason,
                                          credit_tenure,
                                          cumulative_redeemed_percent
                                   FROM (SELECT brand,
                                                country,
                                                currency,
                                                original_credit_reason,
                                                credit_tenure,
                                                SUM(redeemed_percent)
                                                    OVER (PARTITION BY brand, country, currency, original_credit_reason ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_redeemed_percent
                                         FROM (SELECT brand,
                                                      country,
                                                      currency,
                                                      original_credit_reason,
                                                      credit_tenure,
                                                      NVL(SUM(redeemed) / SUM(original_issued_amount), 0) AS redeemed_percent
                                               FROM _jfb_sxf_base
                                               WHERE activity_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                                 AND original_issued_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                                 AND currency = 'Local Gross VAT'
                                               GROUP BY brand, country, currency, original_credit_reason, credit_tenure))),
     _expected_redemption_rate_24 AS (SELECT brand,
                                             country,
                                             currency,
                                             original_credit_reason,
                                             credit_tenure,
                                             cumulative_redeemed_percent
                                      FROM (SELECT brand,
                                                   country,
                                                   currency,
                                                   original_credit_reason,
                                                   credit_tenure,
                                                   SUM(redeemed_percent)
                                                       OVER (PARTITION BY brand, country, currency ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_redeemed_percent
                                            FROM (SELECT brand,
                                                         country,
                                                         currency,
                                                         original_credit_reason,
                                                         credit_tenure,
                                                         NVL(SUM(redeemed) / SUM(original_issued_amount), 0) AS redeemed_percent
                                                  FROM _jfb_sxf_base
                                                  WHERE activity_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                                    AND original_issued_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                                    AND currency = 'Local Gross VAT'
                                                  GROUP BY brand, country, currency, original_credit_reason, credit_tenure))),
     _expected_unredeem_rate AS (SELECT brand,
                                        country,
                                        currency,
                                        original_credit_reason,
                                        credit_tenure,
                                        cumulative_unredeem_percent
                                 FROM (SELECT brand,
                                              country,
                                              currency,
                                              original_credit_reason,
                                              credit_tenure,
                                              SUM(unredeemed_percent)
                                                  OVER (PARTITION BY brand, country, currency, original_credit_reason ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_unredeem_percent
                                       FROM (SELECT brand,
                                                    country,
                                                    currency,
                                                    original_credit_reason,
                                                    credit_tenure,
                                                    NVL((SUM(issued) - SUM(redeemed) - SUM(cancelled)), 0) /
                                                    SUM(original_issued_amount) AS unredeemed_percent
                                             FROM _jfb_sxf_base
                                             WHERE activity_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                               AND original_issued_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                               AND currency = 'Local Gross VAT'
                                             GROUP BY brand, country, currency, original_credit_reason, credit_tenure)))
SELECT a.brand || ' ' || a.country                                                         business_unit,
       a.brand,
       a.region,
       a.country,
       a.original_credit_tender,
       a.original_credit_reason                                                            store_credit_reason,
       a.original_credit_type,
       a.original_issued_month                                                             issued_month,
       a.deferred_recognition_label_credit                                                 deferred_recognition_label,
       a.currency,
       a.original_issued_amount,
       datediff(MONTH, original_issued_month, $month_thru) + 1                             credit_tenure,
       $month_thru                                                                         recognition_booking_month,
       SUM(issued)                                                                         issued_to_date,
       SUM(redeemed)                                                                       redeemed_to_date,
       SUM(cancelled)                                                                      cancelled_to_date,
       SUM(expired)                                                                        expired_to_date,
       issued_to_date - redeemed_to_date - cancelled_to_date                               unredeemed_to_date,
       redeemed_to_date / NULLIF(issued_to_date, 0)                                        redeemed_rate_to_date,
       cancelled_to_date / NULLIF(issued_to_date, 0)                                       cancelled_to_date_pct,
       unredeemed_to_date / NULLIF(issued_to_date, 0)                                      unredeemed_to_date_pct,
       r10.cumulative_redeemed_percent                                                     expected_redeemed_rate,
       ur.cumulative_unredeem_percent                                                      expected_unredeemed_rate,
       r24.cumulative_redeemed_percent                                                     expected_redeemed_rate_max,
       iff((datediff(MONTH, original_issued_month, $month_thru) + 1) > 24, 0,
           redeemed_rate_to_date / expected_redeemed_rate)                                 actual_redeemed_rate_vs_expected,
       1 - expected_redeemed_rate_max - expected_unredeemed_rate                           assumption_cumulative_cancelled_pct,
       issued_to_date * expected_redeemed_rate                                             expected_redeemed,
       iff(datediff(MONTH, original_issued_month, $month_thru) + 1 <= 24,
           iff(unredeemed_to_date < expected_unredeemed_rate * issued_to_date, 0,
               expected_redeemed_rate_max * issued_to_date - redeemed_to_date), 0)         expected_additional_redeemed,
       issued_to_date * assumption_cumulative_cancelled_pct                                expected_cancelled,
       iff(datediff(MONTH, original_issued_month, $month_thru) + 1 <= 24,
           iff(unredeemed_to_date < expected_unredeemed_rate * issued_to_date, 0,
               (1 - expected_unredeemed_rate - expected_redeemed_rate_max) * issued_to_date -
               cancelled_to_date), 0)                                                      expected_additional_cancelled,
       issued_to_date * expected_unredeemed_rate                                           expected_unredeemed,
       CASE
           WHEN deferred_recognition_label_credit = 'Cant Recognize'
               THEN issued_to_date - ifnull(redeemed_to_date, 0) - ifnull(cancelled_to_date, 0)
           WHEN deferred_recognition_label_credit = 'Recognize 40%'
               THEN (issued_to_date - ifnull(redeemed_to_date, 0) - ifnull(cancelled_to_date, 0)) * .6
           ELSE 0 END                                                                      unredeemed_cant_recognize,
       CASE
           WHEN deferred_recognition_label_credit = 'Recognize'
               THEN 0
           WHEN deferred_recognition_label_credit = 'Recognize 40%'
               THEN iff(unredeemed_to_date < expected_unredeemed_rate * issued_to_date,
                        unredeemed_to_date, expected_unredeemed) * 0.6
           ELSE iff(unredeemed_to_date < expected_unredeemed_rate * issued_to_date,
                    unredeemed_to_date, expected_unredeemed) END                           expected_cannot_recognize,
       CASE
           WHEN (datediff(MONTH, original_issued_month, $month_thru) + 1) > 24
               THEN
               CASE
                   WHEN a.deferred_recognition_label_credit = 'Cant Recognize'
                       THEN 0
                   WHEN a.deferred_recognition_label_credit = 'Recognize'
                       THEN unredeemed_to_date
                   ELSE
                       unredeemed_to_date * 0.4
                   END
           ELSE
               CASE
                   WHEN a.deferred_recognition_label_credit = 'Cant Recognize'
                       THEN 0
                   WHEN a.deferred_recognition_label_credit = 'Recognize' -- if recognize or recognize 40% then check if expected_unredeemed > unredeemed to get expected_breakage_to_record
                       THEN iff(unredeemed_to_date <
                                issued_to_date * expected_unredeemed_rate,
                                unredeemed_to_date, issued_to_date * expected_unredeemed_rate)
                   ELSE
                           iff(unredeemed_to_date < (issued_to_date * expected_unredeemed_rate),
                               unredeemed_to_date, (issued_to_date * expected_unredeemed_rate)) * 0.4
                   END
           END                                                                             expected_breakage,
       iff((datediff(MONTH, original_issued_month, $month_thru) + 1) > 24,
           expected_breakage,
           expected_breakage *
           iff(actual_redeemed_rate_vs_expected > 1, 1, actual_redeemed_rate_vs_expected)) breakage_to_record,
       0                                                                                   change_in_breakage_to_record,
       'Membership Credit'                                                                 breakage_type
FROM _activity_amount a
         LEFT JOIN _expected_redemption_rate r10
                   ON r10.credit_tenure = 10 AND a.brand = r10.brand AND a.country = r10.country AND
                      a.original_credit_reason = r10.original_credit_reason
         LEFT JOIN _expected_redemption_rate_24 r24
                   ON r24.credit_tenure = 24 AND a.brand = r24.brand AND a.country = r24.country AND
                      a.original_credit_reason = r24.original_credit_reason
         LEFT JOIN _expected_unredeem_rate ur
                   ON ur.credit_tenure = 24 AND a.brand = ur.brand AND a.country = ur.country AND
                      a.original_credit_reason = ur.original_credit_reason
GROUP BY a.brand,
         a.region,
         a.country,
         a.original_credit_tender,
         a.original_credit_reason,
         a.original_credit_type,
         a.original_issued_month,
         a.deferred_recognition_label_credit,
         a.currency,
         a.original_issued_amount,
         $month_thru,
         datediff(MONTH, original_issued_month, $month_thru) + 1,
         expected_redeemed_rate,
         expected_unredeemed_rate,
         expected_redeemed_rate_max
;

DELETE FROM breakage.jfb_sxf_membership_credit_breakage_report
where recognition_booking_month = $month_thru;

INSERT INTO breakage.jfb_sxf_membership_credit_breakage_report
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
       expected_redeemed,
       expected_additional_redeemed,
       expected_cancelled,
       expected_additional_cancelled,
       expected_unredeemed,
       unredeemed_cant_recognize,
       expected_cannot_recognize,
       expected_breakage,
       breakage_to_record,
       change_in_breakage_to_record,
       breakage_type
FROM _append_jfb_sxf_credits;

CREATE TRANSIENT TABLE IF NOT EXISTS breakage.jfb_sxf_membership_credit_breakage_report_snapshot (
	business_unit VARCHAR(155),
	brand VARCHAR(155),
	region VARCHAR(2),
	country VARCHAR(155),
	original_credit_tender VARCHAR(4),
	store_credit_reason VARCHAR(155),
	original_credit_type VARCHAR(15),
	issued_month DATE,
	deferred_recognition_label VARCHAR(155),
	currency VARCHAR(55),
	original_issued_amount NUMBER(20,4),
	credit_tenure NUMBER(38,0),
	recognition_booking_month DATE,
	issued_to_date NUMBER(20,4),
	redeemed_to_date NUMBER(20,4),
	cancelled_to_date NUMBER(20,4),
	expired_to_date NUMBER(1,0),
	unredeemed_to_date NUMBER(20,4),
	redeemed_rate_to_date NUMBER(20,4),
	cancelled_to_date_pct NUMBER(20,4),
	unredeemed_to_date_pct NUMBER(20,4),
	expected_redeemed_rate NUMBER(20,4),
	expected_unredeemed_rate NUMBER(20,4),
	expected_redeemed_rate_max NUMBER(20,4),
	actual_redeemed_rate_vs_expected NUMBER(20,4),
	assumption_cumulative_cancelled_pct NUMBER(20,4),
	expected_redeemed NUMBER(20,4),
	expected_additional_redeemed NUMBER(20,4),
	expected_cancelled NUMBER(20,4),
	expected_additional_cancelled NUMBER(20,4),
	expected_unredeemed NUMBER(20,4),
	unredeemed_cant_recognize NUMBER(20,4),
	expected_cannot_recognize NUMBER(20,4),
	expected_breakage NUMBER(20,4),
	breakage_to_record NUMBER(20,4),
	change_in_breakage_to_record NUMBER(20,4),
	breakage_type VARCHAR(17),
	snapshot_timestamp TIMESTAMP_LTZ(9)
);

INSERT INTO breakage.jfb_sxf_membership_credit_breakage_report_snapshot
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
       expected_redeemed,
       expected_additional_redeemed,
       expected_cancelled,
       expected_additional_cancelled,
       expected_unredeemed,
       unredeemed_cant_recognize,
       expected_cannot_recognize,
       expected_breakage,
       breakage_to_record,
       change_in_breakage_to_record,
       breakage_type,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM breakage.jfb_sxf_membership_credit_breakage_report;

DELETE
FROM breakage.jfb_sxf_membership_credit_breakage_report_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
