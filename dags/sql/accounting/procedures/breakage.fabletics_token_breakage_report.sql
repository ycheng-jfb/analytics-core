/*
 Updated to Add Currency as a Dimension, and Simply the Code by Removing the Extended Credits in the Model
 */

SET month_thru = DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE));

CREATE OR REPLACE TEMP TABLE _extended_key_original AS
SELECT dcf.original_credit_key
FROM lake_consolidated_view.ultra_merchant.membership_token mt
         JOIN reporting_base_prod.shared.dim_credit dcf
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
       'Local Gross VAT'                                                    AS currency,
       DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       SUM(fce.credit_activity_gross_vat_local_amount)                      AS activity_amount
FROM reporting_base_prod.shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN reporting_base_prod.shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
         LEFT JOIN _extended_key_original ek
                   ON dcf.original_credit_key = ek.original_credit_key
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled', 'Expired')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_country IN ('US', 'UK', 'DE', 'DK', 'ES', 'FR', 'NL', 'SE', 'CA')
  AND brand IN ('Fabletics Womens', 'Fabletics Mens', 'Yitty')
GROUP BY brand,
         country,
         region,
         is_extended,
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
       IFF(ek.original_credit_key IS NOT NULL, TRUE, FALSE)                 AS is_extended,
       credit_activity_type,
       original_credit_tender,
       original_credit_type,
       original_credit_reason,
       'Local Net VAT'                                                      AS currency,
       DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       SUM(fce.credit_activity_local_amount_issuance_vat)                   AS activity_amount
FROM reporting_base_prod.shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN reporting_base_prod.shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
         LEFT JOIN _extended_key_original ek
                   ON dcf.original_credit_key = ek.original_credit_key
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled', 'Expired')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_country IN ('UK', 'DE', 'DK', 'ES', 'FR', 'NL', 'SE')
  AND brand IN ('Fabletics Womens', 'Fabletics Mens', 'Yitty')
GROUP BY brand,
         country,
         region,
         is_extended,
         credit_activity_type,
         original_credit_tender,
         original_credit_type,
         original_credit_reason,
         currency,
         original_issued_month,
         activity_month;

CREATE OR REPLACE TEMPORARY TABLE _pivot_by_extension AS
WITH _issuance AS (SELECT *
                   FROM _original_credit_activity c
                            JOIN (SELECT month_date calendar_month
                                  FROM edw_prod.data_model.dim_date) dm
                                 ON dm.calendar_month >= c.original_issued_month
                                     AND dm.calendar_month <= DATEADD(MONTH, 12, CURRENT_DATE())
                   WHERE credit_activity_type = 'Issued')
SELECT DISTINCT i.brand,
                i.region,
                i.country,
                i.is_extended,
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
                                    is_extended,
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
                           AND i.is_extended = p.is_extended
                           AND i.original_credit_tender = p.original_credit_tender
                           AND i.original_credit_type = p.original_credit_type
                           AND i.original_credit_reason = p.original_credit_reason
                           AND i.activity_month = p.original_issued_month
                           AND i.currency = p.currency
                           AND i.calendar_month = p.activity_month;

CREATE OR REPLACE TEMPORARY TABLE _fl_token_base_agg AS
SELECT brand,
       region,
       country,
       original_credit_tender,
       IFF(original_credit_type = 'Token', 'Token Billing with Refund', original_credit_reason) original_credit_reason,
       original_credit_type,
       original_issued_month,
       currency,
       activity_month,
       DATEDIFF(MONTH, original_issued_month, activity_month) + 1                               credit_tenure,
       SUM(original_issued_amount)                                                              original_issued_amount,
       SUM(issued)                                                                              issued,
       SUM(redeemed)                                                                            redeemed,
       SUM(cancelled)                                                                           cancelled,
       SUM(expired)                                                                             expired
FROM _pivot_by_extension
WHERE original_credit_tender = 'Cash'
  AND ((original_credit_type = 'Fixed Credit' AND original_credit_reason = 'Membership Credit')
    OR (original_credit_type = 'Token' AND original_credit_reason = 'Token Billing')
    OR (original_credit_type = 'Token' AND original_credit_reason = 'Refund'))
GROUP BY brand,
         region,
         country,
         original_credit_tender,
         IFF(original_credit_type = 'Token', 'Token Billing with Refund', original_credit_reason),
         original_credit_type,
         original_issued_month,
         currency,
         activity_month;

CREATE OR REPLACE TEMPORARY TABLE _append_fl_token AS
WITH _activity_amount AS (SELECT *
                          FROM _fl_token_base_agg
                          WHERE IFF(brand = 'Fabletics Mens',
                                    original_credit_type IN ('Fixed Credit', 'Token'),
                                    original_credit_type = 'Token')
                            AND activity_month <= $month_thru
                            AND original_issued_month <= $month_thru)
    ,
     _expected_redemption_rate AS (SELECT country,
                                          brand,
                                          credit_tenure,
                                          cumulative_redeemed_percent expected_rd
                                   FROM (SELECT country,
                                                brand,
                                                credit_tenure,
                                                SUM(redeemed_percent)
                                                    OVER (PARTITION BY country, brand ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_redeemed_percent

                                         FROM (SELECT country,
                                                      brand,
                                                      credit_tenure,
                                                      NVL(SUM(redeemed)
                                                              / SUM(original_issued_amount), 0) AS redeemed_percent
                                               FROM _fl_token_base_agg
                                               WHERE activity_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                                 AND original_issued_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                                 AND currency = 'Local Gross VAT'
                                               GROUP BY country,
                                                        brand,
                                                        credit_tenure))
                                   WHERE credit_tenure = 12),


     _expected_redemption_rate_yitty AS (SELECT country,
                                                brand,
                                                cumulative_redeemed_percent expected_rd_yitty --0.6343572764 Yitty, 0.6336674430 FLW
                                         FROM (SELECT country,
                                                      brand,
                                                      credit_tenure,
                                                      SUM(redeemed_percent)
                                                          OVER (PARTITION BY country, brand ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_redeemed_percent
                                               FROM (SELECT DISTINCT 'US'          country,
                                                                     'Yitty'       brand,
                                                                     credit_tenure,
                                                                     NVL(
                                                                             SUM(redeemed) OVER (PARTITION BY country, credit_tenure) /
                                                                             SUM(original_issued_amount) OVER (PARTITION BY country, credit_tenure),
                                                                             0) AS redeemed_percent
                                                     FROM _fl_token_base_agg
                                                     WHERE country = 'US'
                                                       AND brand IN ('Yitty')
                                                       AND activity_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                                       AND original_issued_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru))
                                         WHERE credit_tenure = 12)
    ,
     _expected_redemption_rate_24 AS (SELECT country,
                                             brand,
                                             cumulative_redeemed_percent expected_rd_24
                                      FROM (SELECT country,
                                                   brand,
                                                   credit_tenure,
                                                   SUM(redeemed_percent)
                                                       OVER (PARTITION BY country, brand ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_redeemed_percent
                                            FROM (SELECT country,
                                                         credit_tenure,
                                                         brand,
                                                         NVL(SUM(redeemed) / SUM(original_issued_amount), 0) AS redeemed_percent
                                                  FROM _fl_token_base_agg
                                                  WHERE activity_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                                    AND original_issued_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                                    AND IFF(country IN ('UK', 'US', 'CA'), credit_tenure <= 13,
                                                            credit_tenure <= 24)
                                                    AND currency = 'Local Gross VAT'
                                                  GROUP BY country,
                                                           credit_tenure,
                                                           brand)
                                            WHERE redeemed_percent <> 0)
                                      QUALIFY ROW_NUMBER()
                                                  OVER (PARTITION BY country,brand ORDER BY credit_tenure DESC) =
                                              1),

     _expected_redemption_rate_24_yitty AS (SELECT country,
                                                   brand,
                                                   cumulative_redeemed_percent expected_rd_24_yitty --0.6981879598 Yitty, 0.6879556299 FLW
                                            FROM (SELECT country,
                                                         brand,
                                                         credit_tenure,
                                                         SUM(redeemed_percent)
                                                             OVER (PARTITION BY country, brand ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_redeemed_percent
                                                  FROM (SELECT DISTINCT country,
                                                                        credit_tenure,
                                                                        'Yitty'       brand,
                                                                        NVL(
                                                                                SUM(redeemed) OVER (PARTITION BY country, credit_tenure) /
                                                                                SUM(original_issued_amount) OVER (PARTITION BY country, credit_tenure),
                                                                                0) AS redeemed_percent
                                                        FROM _fl_token_base_agg
                                                        WHERE country = 'US'
                                                          AND brand IN ('Yitty')
                                                          AND activity_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                                                          AND original_issued_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru))
                                            WHERE credit_tenure = 13)
    ,

     _expected_unredeem_rate
         AS (SELECT country,
                    brand,
                    credit_tenure,
                    cumulative_unredeem_percent
                        expected_unrd
             FROM (SELECT country,
                          brand,
                          credit_tenure,
                          SUM(unredeemed_percent)
                              OVER (PARTITION BY country,brand ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_unredeem_percent
                   FROM (SELECT country,
                                brand,
                                credit_tenure,
                                SUM(original_issued_amount),
                                SUM(redeemed),
                                NVL((SUM(issued) - SUM(redeemed) - SUM(cancelled)) / SUM(original_issued_amount),
                                    0) AS unredeemed_percent
                         FROM _fl_token_base_agg
                         WHERE activity_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                           AND original_issued_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                           AND currency = 'Local Gross VAT'
                           AND IFF(country IN ('UK', 'US', 'CA'), credit_tenure <= 13, credit_tenure <= 24)
                         GROUP BY country,
                                  brand,
                                  credit_tenure)
                   WHERE unredeemed_percent <> 0)
             QUALIFY ROW_NUMBER() OVER (PARTITION BY country,brand ORDER BY credit_tenure DESC) = 1),

     _expected_unredeem_rate_yitty
         AS (SELECT country,
                    brand,
                    cumulative_unredeem_percent expected_unrd_yitty
             FROM (SELECT country,
                          brand,
                          credit_tenure,
                          SUM(unredeemed_percent)
                              OVER (PARTITION BY country, brand ORDER BY credit_tenure ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_unredeem_percent
                   FROM (SELECT DISTINCT country,
                                         'Yitty'   brand,
                                         credit_tenure,
                                         NVL((SUM(issued) OVER (PARTITION BY credit_tenure) -
                                              SUM(redeemed) OVER (PARTITION BY credit_tenure) -
                                              SUM(cancelled) OVER (PARTITION BY credit_tenure)) /
                                             SUM(original_issued_amount) OVER (PARTITION BY credit_tenure),
                                             0) AS unredeemed_percent
                         FROM _fl_token_base_agg
                         WHERE country = 'US'
                           AND brand IN ('Yitty')
                           AND activity_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru
                           AND original_issued_month BETWEEN DATEADD(MONTH, -23, $month_thru) AND $month_thru))
             WHERE credit_tenure = 13)

SELECT a.brand,
       a.region,
       a.country,
       a.original_credit_tender,
       a.original_credit_reason,
       a.original_credit_type,
       a.original_issued_month,
       a.currency,
       $month_thru                                                                             activity_month,
       DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1                               credit_tenure,
       IFF(a.country = 'US' AND a.brand = 'Yitty', rdyt.expected_rd_yitty, rd.expected_rd)     expected_redeemed_rate,
       IFF(a.country = 'US' AND a.brand = 'Yitty', uryt.expected_unrd_yitty, ur.expected_unrd) expected_unredeemed_rate,
       IFF(a.country = 'US' AND a.brand = 'Yitty', rd2yt.expected_rd_24_yitty,
           rd2.expected_rd_24)                                                                 expected_redeemed_rate_max,
       a.original_issued_amount,

       SUM(issued)                                                                             issued_to_date,
       SUM(redeemed)                                                                           redeemed_to_date,
       SUM(cancelled)                                                                          cancelled_to_date,
       SUM(expired)                                                                            expired_to_date,
       issued_to_date
           - redeemed_to_date
           - cancelled_to_date                                                                 unredeemed_to_date,
       redeemed_to_date / NULLIF(issued_to_date, 0)                                            redeemed_rate_to_date,
       CASE
           WHEN a.brand IN ('Fabletics Womens', 'Fabletics Mens', 'Yitty')
               AND a.country IN ('US', 'CA', 'UK')
               AND DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 13 THEN 0
           WHEN DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 24 THEN 0
           WHEN unredeemed_to_date < expected_unredeemed_rate * issued_to_date THEN 0
           ELSE expected_redeemed_rate_max * issued_to_date - redeemed_to_date END
                                                                                               expected_additional_redeemed,
       CASE
           WHEN a.brand IN ('Fabletics Womens', 'Fabletics Mens', 'Yitty')
               AND a.country IN ('US', 'CA', 'UK')
               AND DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 13 THEN 0
           WHEN DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 24 THEN 0
           WHEN unredeemed_to_date < expected_unredeemed_rate * issued_to_date THEN 0
           WHEN unredeemed_to_date < expected_unredeemed_rate * issued_to_date THEN 0
           ELSE (1 - expected_unredeemed_rate - expected_redeemed_rate_max) * issued_to_date - cancelled_to_date END
                                                                                               expected_additional_cancelled,
       0                                                                                       expected_additional_activity_amount_m14to19,
       redeemed_rate_to_date /
       NULLIF(expected_redeemed_rate, 0)                                                       actual_redeemed_rate_vs_expected,
       CASE
           WHEN a.brand IN ('Fabletics Womens', 'Fabletics Mens', 'Yitty')
               AND a.country IN ('US', 'CA', 'UK')
               AND DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 13 THEN unredeemed_to_date
           WHEN DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 24 THEN unredeemed_to_date
           WHEN unredeemed_to_date < issued_to_date * expected_unredeemed_rate THEN unredeemed_to_date
           ELSE issued_to_date * expected_unredeemed_rate END
                                                                                               expected_breakage_to_record,
       CASE
           WHEN a.brand IN ('Fabletics Womens', 'Fabletics Mens', 'Yitty')
               AND a.country IN ('US', 'CA', 'UK')
               AND DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 13 THEN unredeemed_to_date
           WHEN DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1 > 24 THEN unredeemed_to_date
           WHEN unredeemed_to_date < (expected_breakage_to_record *
                                      IFF(actual_redeemed_rate_vs_expected > 1, 1, actual_redeemed_rate_vs_expected))
               THEN unredeemed_to_date
           ELSE expected_breakage_to_record *
                IFF(actual_redeemed_rate_vs_expected > 1, 1, actual_redeemed_rate_vs_expected) END
                                                                                               breakage_recorded
FROM _activity_amount a
         LEFT JOIN _expected_redemption_rate rd ON a.country = rd.country AND a.brand = rd.brand
         LEFT JOIN _expected_redemption_rate_yitty rdyt ON a.country = rdyt.country AND a.brand = rdyt.brand
         LEFT JOIN _expected_redemption_rate_24 rd2 ON a.country = rd2.country AND a.brand = rd2.brand
         LEFT JOIN _expected_redemption_rate_24_yitty rd2yt ON a.country = rd2yt.country AND a.brand = rd2yt.brand
         LEFT JOIN _expected_unredeem_rate ur ON a.country = ur.country AND a.brand = ur.brand
         LEFT JOIN _expected_unredeem_rate_yitty uryt ON a.country = uryt.country AND a.brand = uryt.brand
GROUP BY a.brand,
         a.region,
         a.country,
         a.original_credit_tender,
         a.original_credit_reason,
         a.original_credit_type,
         a.original_issued_month,
         a.currency,
         $month_thru,
         DATEDIFF(MONTH, a.original_issued_month, $month_thru) + 1,
         expected_redeemed_rate,
         expected_unredeemed_rate,
         expected_redeemed_rate_max,
         a.original_issued_amount
;


MERGE INTO breakage.fabletics_token_breakage_report t
    USING _append_fl_token a
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
                expected_breakage_to_record, breakage_recorded, expected_redeemed_rate_max,
                expected_additional_redeemed, expected_additional_cancelled,
                expected_additional_activity_amount_m14to19
            )
            VALUES (brand, region, country, currency, original_credit_tender, original_credit_reason,
                    original_credit_type,
                    original_issued_amount,
                    original_issued_month, activity_month,
                    credit_tenure, issued_to_date, redeemed_to_date, cancelled_to_date, expired_to_date,
                    unredeemed_to_date,
                    redeemed_rate_to_date,
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
                t.expected_additional_activity_amount_m14to19 = a.expected_additional_activity_amount_m14to19
;

CREATE TRANSIENT TABLE IF NOT EXISTS breakage.fabletics_token_breakage_report_reset (
	brand VARCHAR(16777216),
	region VARCHAR(16777216),
	country VARCHAR(16777216),
	currency VARCHAR(15),
	original_credit_tender VARCHAR(16777216),
	original_credit_reason VARCHAR(16777216),
	original_credit_type VARCHAR(16777216),
	original_issued_amount FLOAT,
	original_issued_month DATE,
	activity_month DATE,
	credit_tenure NUMBER(38,0),
	issued_to_date FLOAT,
	redeemed_to_date FLOAT,
	cancelled_to_date FLOAT,
	expired_to_date FLOAT,
	unredeemed_to_date FLOAT,
	redeemed_rate_to_date FLOAT,
	expected_redeemed_rate FLOAT,
	expected_unredeemed_rate FLOAT,
	actual_redeemed_rate_vs_expected FLOAT,
	expected_breakage_to_record FLOAT,
	breakage_recorded FLOAT,
	expected_redeemed_rate_max FLOAT,
	expected_additional_redeemed FLOAT,
	expected_additional_cancelled FLOAT,
	expected_additional_activity_amount_m14to19 FLOAT
);

MERGE INTO breakage.fabletics_token_breakage_report_reset t
    USING _append_fl_token a
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
                expected_breakage_to_record, breakage_recorded, expected_redeemed_rate_max,
                expected_additional_redeemed, expected_additional_cancelled,
                expected_additional_activity_amount_m14to19
            )
            VALUES (brand, region, country, currency, original_credit_tender, original_credit_reason,
                    original_credit_type,
                    original_issued_amount,
                    original_issued_month, activity_month,
                    credit_tenure, issued_to_date, redeemed_to_date, cancelled_to_date, expired_to_date,
                    unredeemed_to_date,
                    redeemed_rate_to_date,
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
                t.expected_additional_activity_amount_m14to19 = a.expected_additional_activity_amount_m14to19
;

CREATE TRANSIENT TABLE IF NOT EXISTS breakage.fabletics_token_breakage_report_snapshot (
	brand VARCHAR(16777216),
	region VARCHAR(16777216),
	country VARCHAR(16777216),
	currency VARCHAR(15),
	original_credit_tender VARCHAR(16777216),
	original_credit_reason VARCHAR(16777216),
	original_credit_type VARCHAR(16777216),
	original_issued_amount FLOAT,
	original_issued_month DATE,
	activity_month DATE,
	credit_tenure NUMBER(38,0),
	issued_to_date FLOAT,
	redeemed_to_date FLOAT,
	cancelled_to_date FLOAT,
	expired_to_date FLOAT,
	unredeemed_to_date FLOAT,
	redeemed_rate_to_date FLOAT,
	expected_redeemed_rate FLOAT,
	expected_unredeemed_rate FLOAT,
	actual_redeemed_rate_vs_expected FLOAT,
	expected_breakage_to_record FLOAT,
	breakage_recorded FLOAT,
	expected_redeemed_rate_max FLOAT,
	expected_additional_redeemed FLOAT,
	expected_additional_cancelled FLOAT,
	expected_additional_activity_amount_m14to19 FLOAT,
	snapshot_timestamp TIMESTAMP_LTZ(9)
);


INSERT INTO breakage.fabletics_token_breakage_report_snapshot
SELECT brand,
       region,
       country,
       currency,
       original_credit_tender,
       original_credit_reason,
       original_credit_type,
       original_issued_amount,
       original_issued_month,
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
       actual_redeemed_rate_vs_expected,
       expected_breakage_to_record,
       breakage_recorded,
       expected_redeemed_rate_max,
       expected_additional_redeemed,
       expected_additional_cancelled,
       expected_additional_activity_amount_m14to19,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM breakage.fabletics_token_breakage_report;

DELETE
FROM breakage.fabletics_token_breakage_report_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
