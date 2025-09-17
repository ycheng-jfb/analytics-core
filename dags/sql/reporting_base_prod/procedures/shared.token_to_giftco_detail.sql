SET day_thru = DATEADD(DAY, -1, CURRENT_TIMESTAMP()::DATE);

CREATE OR REPLACE TEMP TABLE _original_credit_activity AS
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
       date_trunc('year', dcf.original_credit_issued_local_datetime::DATE)  AS original_issued_year,
       date_trunc('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       date_trunc('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       (fce.credit_activity_local_datetime::DATE)                           AS activity_date,
       SUM(fce.credit_activity_gross_vat_local_amount)                      AS activity_amount,
       'Local Gross VAT'                                                    AS currency
FROM shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled', 'Expired')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand IN ('JustFab', 'FabKids', 'ShoeDazzle')
  AND st.store_country IN ('US')
  AND dcf.giftco_transfer_status <> 'Membership Credit to Giftco'
  AND CASE
          WHEN credit_activity_type IN ('Redeemed', 'Cancelled', 'Expired')
              THEN activity_date BETWEEN '2024-02-02' AND $day_thru
          WHEN credit_activity_type IN ('Issued')
              THEN activity_date > '2010-01-01'
    END
  AND dcf.original_credit_issued_local_datetime::DATE < '2022-01-01'
  AND original_credit_tender = 'Cash'
  AND original_credit_type = 'Fixed Credit'
  AND original_credit_reason = 'Membership Credit'
GROUP BY
    brand,
    country,
    region,
    dcf.giftco_transfer_status,
    credit_activity_type,
    original_credit_tender,
    original_credit_type,
    original_credit_reason,
    original_issued_year,
    original_issued_month,
    activity_month,
    activity_date

UNION

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
       date_trunc('year', dcf.original_credit_issued_local_datetime::DATE)  AS original_issued_year,
       date_trunc('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       date_trunc('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       (fce.credit_activity_local_datetime::DATE)                           AS activity_date,
       SUM(fce.credit_activity_local_amount)                                AS activity_amount,
       'Local Net VAT'                                                      AS currency
FROM shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled', 'Expired')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand IN ('JustFab', 'FabKids', 'ShoeDazzle')
  AND st.store_country IN ('US')
  AND dcf.giftco_transfer_status <> 'Membership Credit to Giftco'
  AND CASE
          WHEN credit_activity_type IN ('Redeemed', 'Cancelled', 'Expired')
              THEN activity_date BETWEEN '2024-02-02' AND $day_thru
          WHEN credit_activity_type IN ('Issued')
              THEN activity_date > '2010-01-01'
    END
  AND dcf.original_credit_issued_local_datetime::DATE < '2022-01-01'
  AND original_credit_tender = 'Cash'
  AND original_credit_type = 'Fixed Credit'
  AND original_credit_reason = 'Membership Credit'
GROUP BY
    brand,
    country,
    region,
    dcf.giftco_transfer_status,
    credit_activity_type,
    original_credit_tender,
    original_credit_type,
    original_credit_reason,
    original_issued_year,
    original_issued_month,
    activity_month,
    activity_date

UNION

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
       date_trunc('year', dcf.original_credit_issued_local_datetime::DATE)  AS original_issued_year,
       date_trunc('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       date_trunc('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       (fce.credit_activity_local_datetime::DATE)                           AS activity_date,
       SUM(fce.credit_activity_gross_vat_local_amount *
           credit_issued_usd_conversion_rate)                               AS activity_amount,
       'USD Gross VAT'                                                      AS currency
FROM shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled', 'Expired')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand IN ('JustFab', 'FabKids', 'ShoeDazzle')
  AND st.store_country IN ('US')
  AND dcf.giftco_transfer_status <> 'Membership Credit to Giftco'
  AND CASE
          WHEN credit_activity_type IN ('Redeemed', 'Cancelled', 'Expired')
              THEN activity_date BETWEEN '2024-02-02' AND $day_thru
          WHEN credit_activity_type IN ('Issued')
              THEN activity_date > '2010-01-01'
    END
  AND dcf.original_credit_issued_local_datetime::DATE < '2022-01-01'
  AND original_credit_tender = 'Cash'
  AND original_credit_type = 'Fixed Credit'
  AND original_credit_reason = 'Membership Credit'
GROUP BY
    brand,
    country,
    region,
    dcf.giftco_transfer_status,
    credit_activity_type,
    original_credit_tender,
    original_credit_type,
    original_credit_reason,
    original_issued_year,
    original_issued_month,
    activity_month,
    activity_date

UNION

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
       date_trunc('year', dcf.original_credit_issued_local_datetime::DATE)  AS original_issued_year,
       date_trunc('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       date_trunc('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       (fce.credit_activity_local_datetime::DATE)                           AS activity_date,
       SUM(fce.credit_activity_local_amount *
           credit_issued_usd_conversion_rate)                               AS activity_amount,
       'USD Net VAT'                                                        AS currency
FROM shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled', 'Expired')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand IN ('JustFab', 'FabKids', 'ShoeDazzle')
  AND st.store_country IN ('US')
  AND dcf.giftco_transfer_status <> 'Membership Credit to Giftco'
  AND CASE
          WHEN credit_activity_type IN ('Redeemed', 'Cancelled', 'Expired')
              THEN activity_date BETWEEN '2024-02-02' AND $day_thru
          WHEN credit_activity_type IN ('Issued')
              THEN activity_date > '2010-01-01'
    END
  AND dcf.original_credit_issued_local_datetime::DATE < '2022-01-01'
  AND original_credit_tender = 'Cash'
  AND original_credit_type = 'Fixed Credit'
  AND original_credit_reason = 'Membership Credit'
GROUP BY
    brand,
    country,
    region,
    dcf.giftco_transfer_status,
    credit_activity_type,
    original_credit_tender,
    original_credit_type,
    original_credit_reason,
    original_issued_year,
    original_issued_month,
    activity_month,
    activity_date

UNION

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
       date_trunc('year', dcf.original_credit_issued_local_datetime::DATE)  AS original_issued_year,
       date_trunc('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       date_trunc('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       fce.credit_activity_local_datetime::date                             AS activity_date, SUM(fce.credit_activity_gross_vat_local_amount) AS activity_amount,
       'Local Gross VAT'                                                    AS currency
FROM shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand IN ('JustFab', 'FabKids', 'ShoeDazzle')
  AND st.store_country NOT IN ('US')
  AND dcf.giftco_transfer_status <> 'Membership Credit to Giftco'
  AND activity_date <= $day_thru
  AND dcf.original_credit_issued_local_datetime::DATE < '2022-01-01'
  AND original_credit_tender = 'Cash'
  AND original_credit_type = 'Fixed Credit'
  AND original_credit_reason = 'Membership Credit'
GROUP BY
    brand,
    country,
    region,
    dcf.giftco_transfer_status,
    credit_activity_type,
    original_credit_tender,
    original_credit_type,
    original_credit_reason,
    original_issued_year,
    original_issued_month,
    activity_month,
    activity_date

UNION

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
       date_trunc('year', dcf.original_credit_issued_local_datetime::DATE)  AS original_issued_year,
       date_trunc('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       date_trunc('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       fce.credit_activity_local_datetime::date                             AS activity_date, SUM(fce.credit_activity_local_amount) AS activity_amount,
       'Local Net VAT'                                                      AS currency
FROM shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand IN ('JustFab', 'FabKids', 'ShoeDazzle')
  AND st.store_country NOT IN ('US')
  AND dcf.giftco_transfer_status <> 'Membership Credit to Giftco'
  AND activity_date <= $day_thru
  AND dcf.original_credit_issued_local_datetime::DATE < '2022-01-01'
  AND original_credit_tender = 'Cash'
  AND original_credit_type = 'Fixed Credit'
  AND original_credit_reason = 'Membership Credit'
GROUP BY
    brand,
    country,
    region,
    dcf.giftco_transfer_status,
    credit_activity_type,
    original_credit_tender,
    original_credit_type,
    original_credit_reason,
    original_issued_year,
    original_issued_month,
    activity_month,
    activity_date

UNION

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
       date_trunc('year', dcf.original_credit_issued_local_datetime::DATE)  AS original_issued_year,
       date_trunc('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       date_trunc('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       fce.credit_activity_local_datetime::date                             AS activity_date, SUM(
        fce.credit_activity_gross_vat_local_amount *
        credit_issued_usd_conversion_rate) AS activity_amount,
       'USD Gross VAT'                                                      AS currency
FROM shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand IN ('JustFab', 'FabKids', 'ShoeDazzle')
  AND st.store_country NOT IN ('US')
  AND dcf.giftco_transfer_status <> 'Membership Credit to Giftco'
  AND activity_date <= $day_thru
  AND dcf.original_credit_issued_local_datetime::DATE < '2022-01-01'
  AND original_credit_tender = 'Cash'
  AND original_credit_type = 'Fixed Credit'
  AND original_credit_reason = 'Membership Credit'
GROUP BY
    brand,
    country,
    region,
    dcf.giftco_transfer_status,
    credit_activity_type,
    original_credit_tender,
    original_credit_type,
    original_credit_reason,
    original_issued_year,
    original_issued_month,
    activity_month,
    activity_date

UNION

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
       date_trunc('year', dcf.original_credit_issued_local_datetime::DATE)  AS original_issued_year,
       date_trunc('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       date_trunc('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       fce.credit_activity_local_datetime::date                             AS activity_date, SUM(
        fce.credit_activity_local_amount *
        credit_issued_usd_conversion_rate) AS activity_amount,
       'USD Net VAT'                                                        AS currency
FROM shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand IN ('JustFab', 'FabKids', 'ShoeDazzle')
  AND st.store_country NOT IN ('US')
  AND dcf.giftco_transfer_status <> 'Membership Credit to Giftco'
  AND activity_date <= $day_thru
  AND dcf.original_credit_issued_local_datetime::DATE < '2022-01-01'
  AND original_credit_tender = 'Cash'
  AND original_credit_type = 'Fixed Credit'
  AND original_credit_reason = 'Membership Credit'
GROUP BY
    brand,
    country,
    region,
    dcf.giftco_transfer_status,
    credit_activity_type,
    original_credit_tender,
    original_credit_type,
    original_credit_reason,
    original_issued_year,
    original_issued_month,
    activity_month,
    activity_date
;

CREATE OR REPLACE TEMPORARY TABLE _token_to_giftco_base AS
SELECT brand,
       region,
       country,
       giftco_transfer_status,
       currency,
       original_credit_tender,
       original_credit_reason,
       original_credit_type,
       original_issued_year,
       original_issued_month,
       activity_date,
       SUM(iff(credit_activity_type = 'Issued', activity_amount, 0))    AS issued,
       SUM(iff(credit_activity_type = 'Redeemed', activity_amount, 0))  AS redeemed,
       SUM(iff(credit_activity_type = 'Cancelled', activity_amount, 0)) AS cancelled,
       SUM(iff(credit_activity_type = 'Expired', activity_amount, 0))   AS expired
FROM _original_credit_activity
GROUP BY brand,
         region,
         country,
         giftco_transfer_status,
         currency,
         original_credit_tender,
         original_credit_reason,
         original_credit_type,
         original_issued_year,
         original_issued_month,
         activity_date;

CREATE OR REPLACE TEMPORARY TABLE _date_base AS
SELECT *
FROM (SELECT DISTINCT brand,
                      region,
                      country,
                      giftco_transfer_status,
                      currency,
                      original_issued_year,
                      original_issued_month
      FROM _token_to_giftco_base) base
         CROSS JOIN
     (SELECT full_date
      FROM edw_prod.data_model.dim_date
      WHERE full_date BETWEEN '2024-02-02' AND $day_thru) dates;

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.token_to_giftco_detail AS
SELECT COALESCE(ttg.brand, db.brand)                                   AS brand,
       COALESCE(ttg.region, db.region)                                 AS region,
       COALESCE(ttg.country, db.country)                               AS country,
       COALESCE(ttg.giftco_transfer_status, db.giftco_transfer_status) AS giftco_transfer_status,
       COALESCE(ttg.currency, db.currency)                             AS currency,
       original_credit_tender,
       original_credit_reason,
       original_credit_type,
       COALESCE(ttg.original_issued_year, db.original_issued_year)     AS original_issued_year,
       COALESCE(ttg.original_issued_month, db.original_issued_month)   AS original_issued_month,
       activity_date,
       COALESCE(issued, 0)                                             AS issued,
       COALESCE(redeemed, 0)                                           AS redeemed,
       COALESCE(cancelled, 0)                                          AS cancelled,
       COALESCE(expired, 0)                                            AS expired,
       full_date
FROM _token_to_giftco_base ttg
         FULL OUTER JOIN _date_base db
                         ON ttg.brand = db.brand
                             AND ttg.country = db.country
                             AND ttg.giftco_transfer_status = db.giftco_transfer_status
                             AND ttg.currency = db.currency
                             AND ttg.original_issued_year = db.original_issued_year
                             AND ttg.original_issued_month = db.original_issued_month
                             AND ttg.activity_date = db.full_date;
