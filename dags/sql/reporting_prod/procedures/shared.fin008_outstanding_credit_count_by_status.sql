CREATE OR REPLACE TEMPORARY TABLE _dates_for_fin AS
WITH _all_dates AS (SELECT DISTINCT month_date
                    FROM edw_prod.data_model.dim_date
                    WHERE month_date >= LAST_DAY(DATEADD(MONTH, - 2, CURRENT_TIMESTAMP()))
                      AND month_date < DATE_TRUNC(MONTH, CURRENT_TIMESTAMP())),
     _date_combinations AS (SELECT LAST_DAY(month_date)::DATE                          AS credit_active_as_of_date,
                                   credit_active_as_of_date                            AS credit_issued_max_date,
                                   DATE_TRUNC(MONTH, credit_issued_max_date::DATE) - 1 AS vip_cohort_max_date,
                                   credit_active_as_of_date::DATE                      AS member_status_date
                            FROM _all_dates
                            UNION ALL
                            SELECT LAST_DAY(month_date)::DATE                          AS credit_active_as_of_date,
                                   DATEADD(MONTH, -12, credit_active_as_of_date)       AS credit_issued_max_date,
                                   DATE_TRUNC(MONTH, credit_issued_max_date::DATE) - 1 AS vip_cohort_max_date,
                                   credit_active_as_of_date::DATE                      AS member_status_date
                            FROM _all_dates)
SELECT credit_active_as_of_date,
       credit_issued_max_date,
       vip_cohort_max_date,
       member_status_date,
       row_number() over (ORDER BY credit_active_as_of_date ASC) AS num_for_recursion
FROM _date_combinations
ORDER BY credit_active_as_of_date DESC;


CREATE OR REPLACE TEMPORARY TABLE _first_vip_cohort AS
WITH _first_cohort AS
(
SELECT
  CASE WHEN store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
           WHEN store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE store_brand END                             AS store_brand,
       store_country                                        AS store_country,
       store_region                                         AS store_region,
       v.customer_id,
       DATE_TRUNC(MONTH, (SELECT member_status_date
                  FROM _dates_for_fin WHERE num_for_recursion=1) ) AS membership_month_date,
       IFF(v2.membership_state = 'VIP', 'VIP', 'Cancelled') AS member_status,
       1                                                    AS date_flag,
       DATE_TRUNC(MONTH, v.activation_local_datetime)::DATE AS first_vip_cohort
FROM edw_prod.data_model.fact_activation v
         JOIN edw_prod.data_model.dim_store st ON st.store_id = v.store_id
         JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = v.customer_id
         LEFT JOIN edw_prod.data_model.fact_membership_event v2 ON v2.customer_id = dc.customer_id
    AND v2.event_start_local_datetime::DATE < DATEADD(DAY, 1, (SELECT TO_VARCHAR(member_status_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=1) )
    AND v2.event_end_local_datetime::DATE >= DATEADD(DAY, 1, (SELECT TO_VARCHAR(member_status_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=1) )
WHERE v.activation_local_datetime::DATE <= (SELECT TO_VARCHAR(vip_cohort_max_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=1)
  AND v.activation_sequence_number = 1
UNION ALL
 SELECT CASE
           WHEN store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
           WHEN store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE store_brand END                             AS store_brand,
       store_country                                        AS store_country,
       store_region                                         AS store_region,
       v.customer_id,
       DATE_TRUNC(MONTH, (SELECT member_status_date
                  FROM _dates_for_fin WHERE num_for_recursion=2) ) AS membership_month_date,
       IFF(v2.membership_state = 'VIP', 'VIP', 'Cancelled') AS member_status,
       2                                                    AS Date_flag,
       DATE_TRUNC(MONTH, v.activation_local_datetime)::DATE AS first_vip_cohort
FROM edw_prod.data_model.fact_activation v
         JOIN edw_prod.data_model.dim_store st ON st.store_id = v.store_id
         JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = v.customer_id
         LEFT JOIN edw_prod.data_model.fact_membership_event v2 ON v2.customer_id = dc.customer_id
    AND v2.event_start_local_datetime::DATE < DATEADD(DAY, 1, (SELECT TO_VARCHAR(member_status_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=2) )
    AND v2.event_end_local_datetime::DATE >= DATEADD(DAY, 1, (SELECT TO_VARCHAR(member_status_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=2) )
WHERE v.activation_local_datetime::DATE <= (SELECT TO_VARCHAR(vip_cohort_max_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=2)
   AND v.activation_sequence_number = 1
)
SELECT store_brand,
       store_country,
       store_region,
       customer_id,
       membership_month_date,
       member_status,
       date_flag,
       first_vip_cohort
FROM _first_cohort;


CREATE OR REPLACE TEMPORARY TABLE _customers_without_cohort AS
WITH _customers_cohort AS
(SELECT CASE
           WHEN st.store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                                                  AS store_brand,
       st.store_country                                                                             AS store_country,
       st.store_region                                                                              AS store_region,
       dcf2.customer_id,
       DATE_TRUNC(MONTH, (SELECT member_status_date
                  FROM _dates_for_fin WHERE num_for_recursion=1))                                   AS membership_month_date,
       IFF(v2.membership_state = 'VIP', 'VIP', 'Cancelled')                                         AS member_status,
       1                                                                                            AS date_flag,
       MIN(DATEADD(MONTH, -1, DATE_TRUNC(MONTH, dcf2.original_credit_issued_local_datetime))::DATE) AS vip_cohort
FROM reporting_base_prod.shared.dim_credit dcf2
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf2.store_id
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf2.customer_id
         LEFT JOIN _first_vip_cohort vc ON dcf2.customer_id = vc.customer_id
            AND vc.date_flag = 1
         LEFT JOIN edw_prod.data_model.fact_membership_event v2 ON v2.customer_id = dcf2.customer_id
    AND v2.event_start_local_datetime < DATEADD(DAY, 1, (SELECT TO_VARCHAR(member_status_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=1))
    AND v2.event_end_local_datetime >= DATEADD(DAY, 1, (SELECT TO_VARCHAR(member_status_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=1))
WHERE ((dcf2.credit_reason = 'Membership Credit' AND dcf2.credit_type = 'Fixed Credit')
    OR (dcf2.credit_type = 'Token' AND dcf2.credit_reason = 'Token Billing'))
  AND vc.customer_id IS NULL
  AND dcf2.credit_issued_hq_datetime::DATE <= (SELECT TO_VARCHAR(vip_cohort_max_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=1)
GROUP BY gender,
         st.store_brand,
         st.store_country,
         st.store_region,
         dcf2.customer_id,
         v2.membership_state,
         membership_month_date
UNION ALL
SELECT CASE
           WHEN st.store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                                                  AS store_brand,
       st.store_country                                                                             AS store_country,
       st.store_region                                                                              AS store_region,
       dcf2.customer_id,
       DATE_TRUNC(MONTH, (SELECT member_status_date
                            FROM _dates_for_fin WHERE num_for_recursion=2))                         AS membership_month_date,
       IFF(v2.membership_state = 'VIP', 'VIP', 'Cancelled')                                         AS member_status,
       2                                                                                            AS date_flag,
       MIN(DATEADD(MONTH, -1, DATE_TRUNC(MONTH, dcf2.original_credit_issued_local_datetime))::DATE) AS vip_cohort
FROM reporting_base_prod.shared.dim_credit dcf2
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf2.store_id
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf2.customer_id
         LEFT JOIN _first_vip_cohort vc ON dcf2.customer_id = vc.customer_id
            AND vc.date_flag = 2
         LEFT JOIN edw_prod.data_model.fact_membership_event v2 ON v2.customer_id = dcf2.customer_id
    AND v2.event_start_local_datetime < DATEADD(DAY, 1, (SELECT TO_VARCHAR(member_status_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=2))
    AND v2.event_end_local_datetime >= DATEADD(DAY, 1, (SELECT TO_VARCHAR(member_status_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=2))
WHERE ((dcf2.credit_reason = 'Membership Credit' AND dcf2.credit_type = 'Fixed Credit')
    OR (dcf2.credit_type = 'Token' AND dcf2.credit_reason = 'Token Billing'))
  AND vc.customer_id IS NULL
  AND dcf2.credit_issued_hq_datetime::DATE <= (SELECT vip_cohort_max_date
                                                                FROM _dates_for_fin WHERE num_for_recursion=2)
GROUP BY gender,
         st.store_brand,
         st.store_country,
         st.store_region,
         dcf2.customer_id,
         v2.membership_state,
         membership_month_date
)
SELECT store_brand,
       store_country,
       store_region,
       customer_id,
       membership_month_date,
       member_status,
       date_flag,
       vip_cohort
FROM _customers_cohort;


CREATE OR REPLACE TEMPORARY TABLE _first_vip_cohort_final AS
SELECT store_brand,
       store_country,
       store_region,
       customer_id,
       membership_month_date,
       member_status,
       date_flag,
       first_vip_cohort
FROM _first_vip_cohort
UNION
SELECT store_brand,
       store_country,
       store_region,
       customer_id,
       membership_month_date,
       member_status,
       date_flag,
       vip_cohort
FROM _customers_without_cohort;


CREATE OR REPLACE TEMPORARY TABLE _original_credit_activity AS
WITH _credit_activity_original AS
(SELECT first_vip_cohort,
       CASE
           WHEN st.store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                          AS store_brand,
       st.store_country,
       st.store_region,
       dcf.customer_id,
       dcf.credit_issued_local_gross_vat_amount                             AS credit_price,
       credit_activity_type                                                 AS activity_type,
       member_status,
       f.membership_month_date,
       DATE_TRUNC(MONTH, (SELECT credit_active_as_of_date
                  FROM _dates_for_fin WHERE num_for_recursion=1))           AS credit_activity_month_date_max,
       DATE_TRUNC(MONTH, (SELECT credit_issued_max_date
                  FROM _dates_for_fin WHERE num_for_recursion=1))           AS credit_issuance_month_date_max,
       dcf.original_credit_tender                                           AS original_credit_tender,
       dcf.original_credit_type                                             AS original_credit_type,
       dcf.original_credit_reason                                           AS original_credit_reason,
       DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       1                                                                    AS date_flag,
       SUM(fce.credit_activity_gross_vat_local_amount)                      AS activity_amount
FROM reporting_base_prod.shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN reporting_base_prod.shared.fact_credit_event fce ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
         LEFT JOIN _first_vip_cohort_final f ON f.customer_id = dcf.customer_id
             AND f.date_flag = 1
WHERE credit_activity_type IN ('Issued', 'Cancelled', 'Redeemed', 'Expired')
  AND original_credit_activity_type_action = 'Include'
  AND ((dcf.original_credit_reason = 'Membership Credit' AND dcf.original_credit_type = 'Fixed Credit')
    OR (dcf.original_credit_type = 'Token' AND dcf.original_credit_reason = 'Token Billing'))
  AND original_credit_issued_local_datetime >=
      IFF(st.store_country = 'US', '2018-12-01', '2010-01-01')
  AND original_credit_issued_local_datetime <= (SELECT TO_VARCHAR(credit_issued_max_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=1)
  AND credit_activity_local_datetime <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=1)
GROUP BY first_vip_cohort,
         st.store_brand,
         gender,
         st.store_country,
         st.store_region,
         dcf.customer_id,
         dcf.credit_issued_local_gross_vat_amount,
         credit_activity_type,
         member_status,
         f.membership_month_date,
         credit_activity_month_date_max,
         credit_issuance_month_date_max,
         original_credit_tender,
         original_credit_type,
         original_credit_reason,
         original_issued_month,
         activity_month
UNION ALL
SELECT first_vip_cohort,
       CASE
           WHEN st.store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                          AS store_brand,
       st.store_country,
       st.store_region,
       dcf.customer_id,
       dcf.credit_issued_local_gross_vat_amount                             AS credit_price,
       credit_activity_type                                                 AS activity_type,
       member_status,
       f.membership_month_date,
       DATE_TRUNC(MONTH, (SELECT credit_active_as_of_date
                  FROM _dates_for_fin WHERE num_for_recursion=2))           AS credit_activity_month_date_max,
       DATE_TRUNC(MONTH, (SELECT credit_issued_max_date
                  FROM _dates_for_fin WHERE num_for_recursion=2))           AS credit_issuance_month_date_max,
       dcf.original_credit_tender                                           AS original_credit_tender,
       dcf.original_credit_type                                             AS original_credit_type,
       dcf.original_credit_reason                                           AS original_credit_reason,
       DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
        2                                                                   AS date_flag,
       SUM(fce.credit_activity_gross_vat_local_amount)                      AS activity_amount
FROM reporting_base_prod.shared.dim_credit dcf
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN reporting_base_prod.shared.fact_credit_event fce ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
         LEFT JOIN _first_vip_cohort_final f ON f.customer_id = dcf.customer_id
                AND f.date_flag=2
WHERE credit_activity_type IN ('Issued', 'Cancelled', 'Redeemed', 'Expired')
  AND original_credit_activity_type_action = 'Include'
  AND ((dcf.original_credit_reason = 'Membership Credit' AND dcf.original_credit_type = 'Fixed Credit')
    OR (dcf.original_credit_type = 'Token' AND dcf.original_credit_reason = 'Token Billing'))
  AND original_credit_issued_local_datetime >=
      IFF(st.store_country = 'US', '2018-12-01', '2010-01-01')
  AND original_credit_issued_local_datetime <= (SELECT TO_VARCHAR(credit_issued_max_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=2)
  AND credit_activity_local_datetime <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=2)
GROUP BY first_vip_cohort,
         st.store_brand,
         gender,
         st.store_country,
         st.store_region,
         dcf.customer_id,
         dcf.credit_issued_local_gross_vat_amount,
         credit_activity_type,
         member_status,
         f.membership_month_date,
         credit_activity_month_date_max,
         credit_issuance_month_date_max,
         original_credit_tender,
         original_credit_type,
         original_credit_reason,
         original_issued_month,
         activity_month
)
SELECT first_vip_cohort,
       store_brand,
       store_country,
       store_region,
       customer_id,
       credit_price,
       activity_type,
       member_status,
       membership_month_date,
       credit_activity_month_date_max,
       credit_issuance_month_date_max,
       original_credit_tender,
       original_credit_type,
       original_credit_reason,
       original_issued_month,
       activity_month,
       date_flag,
       activity_amount
FROM _credit_activity_original;


CREATE OR REPLACE TEMPORARY TABLE _by_customer_by_price AS
WITH _customer_price AS
(SELECT store_brand,
       store_country,
       store_region,
       member_status,
       o.customer_id,
       credit_price,
       first_vip_cohort,
       membership_month_date,
       credit_issuance_month_date_max,
       credit_activity_month_date_max,
       1 AS date_flag,
       SUM(IFF(activity_type = 'Issued' AND activity_month <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=1) AND date_flag=1, activity_amount, 0))
           - SUM(IFF(activity_type = 'Redeemed' AND activity_month <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=1) AND date_flag=1, activity_amount, 0))
           - SUM(IFF(activity_type = 'Cancelled' AND activity_month <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=1) AND date_flag=1, activity_amount, 0))
           - SUM(IFF(activity_type = 'Expired' AND activity_month <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=1) AND date_flag=1, activity_amount,
                     0))                                                             AS outstanding_amount,
       IFF(outstanding_amount < 0, 0, ROUND(outstanding_amount / (credit_price), 0)) AS outstanding_count
FROM _original_credit_activity o
 WHERE date_flag=1
GROUP BY store_brand,
         store_country,
         store_region,
         member_status,
         o.customer_id,
         credit_price,
         first_vip_cohort,
         membership_month_date,
         credit_issuance_month_date_max,
         credit_activity_month_date_max,
         date_flag
UNION ALL
SELECT store_brand,
       store_country,
       store_region,
       member_status,
       o.customer_id,
       credit_price,
       first_vip_cohort,
       membership_month_date,
       credit_issuance_month_date_max,
       credit_activity_month_date_max,
       2 AS date_flag,
       SUM(IFF(activity_type = 'Issued' AND activity_month <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=2) AND date_flag=2, activity_amount, 0))
           - SUM(IFF(activity_type = 'Redeemed' AND activity_month <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=2) AND date_flag=2, activity_amount, 0))
           - SUM(IFF(activity_type = 'Cancelled' AND activity_month <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=2) AND date_flag=2, activity_amount, 0))
           - SUM(IFF(activity_type = 'Expired' AND activity_month <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM _dates_for_fin WHERE num_for_recursion=2) AND date_flag=2, activity_amount,
                     0))                                                             AS outstanding_amount,
       IFF(outstanding_amount < 0, 0, ROUND(outstanding_amount / (credit_price), 0)) AS outstanding_count
FROM _original_credit_activity o
 WHERE date_flag=2
GROUP BY store_brand,
         store_country,
         store_region,
         member_status,
         o.customer_id,
         credit_price,
         first_vip_cohort,
         membership_month_date,
         credit_issuance_month_date_max,
         credit_activity_month_date_max,
         date_flag
)
SELECT store_brand,
       store_country,
       store_region,
       member_status,
       customer_id,
       credit_price,
       first_vip_cohort,
       membership_month_date,
       credit_issuance_month_date_max,
       credit_activity_month_date_max,
       date_flag,
       outstanding_amount,
       outstanding_count
FROM _customer_price;


CREATE OR REPLACE TEMPORARY TABLE _by_customer AS
SELECT store_brand,
       store_country,
       store_region,
       member_status,
       customer_id,
       first_vip_cohort,
       membership_month_date,
       credit_activity_month_date_max,
       credit_issuance_month_date_max,
       1 AS date_flag,
       SUM(CASE WHEN date_flag=1 THEN outstanding_amount ELSE 0 END) outstanding_amount,
       SUM(CASE WHEN date_flag=1 THEN outstanding_count ELSE 0 END)  outstanding_count
FROM _by_customer_by_price
    WHERE date_flag=1
GROUP BY store_brand,
         store_country,
         store_region,
         member_status,
         customer_id,
         first_vip_cohort,
         membership_month_date,
         credit_activity_month_date_max,
         credit_issuance_month_date_max,
         date_flag
UNION ALL
SELECT store_brand,
       store_country,
       store_region,
       member_status,
       customer_id,
       first_vip_cohort,
       membership_month_date,
       credit_activity_month_date_max,
       credit_issuance_month_date_max,
       2 AS date_flag,
       SUM(CASE WHEN date_flag=2 THEN outstanding_amount ELSE 0 END) outstanding_amount,
       SUM(CASE WHEN date_flag=2 THEN outstanding_count ELSE 0 END)  outstanding_count
FROM _by_customer_by_price
    WHERE date_flag=2
GROUP BY store_brand,
         store_country,
         store_region,
         member_status,
         customer_id,
         first_vip_cohort,
         membership_month_date,
         credit_activity_month_date_max,
         credit_issuance_month_date_max,
         date_flag;


INSERT INTO _by_customer
SELECT a.store_brand,
       a.store_country,
       a.store_region,
       a.member_status,
       a.customer_id,
       a.first_vip_cohort,
       a.membership_month_date,
       a.membership_month_date AS credit_activity_month_date_max,
       a.membership_month_date AS credit_issuance_month_date_max,
       1                       AS date_flag,
       0                       AS outstanding_amount,
       0                       AS outstanding_count
FROM _first_vip_cohort a
         LEFT JOIN _by_customer bc ON a.customer_id = bc.customer_id
         AND bc.date_flag=1
WHERE bc.customer_id IS NULL AND a.date_flag=1
UNION ALL
SELECT
       a.store_brand,
       a.store_country,
       a.store_region,
       a.member_status,
       a.customer_id,
       a.first_vip_cohort,
       a.membership_month_date,
       a.membership_month_date AS credit_activity_month_date_max,
       a.membership_month_date AS credit_issuance_month_date_max,
       2                       AS date_flag,
       0                       AS outstanding_amount,
       0                       AS outstanding_count
FROM _first_vip_cohort a
         LEFT JOIN _by_customer bc ON a.customer_id = bc.customer_id
         AND bc.date_flag=2
WHERE bc.customer_id IS NULL AND a.date_flag=2;


CREATE OR REPLACE TEMPORARY TABLE _by_customer_final AS
SELECT store_brand,
       store_country,
       store_region,
       member_status,
       customer_id,
       first_vip_cohort,
       membership_month_date,
       credit_activity_month_date_max,
       credit_issuance_month_date_max,
       date_flag,
       outstanding_amount,
       outstanding_count
FROM (SELECT store_brand,
             store_country,
             store_region,
             member_status,
             customer_id,
             first_vip_cohort,
             membership_month_date,
             credit_activity_month_date_max,
             credit_issuance_month_date_max,
             date_flag,
             outstanding_amount,
             outstanding_count
      FROM _by_customer
      WHERE date_flag=1
      UNION
      SELECT a.store_brand,
             a.store_country,
             a.store_region,
             a.member_status,
             a.customer_id,
             a.first_vip_cohort,
             a.membership_month_date,
             a.membership_month_date,
             a.membership_month_date,
             1 AS date_flag,
             0 AS outstanding_amount,
             0 AS outstanding_count
      FROM _first_vip_cohort_final a
               LEFT JOIN _by_customer bc ON a.customer_id = bc.customer_id
                AND bc.date_flag=1
      WHERE bc.customer_id IS NULL AND a.date_flag=1)
WHERE member_status IS NOT NULL
UNION ALL
SELECT store_brand,
       store_country,
       store_region,
       member_status,
       customer_id,
       first_vip_cohort,
       membership_month_date,
       credit_activity_month_date_max,
       credit_issuance_month_date_max,
       date_flag,
       outstanding_amount,
       outstanding_count
FROM (SELECT store_brand,
             store_country,
             store_region,
             member_status,
             customer_id,
             first_vip_cohort,
             membership_month_date,
             credit_activity_month_date_max,
             credit_issuance_month_date_max,
             date_flag,
             outstanding_amount,
             outstanding_count
      FROM _by_customer
      WHERE date_flag=2
      UNION
      SELECT a.store_brand,
             a.store_country,
             a.store_region,
             a.member_status,
             a.customer_id,
             a.first_vip_cohort,
             a.membership_month_date,
             a.membership_month_date,
             a.membership_month_date,
             2 AS date_flag,
             0 AS outstanding_amount,
             0 AS outstanding_count
      FROM _first_vip_cohort_final a
               LEFT JOIN _by_customer bc ON a.customer_id = bc.customer_id
                AND bc.date_flag=2
      WHERE bc.customer_id IS NULL AND a.date_flag=2)
WHERE member_status IS NOT NULL;


CREATE TABLE IF NOT EXISTS REPORTING_PROD.SHARED.FIN008_BY_CUSTOMER_SNAPSHOT (
	STORE_BRAND VARCHAR,
	STORE_COUNTRY VARCHAR,
	STORE_REGION VARCHAR,
	MEMBER_STATUS VARCHAR,
	CUSTOMER_ID NUMBER,
	FIRST_VIP_COHORT DATE,
	MEMBERSHIP_MONTH_DATE DATE,
	CREDIT_ACTIVITY_MONTH_DATE_MAX DATE,
	CREDIT_ISSUANCE_MONTH_DATE_MAX DATE,
	DATE_FLAG NUMBER,
	OUTSTANDING_AMOUNT NUMBER,
	OUTSTANDING_COUNT NUMBER,
    INSERT_DATETIME DATETIME
);


MERGE INTO REPORTING_PROD.SHARED.FIN008_BY_CUSTOMER_SNAPSHOT AS target
USING (
    SELECT *,
           CURRENT_TIMESTAMP AS INSERT_DATETIME
    FROM _by_customer_final
) AS source
ON target.STORE_BRAND = source.STORE_BRAND
AND target.STORE_COUNTRY = source.STORE_COUNTRY
AND target.STORE_REGION = source.STORE_REGION
AND target.MEMBER_STATUS = source.MEMBER_STATUS
AND target.CUSTOMER_ID = source.CUSTOMER_ID
AND target.FIRST_VIP_COHORT = source.FIRST_VIP_COHORT
AND target.MEMBERSHIP_MONTH_DATE = source.MEMBERSHIP_MONTH_DATE
AND target.CREDIT_ACTIVITY_MONTH_DATE_MAX = source.CREDIT_ACTIVITY_MONTH_DATE_MAX
AND target.CREDIT_ISSUANCE_MONTH_DATE_MAX = source.CREDIT_ISSUANCE_MONTH_DATE_MAX
AND target.DATE_FLAG = source.DATE_FLAG
WHEN MATCHED THEN
    UPDATE SET
        target.OUTSTANDING_AMOUNT = source.OUTSTANDING_AMOUNT,
        target.OUTSTANDING_COUNT = source.OUTSTANDING_COUNT,
        target.INSERT_DATETIME = source.INSERT_DATETIME
WHEN NOT MATCHED THEN
    INSERT (STORE_BRAND, STORE_COUNTRY, STORE_REGION, MEMBER_STATUS, CUSTOMER_ID,
            FIRST_VIP_COHORT, MEMBERSHIP_MONTH_DATE, CREDIT_ACTIVITY_MONTH_DATE_MAX,
            CREDIT_ISSUANCE_MONTH_DATE_MAX, DATE_FLAG, OUTSTANDING_AMOUNT,
            OUTSTANDING_COUNT, INSERT_DATETIME)
    VALUES (source.STORE_BRAND, source.STORE_COUNTRY, source.STORE_REGION,
            source.MEMBER_STATUS, source.CUSTOMER_ID, source.FIRST_VIP_COHORT,
            source.MEMBERSHIP_MONTH_DATE, source.CREDIT_ACTIVITY_MONTH_DATE_MAX,
            source.CREDIT_ISSUANCE_MONTH_DATE_MAX, source.DATE_FLAG,
            source.OUTSTANDING_AMOUNT, source.OUTSTANDING_COUNT,
            source.INSERT_DATETIME);


DELETE FROM REPORTING_PROD.SHARED.FIN008_BY_CUSTOMER_SNAPSHOT
WHERE DATEDIFF(MONTH,credit_activity_month_date_max,current_date)>12;


CREATE OR REPLACE TEMPORARY TABLE _final_table_for_fin AS
SELECT YEAR((SELECT credit_active_as_of_date
                  FROM _dates_for_fin WHERE num_for_recursion=1))::TEXT || '-' || UPPER(MONTHNAME((SELECT credit_active_as_of_date
                                                                                                    FROM _dates_for_fin WHERE num_for_recursion=1))) ||
       DAY((SELECT credit_active_as_of_date
                  FROM _dates_for_fin WHERE num_for_recursion=1))::TEXT      AS credit_outstanding_date,
       YEAR((SELECT credit_issued_max_date
                  FROM _dates_for_fin WHERE num_for_recursion=1))::TEXT || '-' || UPPER(MONTHNAME((SELECT credit_issued_max_date
                                                                                                    FROM _dates_for_fin WHERE num_for_recursion=1))) ||
       DAY((SELECT credit_issued_max_date
                  FROM _dates_for_fin WHERE num_for_recursion=1))::TEXT        AS credit_issued_max_date,
       YEAR((SELECT vip_cohort_max_date
                  FROM _dates_for_fin WHERE num_for_recursion=1))::TEXT || '-' || UPPER(MONTHNAME((SELECT vip_cohort_max_date
                                                                                                    FROM _dates_for_fin WHERE num_for_recursion=1))) ||
       DAY((SELECT vip_cohort_max_date
                  FROM _dates_for_fin WHERE num_for_recursion=1))::TEXT           AS vip_cohort_max_date,
       YEAR((SELECT member_status_date
                  FROM _dates_for_fin WHERE num_for_recursion=1))::TEXT || '-' || UPPER(MONTHNAME((SELECT member_status_date
                                                                                                    FROM _dates_for_fin WHERE num_for_recursion=1))) ||
       DAY((SELECT member_status_date
                  FROM _dates_for_fin WHERE num_for_recursion=1))::TEXT            AS member_status_classification_date,
       store_brand,
       store_country,
       store_region,
       member_status,
       first_vip_cohort,
       date_flag,
       CASE
           WHEN outstanding_count >= 10 THEN '10+'
           ELSE CAST(outstanding_count AS VARCHAR) END AS outstanding_count_bucket,
       COUNT(DISTINCT customer_id)                     AS customer_count,
       SUM(outstanding_amount)                         AS outstanding_amount,
       SUM(outstanding_count)                          AS outstanding_count
FROM _by_customer_final
WHERE date_flag=1
GROUP BY credit_outstanding_date,
         credit_issued_max_date,
         vip_cohort_max_date,
         member_status_classification_date,
         store_brand,
         store_country,
         store_region,
         member_status,
         first_vip_cohort,
         date_flag,
         outstanding_count_bucket
UNION ALL
SELECT
YEAR((SELECT credit_active_as_of_date                                                --
                  FROM _dates_for_fin WHERE num_for_recursion=2))::TEXT || '-' || UPPER(MONTHNAME((SELECT credit_active_as_of_date
                                                                                                    FROM _dates_for_fin WHERE num_for_recursion=2))) ||
       DAY((SELECT credit_active_as_of_date
                  FROM _dates_for_fin WHERE num_for_recursion=2))::TEXT      AS credit_outstanding_date,
       YEAR((SELECT credit_issued_max_date
                  FROM _dates_for_fin WHERE num_for_recursion=2))::TEXT || '-' || UPPER(MONTHNAME((SELECT credit_issued_max_date
                                                                                                    FROM _dates_for_fin WHERE num_for_recursion=2))) ||
       DAY((SELECT credit_issued_max_date
                  FROM _dates_for_fin WHERE num_for_recursion=2))::TEXT        AS credit_issued_max_date,
       YEAR((SELECT vip_cohort_max_date
                  FROM _dates_for_fin WHERE num_for_recursion=2))::TEXT || '-' || UPPER(MONTHNAME((SELECT vip_cohort_max_date
                                                                                                    FROM _dates_for_fin WHERE num_for_recursion=2))) ||
       DAY((SELECT vip_cohort_max_date
                  FROM _dates_for_fin WHERE num_for_recursion=2))::TEXT           AS vip_cohort_max_date,
       YEAR((SELECT member_status_date
                  FROM _dates_for_fin WHERE num_for_recursion=2))::TEXT || '-' || UPPER(MONTHNAME((SELECT member_status_date
                                                                                                    FROM _dates_for_fin WHERE num_for_recursion=2))) ||
       DAY((SELECT member_status_date
                  FROM _dates_for_fin WHERE num_for_recursion=2))::TEXT            AS member_status_classification_date,
       store_brand,
       store_country,
       store_region,
       member_status,
       first_vip_cohort,
       date_flag,
       CASE
           WHEN outstanding_count >= 10 THEN '10+'
           ELSE CAST(outstanding_count AS VARCHAR) END AS outstanding_count_bucket,
       COUNT(DISTINCT customer_id)                     AS customer_count,
       SUM(outstanding_amount)                         AS outstanding_amount,
       SUM(outstanding_count)                          AS outstanding_count
FROM _by_customer_final
WHERE date_flag=2
GROUP BY credit_outstanding_date,
         credit_issued_max_date,
         vip_cohort_max_date,
         member_status_classification_date,
         store_brand,
         store_country,
         store_region,
         member_status,
         first_vip_cohort,
         date_flag,
         outstanding_count_bucket;


MERGE INTO reporting_prod.shared.fin008_outstanding_credit_count_by_status foc
USING _final_table_for_fin ft
    ON foc.CREDIT_OUTSTANDING_DATE = ft.CREDIT_OUTSTANDING_DATE
    AND foc.CREDIT_ISSUED_MAX_DATE = ft.CREDIT_ISSUED_MAX_DATE
    AND foc.VIP_COHORT_MAX_DATE = ft.VIP_COHORT_MAX_DATE
    AND foc.MEMBER_STATUS_CLASSIFICATION_DATE = ft.MEMBER_STATUS_CLASSIFICATION_DATE
    AND foc.STORE_BRAND = ft.STORE_BRAND
    AND foc.STORE_COUNTRY = ft.STORE_COUNTRY
    AND foc.STORE_REGION = ft.STORE_REGION
    AND foc.MEMBER_STATUS = ft.MEMBER_STATUS
    AND foc.FIRST_VIP_COHORT = ft.FIRST_VIP_COHORT
    AND foc.OUTSTANDING_COUNT_BUCKET = ft.OUTSTANDING_COUNT_BUCKET
    WHEN MATCHED THEN
        UPDATE SET foc.CUSTOMER_COUNT = ft.CUSTOMER_COUNT,
                   foc.OUTSTANDING_AMOUNT = ft.OUTSTANDING_AMOUNT,
                   foc.OUTSTANDING_COUNT = ft.OUTSTANDING_COUNT
    WHEN NOT MATCHED THEN
        INSERT (CREDIT_OUTSTANDING_DATE,
                CREDIT_ISSUED_MAX_DATE,
                VIP_COHORT_MAX_DATE,
                MEMBER_STATUS_CLASSIFICATION_DATE,
                STORE_BRAND,
                STORE_COUNTRY,
                STORE_REGION,
                MEMBER_STATUS,
                FIRST_VIP_COHORT,
                OUTSTANDING_COUNT_BUCKET,
                CUSTOMER_COUNT,
                OUTSTANDING_AMOUNT,
                OUTSTANDING_COUNT)
        VALUES (ft.CREDIT_OUTSTANDING_DATE,
                ft.CREDIT_ISSUED_MAX_DATE,
                ft.VIP_COHORT_MAX_DATE,
                ft.MEMBER_STATUS_CLASSIFICATION_DATE,
                ft.STORE_BRAND,
                ft.STORE_COUNTRY,
                ft.STORE_REGION,
                ft.MEMBER_STATUS,
                ft.FIRST_VIP_COHORT,
                ft.OUTSTANDING_COUNT_BUCKET,
                ft.CUSTOMER_COUNT,
                ft.OUTSTANDING_AMOUNT,
                ft.OUTSTANDING_COUNT);




