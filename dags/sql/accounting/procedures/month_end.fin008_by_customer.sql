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
                  FROM month_end.dates_for_fin008 WHERE num_for_recursion=1) ) AS membership_month_date,
       IFF(v2.membership_state = 'VIP', 'VIP', 'Cancelled') AS member_status,
       1                                                    AS date_flag,
       DATE_TRUNC(MONTH, v.activation_local_datetime)::DATE AS first_vip_cohort
FROM edw_prod.data_model.fact_activation v
         JOIN edw_prod.data_model.dim_store st ON st.store_id = v.store_id
         JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = v.customer_id
         LEFT JOIN edw_prod.data_model.fact_membership_event v2 ON v2.customer_id = dc.customer_id
    AND v2.event_start_local_datetime::DATE < DATEADD(DAY, 1, (SELECT TO_VARCHAR(member_status_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=1) )
    AND v2.event_end_local_datetime::DATE >= DATEADD(DAY, 1, (SELECT TO_VARCHAR(member_status_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=1) )
WHERE v.activation_local_datetime::DATE <= (SELECT TO_VARCHAR(vip_cohort_max_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=1)
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
                  FROM month_end.dates_for_fin008 WHERE num_for_recursion=2) ) AS membership_month_date,
       IFF(v2.membership_state = 'VIP', 'VIP', 'Cancelled') AS member_status,
       2                                                    AS Date_flag,
       DATE_TRUNC(MONTH, v.activation_local_datetime)::DATE AS first_vip_cohort
FROM edw_prod.data_model.fact_activation v
         JOIN edw_prod.data_model.dim_store st ON st.store_id = v.store_id
         JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = v.customer_id
         LEFT JOIN edw_prod.data_model.fact_membership_event v2 ON v2.customer_id = dc.customer_id
    AND v2.event_start_local_datetime::DATE < DATEADD(DAY, 1, (SELECT TO_VARCHAR(member_status_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=2) )
    AND v2.event_end_local_datetime::DATE >= DATEADD(DAY, 1, (SELECT TO_VARCHAR(member_status_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=2) )
WHERE v.activation_local_datetime::DATE <= (SELECT TO_VARCHAR(vip_cohort_max_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=2)
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
                  FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))                                   AS membership_month_date,
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
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))
    AND v2.event_end_local_datetime >= DATEADD(DAY, 1, (SELECT TO_VARCHAR(member_status_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))
WHERE ((dcf2.credit_reason = 'Membership Credit' AND dcf2.credit_type = 'Fixed Credit')
    OR (dcf2.credit_type = 'Token' AND dcf2.credit_reason = 'Token Billing'))
  AND vc.customer_id IS NULL
  AND dcf2.credit_issued_hq_datetime::DATE <= (SELECT TO_VARCHAR(vip_cohort_max_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=1)
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
                            FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))                         AS membership_month_date,
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
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))
    AND v2.event_end_local_datetime >= DATEADD(DAY, 1, (SELECT TO_VARCHAR(member_status_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))
WHERE ((dcf2.credit_reason = 'Membership Credit' AND dcf2.credit_type = 'Fixed Credit')
    OR (dcf2.credit_type = 'Token' AND dcf2.credit_reason = 'Token Billing'))
  AND vc.customer_id IS NULL
  AND dcf2.credit_issued_hq_datetime::DATE <= (SELECT vip_cohort_max_date
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=2)
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
                  FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))           AS credit_activity_month_date_max,
       DATE_TRUNC(MONTH, (SELECT credit_issued_max_date
                  FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))           AS credit_issuance_month_date_max,
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
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=1)
  AND credit_activity_local_datetime <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=1)
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
                  FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))           AS credit_activity_month_date_max,
       DATE_TRUNC(MONTH, (SELECT credit_issued_max_date
                  FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))           AS credit_issuance_month_date_max,
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
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=2)
  AND credit_activity_local_datetime <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=2)
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
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=1) AND date_flag=1, activity_amount, 0))
           - SUM(IFF(activity_type = 'Redeemed' AND activity_month <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=1) AND date_flag=1, activity_amount, 0))
           - SUM(IFF(activity_type = 'Cancelled' AND activity_month <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=1) AND date_flag=1, activity_amount, 0))
           - SUM(IFF(activity_type = 'Expired' AND activity_month <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=1) AND date_flag=1, activity_amount,
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
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=2) AND date_flag=2, activity_amount, 0))
           - SUM(IFF(activity_type = 'Redeemed' AND activity_month <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=2) AND date_flag=2, activity_amount, 0))
           - SUM(IFF(activity_type = 'Cancelled' AND activity_month <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=2) AND date_flag=2, activity_amount, 0))
           - SUM(IFF(activity_type = 'Expired' AND activity_month <= (SELECT TO_VARCHAR(credit_active_as_of_date)
                                                                FROM month_end.dates_for_fin008 WHERE num_for_recursion=2) AND date_flag=2, activity_amount,
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
       1 AS                                                            date_flag,
       SUM(CASE WHEN date_flag = 1 THEN outstanding_amount ELSE 0 END) outstanding_amount,
       SUM(CASE WHEN date_flag = 1 THEN outstanding_count ELSE 0 END)  outstanding_count
FROM _by_customer_by_price
WHERE date_flag = 1
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
       2 AS                                                            date_flag,
       SUM(CASE WHEN date_flag = 2 THEN outstanding_amount ELSE 0 END) outstanding_amount,
       SUM(CASE WHEN date_flag = 2 THEN outstanding_count ELSE 0 END)  outstanding_count
FROM _by_customer_by_price
WHERE date_flag = 2
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
    AND bc.date_flag = 1
WHERE bc.customer_id IS NULL
  AND a.date_flag = 1
UNION ALL
SELECT a.store_brand,
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
    AND bc.date_flag = 2
WHERE bc.customer_id IS NULL
  AND a.date_flag = 2;


CREATE OR REPLACE TRANSIENT TABLE month_end.fin008_by_customer AS
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
      WHERE date_flag = 1
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
          AND bc.date_flag = 1
      WHERE bc.customer_id IS NULL
        AND a.date_flag = 1)
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
      WHERE date_flag = 2
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
          AND bc.date_flag = 2
      WHERE bc.customer_id IS NULL
        AND a.date_flag = 2)
WHERE member_status IS NOT NULL;

MERGE INTO month_end.fin008_by_customer_snapshot AS target
    USING (SELECT *,
                  CURRENT_TIMESTAMP AS insert_datetime
           FROM month_end.fin008_by_customer) AS source
    ON target.store_brand = source.store_brand
        AND target.store_country = source.store_country
        AND target.store_region = source.store_region
        AND target.member_status = source.member_status
        AND target.customer_id = source.customer_id
        AND target.first_vip_cohort = source.first_vip_cohort
        AND target.membership_month_date = source.membership_month_date
        AND target.credit_activity_month_date_max = source.credit_activity_month_date_max
        AND target.credit_issuance_month_date_max = source.credit_issuance_month_date_max
        AND target.date_flag = source.date_flag
    WHEN MATCHED THEN
        UPDATE SET
            target.outstanding_amount = source.outstanding_amount,
            target.outstanding_count = source.outstanding_count,
            target.insert_datetime = source.insert_datetime
    WHEN NOT MATCHED THEN
        INSERT (store_brand, store_country, store_region, member_status, customer_id,
                first_vip_cohort, membership_month_date, credit_activity_month_date_max,
                credit_issuance_month_date_max, date_flag, outstanding_amount,
                outstanding_count, insert_datetime)
            VALUES (source.store_brand, source.store_country, source.store_region,
                    source.member_status, source.customer_id, source.first_vip_cohort,
                    source.membership_month_date, source.credit_activity_month_date_max,
                    source.credit_issuance_month_date_max, source.date_flag,
                    source.outstanding_amount, source.outstanding_count,
                    source.insert_datetime);

DELETE
FROM month_end.fin008_by_customer_snapshot
WHERE datediff(MONTH,credit_activity_month_date_max, CURRENT_DATE) > 12;
