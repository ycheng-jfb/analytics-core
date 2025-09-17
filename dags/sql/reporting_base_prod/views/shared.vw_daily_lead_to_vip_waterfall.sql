CREATE OR REPLACE VIEW shared.vw_daily_lead_to_vip_waterfall AS
SELECT CURRENT_TIMESTAMP()                         datetime_refreshed,
       fr.is_secondary_registration,
       fa.is_retail_vip,
       fr.is_retail_registration,
       CASE
           WHEN st.store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END
                                                AS lead_store_brand,
       CASE
           WHEN st1.store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
           WHEN st1.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st1.store_brand END             AS vip_store_brand,
       st.store_region,
       st.store_country,
       fr.registration_local_datetime::DATE     AS registration_date,
       fa.activation_local_datetime::DATE       AS activation_date,
       CASE
           WHEN datediff(DAY, dc.registration_local_datetime::DATE, fa.activation_local_datetime::DATE) + 1 >= 8
               THEN 'D8-30'
           ELSE 'D' || CAST((datediff(DAY, dc.registration_local_datetime::DATE, fa.activation_local_datetime::DATE) +
                             1) AS VARCHAR) END AS tenure_grouped,
       COUNT(DISTINCT fr.customer_id)           AS customer_count
FROM edw_prod.data_model.fact_registration fr
         LEFT JOIN edw_prod.data_model.fact_activation fa
                   ON fa.customer_id = fr.customer_id
                       AND DATEDIFF(DAY, fr.registration_local_datetime, fa.activation_local_datetime) >= 0
                       AND fa.activation_local_datetime::DATE < CURRENT_DATE()
         LEFT JOIN edw_prod.data_model.dim_store st1 ON st1.store_id = fa.store_id
         JOIN edw_prod.data_model.dim_store st ON st.store_id = fr.store_id
         JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = fr.customer_id
WHERE fr.registration_local_datetime::DATE < CURRENT_DATE()
GROUP BY datetime_refreshed,
       fr.is_secondary_registration,
       fa.is_retail_vip,
       fr.is_retail_registration,
       CASE
           WHEN st.store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END,
       CASE
           WHEN st1.store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
           WHEN st1.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st1.store_brand END,
       st.store_region,
       st.store_country,
       fr.registration_local_datetime::DATE,
       fa.activation_local_datetime::DATE,
       CASE
           WHEN datediff(DAY, dc.registration_local_datetime::DATE, fa.activation_local_datetime::DATE) + 1 >= 8
               THEN 'D8-30'
           ELSE 'D' || CAST((datediff(DAY, dc.registration_local_datetime::DATE, fa.activation_local_datetime::DATE) +
                             1) AS VARCHAR) END;
