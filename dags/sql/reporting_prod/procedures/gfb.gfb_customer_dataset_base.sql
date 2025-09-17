SET low_watermark_ltz = (
    SELECT MAX(a.month_date)
    FROM reporting_prod.gfb.gfb_customer_dataset_base a
);


CREATE OR REPLACE TEMP TABLE _period_month AS
SELECT DISTINCT dd.month_date
FROM edw_prod.data_model_jfb.dim_date dd
WHERE month_date >= $low_watermark_ltz
  AND month_date < DATE_TRUNC('MONTH', CURRENT_DATE())
ORDER BY month_date;


CREATE OR REPLACE TEMP TABLE _vip_customer_base AS
SELECT DISTINCT a.customer_id
              , b.month_date
FROM edw_prod.data_model_jfb.fact_activation a
         JOIN _period_month b
              ON b.month_date BETWEEN DATE_TRUNC(MONTH, a.activation_local_datetime) AND a.cancellation_local_datetime
         JOIN reporting_prod.gfb.vw_store ds
              ON ds.store_id = a.store_id;


CREATE OR REPLACE TEMPORARY TABLE _activations_and_cancels AS
SELECT cb.month_date
     , cb.customer_id
     , fa.session_id
     , dsc.store_id
     , ds.store_id                                                                                            AS activating_store_id
     , dsc.store_region                                                                                       AS region
     , (CASE
            WHEN dc.specialty_country_code = 'GB' THEN 'UK'
            WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
            ELSE dsc.store_country END)                                                                        AS country
     , dsc.store_type
     , ds.store_type                                                                                          AS activating_store_type
     , IFF(UPPER(dc.gender) = 'M', 'M', 'F')                                                                  AS customer_gender
     , IFF(dsc.store_brand = 'Yitty', TRUE, FALSE)                                                            AS is_yitty_customer
     , dc.birth_year                                                                                          AS customer_birth_year
     , (CASE
            WHEN dc.birth_year BETWEEN 1930 AND 2005 THEN 2021 - birth_year
            WHEN dc.birth_year = -1 THEN -1
            ELSE 999 END)                                                                                     AS age
     , UPPER(dc.default_state_province)                                                                       AS customer_state_province
     , (CASE
            WHEN dc.mobile_app_cohort_month_date IS NOT NULL
                THEN 1
            ELSE 0 END)                                                                                       AS mobile_app_downloaded_flag
     , dc.how_did_you_hear
     , fa.activation_local_datetime                                                                           AS activation_local_datetime
     , CAST(fa.activation_local_datetime AS DATE)                                                             AS activation_local_date
     , DATEDIFF('days', fr.registration_local_datetime,
                fa.activation_local_datetime)                                                                 AS lead_age_at_activation_in_days
     , NVL(fa.activation_sequence_number, 1) - 1                                                              AS reactivation_count
     , IFF(CAST(fa.cancellation_local_datetime AS DATE) = '9999-12-31', NULL,
           fa.cancellation_local_datetime)                                                                    AS cancel_local_datetime
     , CAST(cancel_local_datetime AS DATE)                                                                    AS cancel_local_date
     , fa.cancel_type
     , DATEDIFF(DAY, activation_local_date, cancel_local_date)                                                AS days_to_cancel
     , IFF(cancel_local_datetime IS NOT NULL, 1, 0)                                                           AS churned_flag
     , fa.order_id
FROM _vip_customer_base cb
         JOIN edw_prod.data_model_jfb.fact_activation fa
              ON fa.customer_id = cb.customer_id
                  AND cb.month_date BETWEEN fa.vip_cohort_month_date AND fa.cancellation_local_datetime
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = fa.customer_id
         JOIN edw_prod.data_model_jfb.dim_store ds
              ON ds.store_id = fa.sub_store_id
         JOIN edw_prod.data_model_jfb.dim_store dsc
              ON dsc.store_id = dc.store_id
         JOIN edw_prod.data_model_jfb.fact_registration fr
              ON fr.customer_id = dc.customer_id
    QUALIFY
            ROW_NUMBER() OVER (PARTITION BY cb.customer_id, cb.month_date ORDER BY fa.activation_local_datetime DESC) =
            1;


CREATE OR REPLACE TEMP TABLE _unique_customer_base AS
SELECT DISTINCT a.customer_id
FROM _vip_customer_base a
WHERE a.customer_id IS NOT NULL
  AND a.customer_id <> -1;


CREATE OR REPLACE TEMP TABLE _influencer_hdyh AS
SELECT DISTINCT a.hdyh
              , b.influencer_classification
FROM _unique_customer_base ucb
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON ucb.customer_id = dc.customer_id
         JOIN lake_view.sharepoint.med_selector_influencer_paid_spend_mappings a
              ON dc.how_did_you_hear = a.hdyh
         JOIN lake_view.sharepoint.med_influencer_master_organic_spend_dimensions_fl b
              ON LOWER(a.full_name) = LOWER(b.creator_name);


CREATE OR REPLACE TEMPORARY TABLE _quiz_size AS
SELECT DISTINCT cb.customer_id
              , dv.apparel_size
              , dv.apparel_core_size_flag
              , dv.footwear_size
              , dv.footwear_core_size_flag
FROM _unique_customer_base cb
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON cb.customer_id = dc.customer_id
         JOIN reporting_prod.gfb.gfb_dim_vip dv
              ON dv.customer_id = dc.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _quiz_heel_height AS
SELECT DISTINCT qa.customer_id
              , LISTAGG(qa.answer_text, ', ') AS heel_height
FROM reporting_prod.gfb.gfb_quiz_activity qa
WHERE LOWER(qa.question_text) LIKE '%heel height%'
GROUP BY qa.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _quiz_shoe_color AS
SELECT DISTINCT qa.customer_id
              , LISTAGG(qa.answer_text, ', ') AS shoe_color
FROM reporting_prod.gfb.gfb_quiz_activity qa
WHERE LOWER(qa.question_text) LIKE '%your shoe color preference%'
GROUP BY qa.customer_id;


CREATE OR REPLACE TEMP TABLE _activating_order_details AS
SELECT DISTINCT ac.customer_id
              , ac.month_date
              , ac.activating_store_type
              , ac.region
              , ac.order_id                  AS activating_order_id
              , fo.order_date                AS order_date
              , fo.creditcard_type           AS activating_payment_method
              , COALESCE(dp_1.offer, 'None') AS offer
FROM _activations_and_cancels ac
         JOIN reporting_prod.gfb.gfb_order_line_data_set_place_date fo
              ON ac.order_id = fo.order_id
                  AND fo.order_classification = 'product order'
         LEFT JOIN reporting_prod.gfb.dim_promo dp_1
                   ON dp_1.promo_id = fo.promo_id_1
    QUALIFY
            RANK() OVER (PARTITION BY ac.customer_id, ac.month_date ORDER BY order_date, ac.order_id) = 1

UNION

SELECT DISTINCT ac.customer_id
              , ac.month_date
              , ac.activating_store_type
              , ac.region
              , ac.order_id                  AS activating_order_id
              , fo.order_date                AS order_date
              , fo.creditcard_type           AS activating_payment_method
              , COALESCE(dp_2.offer, 'None') AS offer
FROM _activations_and_cancels ac
         JOIN reporting_prod.gfb.gfb_order_line_data_set_place_date fo
              ON ac.order_id = fo.order_id
                  AND fo.order_classification = 'product order'
         LEFT JOIN reporting_prod.gfb.dim_promo dp_2
                   ON dp_2.promo_id = fo.promo_id_2
    QUALIFY
            RANK() OVER (PARTITION BY ac.customer_id, ac.month_date ORDER BY order_date, ac.order_id) = 1;


CREATE OR REPLACE TEMPORARY TABLE _activating_order_details_final AS
SELECT aod.customer_id
     , aod.month_date
     , aod.activating_store_type
     , aod.region
     , aod.activating_order_id
     , aod.order_date
     , aod.activating_payment_method

     , REPLACE(REPLACE(LISTAGG(aod.offer, ', '), ', None', ''), 'None, ', '') AS activating_promo_offer
FROM _activating_order_details aod
GROUP BY aod.customer_id
       , aod.month_date
       , aod.activating_store_type
       , aod.region
       , aod.activating_order_id
       , aod.order_date
       , aod.activating_payment_method;


DELETE
FROM reporting_prod.gfb.gfb_customer_dataset_base a
WHERE a.month_date >= $low_watermark_ltz;


INSERT INTO reporting_prod.gfb.gfb_customer_dataset_base
SELECT ac.customer_id,
       ac.month_date,
       ac.activation_local_date,
       ac.activation_local_datetime,
       ac.store_id,
       ac.region,
       ac.country,
       ac.store_type,
       ac.customer_gender,
       ac.is_yitty_customer,
       ac.customer_birth_year,
       ac.age,
       ac.cancel_local_date,
       ac.cancel_local_datetime,
       ac.cancel_type,
       ac.days_to_cancel,
       ac.churned_flag,
       ac.customer_state_province,
       ac.mobile_app_downloaded_flag,
       ac.how_did_you_hear,
       CASE
           WHEN ih.hdyh IS NOT NULL THEN ih.influencer_classification
           WHEN LOWER(ac.how_did_you_hear) ILIKE ANY
                ('%facebook%', '%instagram%', '%pinterest%', '%snapchat%', '%tik%', '%twit%', '%you%')
               THEN 'Social Media'
           WHEN LOWER(ac.how_did_you_hear) ILIKE ANY
                ('acquired style', 'gym/health club', 'jetblue', 'online video', 'other', 'shape', 'unknown', 'online',
                 'youtube') THEN 'Misc'
           WHEN LOWER(ac.how_did_you_hear) ILIKE ANY ('%friend%', '%amigo%') THEN 'Friend'
           WHEN LOWER(ac.how_did_you_hear) ILIKE ANY
                ('amazon', 'banner ad', '%catalog%', 'in-club ad', '%magazine%', 'pandora', '%radio%', '%reddit%',
                 'search engine ad', 'spotify', 'tv/streaming ad', 'television', '%tv%', 'pub sur internet',
                 'online ad') THEN 'Other Paid Media'
           WHEN LOWER(ac.how_did_you_hear) ILIKE 'blogger/influencer' THEN 'Blogger/Influencer'
           WHEN LOWER(ac.how_did_you_hear) ILIKE ANY ('google', 'search engine') THEN 'Search Engine'
           WHEN LOWER(ac.how_did_you_hear) ILIKE '%fabletics%' THEN 'Fabletics'
           WHEN LOWER(ac.how_did_you_hear) ILIKE '%justfab%' THEN 'JustFab'
           WHEN LOWER(ac.how_did_you_hear) ILIKE '%shoedazzle%' THEN 'ShoeDazzle'
           WHEN LOWER(ac.how_did_you_hear) ILIKE '%fabkids%' THEN 'FabKids'
           WHEN LOWER(ac.how_did_you_hear) ILIKE '%event%' THEN 'Event'
           ELSE NULL
           END AS how_did_you_hear_condensed,
       ac.lead_age_at_activation_in_days,
       CASE
           WHEN ac.lead_age_at_activation_in_days <= 7 THEN TO_CHAR(ac.lead_age_at_activation_in_days)
           WHEN ac.lead_age_at_activation_in_days BETWEEN 8 AND 14 THEN '8-14 (2 weeks)'
           WHEN ac.lead_age_at_activation_in_days BETWEEN 15 AND 21 THEN '15-21 (3 weeks)'
           WHEN ac.lead_age_at_activation_in_days BETWEEN 22 AND 28 THEN '22-28 (4 weeks)'
           WHEN ac.lead_age_at_activation_in_days BETWEEN 29 AND 35 THEN '29-35 (5 weeks)'
           WHEN ac.lead_age_at_activation_in_days BETWEEN 36 AND 42 THEN '36-42 (6 weeks)'
           WHEN ac.lead_age_at_activation_in_days BETWEEN 43 AND 49 THEN '43-49 (7 weeks)'
           WHEN ac.lead_age_at_activation_in_days BETWEEN 50 AND 56 THEN '50-56 (8 weeks)'
           WHEN ac.lead_age_at_activation_in_days > 56 THEN '56+ (8 weeks +)'
           END AS lead_age_at_activation_buckets,
       ac.reactivation_count,
       qs.apparel_size,
       qs.apparel_core_size_flag,
       qs.footwear_size,
       qs.footwear_core_size_flag,
       odf.activating_order_id,
       odf.activating_payment_method,
       odf.activating_promo_offer,
       odf.activating_store_type,
       qhh.heel_height,
       qsc.shoe_color
FROM _activations_and_cancels ac
         LEFT JOIN _quiz_size qs
                   ON ac.customer_id = qs.customer_id
         LEFT JOIN _activating_order_details_final odf
                   ON odf.customer_id = ac.customer_id
                       AND odf.month_date = ac.month_date
         LEFT JOIN _influencer_hdyh ih
                   ON ih.hdyh = ac.how_did_you_hear
         LEFT JOIN _quiz_heel_height qhh
                   ON qhh.customer_id = ac.customer_id
         LEFT JOIN _quiz_shoe_color qsc
                   ON qsc.customer_id = ac.customer_id
ORDER BY ac.customer_id,
         ac.month_date;
