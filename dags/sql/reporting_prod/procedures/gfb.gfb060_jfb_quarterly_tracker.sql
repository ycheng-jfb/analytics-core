CREATE OR REPLACE TEMPORARY TABLE _survey_data_filters AS
SELECT CASE
           WHEN UPPER(s.survey_title) LIKE '%JUSTFAB%'
               THEN 'JUSTFAB'
           WHEN UPPER(s.survey_title) LIKE '%SHOEDAZZLE%'
               THEN 'SHOEDAZZLE'
           WHEN UPPER(s.survey_title) LIKE '%FABKIDS%'
               THEN 'FABKIDS'
           END                                                   AS business_unit,
       CASE
           WHEN UPPER(s.survey_title) LIKE '%NA%' THEN 'NA'
           WHEN UPPER(s.survey_title) LIKE '%EU%' THEN 'EU'
           ELSE ds.store_region END                              AS region,
       s.survey_id,
       s.respondent_id,
       s.customer_id,
       CAST(SPLIT_PART(RIGHT(s.survey_title, 8), ' ', 2) AS INT) AS year_int,
       CASE
           WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%jan%'
               THEN 1
           WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%feb%'
               THEN 2
           WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%mar%'
               THEN 3
           WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%apr%'
               THEN 4
           WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%may%'
               THEN 5
           WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%jun%'
               THEN 6
           WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%jul%'
               THEN 7
           WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%aug%'
               THEN 8
           WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%sep%'
               THEN 9
           WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%oct%'
               THEN 10
           WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%nov%'
               THEN 11
           WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%dec%'
               THEN 12
           END                                                   AS month_int,
       DATE_FROM_PARTS(year_int, month_int, 1)                   AS survey_date,
       MAX(CASE
               WHEN business_unit = 'FABKIDS' THEN 'US'
               WHEN LOWER(s.l1_question_id) = '94' THEN s.response
               ELSE NULL END)                                    AS country,
       MAX(CASE
               WHEN business_unit = 'FABKIDS' AND LOWER(s.l1_question_id) = '101' AND s.response = '1'
                   THEN s.l3_question_label
               WHEN LOWER(s.l1_question_id) = '193' AND s.response = '1' THEN s.l3_question_label
               ELSE NULL END)                                    AS jf_nps,
       MAX(CASE
               WHEN business_unit = 'FABKIDS' AND LOWER(s.l1_question_id) = '118' THEN s.response
               WHEN LOWER(s.l1_question_id) = '181' THEN s.response
               ELSE NULL END)                                    AS age,
       MAX(CASE
               WHEN business_unit = 'SHOEDAZZLE' AND s.date_submitted >= '2022-12-16' AND
                    LOWER(s.l1_question_id) = '285' THEN s.response
               WHEN LOWER(s.l1_question_id) = '2' AND s.response = '1' THEN s.l3_question_label
               ELSE NULL END)                                    AS age_bucket,
       MAX(CASE
               WHEN business_unit = 'FABKIDS' AND LOWER(s.l1_question_id) = '89' THEN s.response
               WHEN LOWER(s.l1_question_id) = '236' THEN s.response
               ELSE NULL END)                                    AS income_na,
       MAX(CASE
               WHEN LOWER(s.l1_question_id) = '238' THEN s.response
               ELSE NULL END)                                    AS income_uk,
       MAX(CASE
               WHEN business_unit != 'FABKIDS' AND LOWER(s.l1_question_id) = '89' THEN s.response
               ELSE NULL END)                                    AS income_es_de_fr,
       MAX(CASE
               WHEN business_unit = 'FABKIDS' AND LOWER(s.l1_question_id) = '87' THEN s.response
               WHEN LOWER(s.l1_question_id) = '231' THEN s.response
               ELSE NULL END)                                    AS ethnicity_us,
       MAX(CASE
               WHEN LOWER(s.l1_question_id) = '224' THEN s.response
               ELSE NULL END)                                    AS apparel_size,
       MAX(CASE
               WHEN LOWER(s.l1_question_id) = '4' THEN s.response
               ELSE NULL END)                                    AS hdyh,
       MAX(CASE
               WHEN business_unit = 'FABKIDS' AND LOWER(s.l1_question_id) = '122' THEN s.response
               WHEN LOWER(s.l1_question_id) = '179' THEN s.response
               ELSE NULL END)                                    AS membership_price,
       LISTAGG((CASE
                    WHEN business_unit = 'FABKIDS' AND LOWER(s.l1_question_id) = '124' AND s.response = '1'
                        THEN s.l3_question_label
                    WHEN LOWER(s.l1_question_id) = '189' AND s.response = '1' THEN s.l3_question_label
                    ELSE NULL END),
               ' ')                                              AS vip_type, -- there are cases with multiple selections
       MAX(CASE
               WHEN LOWER(l1_question_id) LIKE '%status%' THEN response
               ELSE NULL END)                                    AS status,
       MAX(CASE
               WHEN business_unit = 'FABKIDS' AND LOWER(s.l1_question_id) = '62' AND s.l3_question_id = '10510'
                   THEN TRY_TO_DECIMAL(REPLACE(s.response, '%', '')) / 100
               WHEN business_unit = 'SHOEDAZZLE' AND LOWER(s.l1_question_id) = '271' AND s.l3_question_id = '12139'
                   THEN TRY_TO_DECIMAL(REPLACE(s.response, '%', '')) / 100
               WHEN LOWER(s.l1_question_id) = '271' AND s.l3_question_id = '12138'
                   THEN TRY_TO_DECIMAL(REPLACE(s.response, '%', '')) / 100
               ELSE NULL END)                                    AS justfab_wallet_share_na,
       MAX(CASE
               WHEN LOWER(s.l1_question_id) = '273' AND s.l3_question_id = '12148'
                   THEN TRY_TO_DECIMAL(REPLACE(s.response, '%', '')) / 100
               ELSE NULL END)                                    AS justfab_wallet_share_eu
FROM lake.alchemer.survey_responses s
         LEFT JOIN edw_prod.data_model_jfb.dim_customer dc
                   ON dc.customer_id = s.customer_id
         LEFT JOIN edw_prod.data_model_jfb.dim_store ds
                   ON ds.store_id = dc.store_id
WHERE business_unit IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS')
  AND s.meta_create_datetime >= '2022-03-01'
  AND s.survey_title ILIKE '%tracker%'
  AND s.survey_title NOT ILIKE '%data%'
  AND s.survey_title NOT ILIKE '%test%'
  AND s.survey_title NOT ILIKE '%copy%'
  AND s.customer_id NOT ILIKE '%contact%'
GROUP BY CASE
             WHEN UPPER(s.survey_title) LIKE '%JUSTFAB%'
                 THEN 'JUSTFAB'
             WHEN UPPER(s.survey_title) LIKE '%SHOEDAZZLE%'
                 THEN 'SHOEDAZZLE'
             WHEN UPPER(s.survey_title) LIKE '%FABKIDS%'
                 THEN 'FABKIDS'
             END,
         CASE
             WHEN UPPER(s.survey_title) LIKE '%NA%' THEN 'NA'
             WHEN UPPER(s.survey_title) LIKE '%EU%' THEN 'EU'
             ELSE ds.store_region END,
         s.survey_id,
         s.respondent_id,
         s.customer_id,
         CAST(SPLIT_PART(RIGHT(s.survey_title, 8), ' ', 2) AS INT),
         CASE
             WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%jan%'
                 THEN 1
             WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%feb%'
                 THEN 2
             WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%mar%'
                 THEN 3
             WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%apr%'
                 THEN 4
             WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%may%'
                 THEN 5
             WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%jun%'
                 THEN 6
             WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%jul%'
                 THEN 7
             WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%aug%'
                 THEN 8
             WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%sep%'
                 THEN 9
             WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%oct%'
                 THEN 10
             WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%nov%'
                 THEN 11
             WHEN LOWER(SPLIT_PART(TRIM(s.survey_title), ' ', REGEXP_COUNT(TRIM(s.survey_title), ' '))) LIKE '%dec%'
                 THEN 12
             END,
         DATE_FROM_PARTS(year_int, month_int, 1);

CREATE OR REPLACE TEMPORARY TABLE _ltv AS
SELECT a.customer_id,
       a.survey_month,
       SUM(clvm.cash_gross_profit)                     AS cash_gross_profit,
       SUM(clvm.product_gross_profit)                  AS product_gross_profit,
       SUM(clvm.product_order_count)                   AS product_order_count,
       SUM(clvm.product_order_unit_count)              AS product_order_unit_count,
       SUM(clvm.product_gross_revenue_excl_shipping +
           clvm.product_order_shipping_revenue_amount) AS product_gross_revenue_incl_shipping,
       SUM(clvm.product_gross_revenue_excl_shipping)   AS product_gross_revenue_excl_shipping
FROM edw_prod.analytics_base.customer_lifetime_value_monthly clvm
         JOIN
     (SELECT DISTINCT sd.customer_id
                    , DATE_TRUNC(MONTH, sd.survey_date) AS survey_month
      FROM _survey_data_filters sd) a ON a.customer_id = clvm.meta_original_customer_id
         AND a.survey_month <= clvm.month_date
GROUP BY a.customer_id,
         a.survey_month;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb060_jfb_quarterly_tracker AS
SELECT s.survey_id,
       s.survey_title,
       s.respondent_id,
       s.customer_id,
       s.question_type,
       s.question_id,
       s.question_label,
       s.l1_question_id,
       s.l1_question_label,
       s.l2_question_id,
       s.l2_question_label,
       s.l3_question_id,
       s.l3_question_label,
       s.response,
       s.response_text,
       s.response_numeric,
       s.response_alt,
       s.updated_at,
       sd.business_unit,
       CASE
           WHEN sd.country IN ('US', 'CA') THEN COALESCE(sd.region, 'NA')
           ELSE COALESCE(sd.region, 'EU') END                           AS region,
       sd.country,
       sd.survey_date,
       sd.jf_nps,
       sd.age,
       sd.age_bucket,
       COALESCE(sd.income_na, sd.income_uk, sd.income_es_de_fr)         AS income,
       sd.ethnicity_us,
       sd.apparel_size,
       sd.hdyh                                                          AS how_did_you_hear,
       sd.membership_price,
       CASE
           WHEN sd.vip_type LIKE '%Downgrade%' THEN 'Downgrade'
           ELSE 'VIP' END                                               AS vip_type,
       COALESCE(sd.justfab_wallet_share_eu, sd.justfab_wallet_share_na) AS wallet_share,
       dv.current_membership_status,
       dv.registration_date,
       DATE_TRUNC(MONTH, dv.registration_date)                          AS registration_month,
       dv.first_activating_date,
       dv.first_activating_cohort,
       dv.recent_activating_date,
       dv.recent_activating_cohort,
       dv.first_cancellation_date,
       dv.recent_vip_cancellation_date,
       dv.activating_promo_names,
       dv.days_between_registration_activation,
       l.cash_gross_profit,
       l.product_gross_profit,
       l.product_order_count,
       l.product_order_unit_count,
       l.product_gross_revenue_incl_shipping,
       l.product_gross_revenue_excl_shipping,
       dv.first_activating_product_revenue,
       CASE
           WHEN sd.survey_date = '2022-06-01' AND sd.business_unit = 'SHOEDAZZLE' THEN NULL
           ELSE DATEDIFF(MONTH, dv.first_activating_cohort, sd.survey_date) + 1
           END                                                          AS tenure,
       CASE
           WHEN dc.first_mobile_app_session_id IS NOT NULL THEN 'Yes'
           ELSE 'No' END                                                AS app_use_flag
FROM lake.alchemer.survey_responses s
         JOIN _survey_data_filters sd
              ON sd.survey_id = s.survey_id
                  AND sd.respondent_id = s.respondent_id
         LEFT JOIN gfb.gfb_dim_vip dv
                   ON dv.customer_id = sd.customer_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer dc
                   ON dc.customer_id = sd.customer_id
         LEFT JOIN _ltv l
                   ON l.customer_id = sd.customer_id
                       AND l.survey_month = DATE_TRUNC(MONTH, sd.survey_date)
WHERE sd.status = 'Complete'
  AND sd.vip_type LIKE ANY ('%VIP%', '%Downgrade%')
  AND sd.customer_id IS NOT NULL
  AND DATE_TRUNC(QUARTER, sd.survey_date) <= DATE_TRUNC(QUARTER, CURRENT_DATE());
