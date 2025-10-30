CREATE OR REPLACE TEMPORARY TABLE _cancellations AS
SELECT DISTINCT fme.customer_id
              , FIRST_VALUE(CAST(fme.event_start_local_datetime AS DATE))
                            OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime ASC) AS first_cancellation_date
              , FIRST_VALUE(fme.membership_type_detail)
                            OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime ASC) AS first_cancellation_type
              , LAST_VALUE(CAST(fme.event_start_local_datetime AS DATE))
                           OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime ASC)  AS recent_vip_cancellation_date
              , LAST_VALUE(fme.membership_type_detail)
                           OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime ASC)  AS recent_cancellation_type
FROM edw_prod.data_model_jfb.fact_membership_event AS fme
         JOIN gfb.vw_store AS st
              ON st.store_id = fme.store_id
WHERE fme.membership_event_type = 'Cancellation';


CREATE OR REPLACE TEMPORARY TABLE _all_vips AS
SELECT DISTINCT st.store_id
              , UPPER(st.store_brand)                                                                       AS business_unit
              , UPPER(st.store_region)                                                                      AS region
              , (CASE
                     WHEN dc.specialty_country_code = 'GB' THEN 'UK'
                     WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
                     ELSE st.store_country END)                                                             AS country
              , dc.customer_id
              , fme.membership_event_type
              --first activating
              , FIRST_VALUE(CAST(fme.event_start_local_datetime AS DATE))
                            OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime ASC) AS first_activating_date
              , DATE_TRUNC(MONTH, first_activating_date)                                                    AS first_activating_cohort
              , FIRST_VALUE(fme.order_id)
                            OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime ASC) AS first_activating_order_id
              , FIRST_VALUE(fme.session_id)
                            OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime ASC) AS first_activating_session_id
              --recent activating
              , LAST_VALUE(CAST(fme.event_start_local_datetime AS DATE))
                           OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime ASC)  AS recent_activating_date
              , LAST_VALUE(fme.event_start_local_datetime)
                           OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime ASC)  AS recent_activating_datetime
              , DATE_TRUNC(MONTH, recent_activating_date)                                                   AS recent_activating_cohort
              , LAST_VALUE(fme.order_id)
                           OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime ASC)  AS recent_activating_order_id
              , LAST_VALUE(fme.session_id)
                           OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime ASC)  AS recent_activating_session_id
              --ltv
              , clv.cash_gross_profit
              --product order
              , clv.product_order_count
              , clv.product_gross_profit
              , clv.product_gross_revenue_excl_shipping
              , clv.product_order_unit_count
              , clv.product_gross_revenue_incl_shipping
              , clv.activating_gross_margin
              , clv.product_order_return_unit_count
              , COALESCE(clv.product_order_count, 0) -
                COALESCE(clv.activating_product_order_count, 0)                                             AS repeat_product_order_count
              --cancellation
              , c.first_cancellation_date
              , c.first_cancellation_type
              , c.recent_vip_cancellation_date
              , c.recent_cancellation_type
              , cme.membership_state                                                                        AS current_membership_status
              , clv.is_reactivated_vip
              , dc.how_did_you_hear                                                                         AS hdyh
              , (CASE
                     WHEN dc.birth_year = -1 OR dc.birth_month = -1 THEN NULL
                     ELSE DATE_FROM_PARTS(dc.birth_year, dc.birth_month, 1) END)                            AS birthday_month
              , DATEDIFF('year', birthday_month, CURRENT_DATE())                                            AS age
              , LOWER(dc.email)                                                                             AS email
              , UPPER(dc.default_state_province)                                                            AS default_state_province
              , LAST_VALUE(fme.membership_type_detail)
                           OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime ASC)  AS recent_activition_type
              , FIRST_VALUE(fme.membership_type_detail)
                            OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime ASC) AS first_activition_type
              , dc.membership_level
              ,dc.default_postal_code AS customer_postal_code
FROM edw_prod.data_model_jfb.fact_membership_event fme
         JOIN gfb.vw_store st
              ON st.store_id = fme.store_id
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = fme.customer_id
         LEFT JOIN
     (
         SELECT clv.meta_original_customer_id                          AS customer_id
              , MAX(clv.is_reactivated_vip)                            AS is_reactivated_vip
              , SUM(clv.cash_gross_profit)                             AS cash_gross_profit
              , SUM(clv.product_order_count)                           AS product_order_count
              , SUM(clv.product_gross_profit)                          AS product_gross_profit
              , SUM(clv.product_gross_revenue_excl_shipping)           AS product_gross_revenue_excl_shipping
              , SUM(clv.product_order_unit_count)                      AS product_order_unit_count
              , SUM(clv.product_gross_revenue_excl_shipping +
                    clv.product_order_shipping_revenue_amount)         AS product_gross_revenue_incl_shipping
              , SUM(clv.activating_product_gross_revenue - clv.activating_product_order_landed_product_cost_amount -
                    clv.activating_product_order_shipping_cost_amount) AS activating_gross_margin
              , SUM(clv.product_order_return_unit_count)               AS product_order_return_unit_count
              , SUM(clv.activating_product_order_count)                AS activating_product_order_count
         FROM edw_prod.analytics_base.customer_lifetime_value_ltd clv
         GROUP BY clv.meta_original_customer_id
     ) clv
     ON clv.customer_id = fme.customer_id
         JOIN edw_prod.data_model_jfb.fact_membership_event cme
              ON cme.customer_id = clv.customer_id
                  AND cme.is_current = 1
         LEFT JOIN _cancellations c
                   ON c.customer_id = fme.customer_id
WHERE fme.membership_event_type = 'Activation';


CREATE OR REPLACE TEMPORARY TABLE _activating_offer AS
SELECT a.customer_id
     , LISTAGG(a.activating_offers, ', ') AS activating_offers
FROM (
         SELECT DISTINCT av.customer_id
                       , pds.activating_offers
         FROM _all_vips av
                  JOIN gfb.gfb011_promo_data_set pds
                       ON pds.order_id = av.first_activating_order_id
     ) a
GROUP BY a.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _activating_order AS
SELECT DISTINCT a.customer_id
             , a.activating_payment_method
             , UPPER(a.city)                                                                           AS activating_city
             , (CASE
                    WHEN LOWER(a.state) = 'cal' THEN 'CA'
                    WHEN LOWER(a.state) = 'ohi' THEN 'OH'
                    WHEN LOWER(a.state) = 'flo' THEN 'FL'
                    WHEN LOWER(a.state) = 'ill' THEN 'IL'
                    WHEN LOWER(a.state) = 'ari' THEN 'AZ'
                    ELSE UPPER(a.state) END)                                                           AS activating_state
             , UPPER(a.country)                                                                        AS activating_country
             , UPPER(a.region)                                                                         AS activating_region
             , (CASE
                    WHEN a.total_qty_sold > 1 THEN 'multiple item activating'
                    ELSE 'single item activating' END)                                                 AS activating_item_count
             , a.activating_departments
             , (CASE
                    WHEN CONTAINS(a.activating_is_plus_size, 'Y') THEN 'plus size activating'
                    ELSE 'non pluss size activating' END)                                              AS activating_plus_size
             , (CASE
                    WHEN CONTAINS(a.activating_ww_wc, 'W') THEN 'ww/wc activating'
                    ELSE 'non ww/wc activating' END)                                                   AS activating_ww_wc
             , (CASE
                    WHEN CONTAINS(a.activating_comfort, 'Y') THEN 'Y'
                    ELSE 'N' END)                                                                      AS activating_comfort
             , os.type                                                                                 AS activating_order_shipping_option
             , del.date_delivered
             , DATEDIFF(DAY, a.order_date, del.date_delivered) + 1                                     AS days_of_delivery
             , a.gamer_activation_flag
             , a.total_product_revenue
FROM (
        SELECT av.customer_id
             , (CASE
                    WHEN olp.is_prepaid_creditcard = 1 THEN 'ppcc'
                    ELSE olp.creditcard_type END)    AS activating_payment_method
             , olp.billing_country                   AS country
             , olp.billing_state                     AS state
             , olp.billing_city                      AS city
             , olp.region
             , olp.order_id
             , olp.order_date
             , (CASE
                    WHEN gamer.order_id IS NOT NULL THEN 'Gamer Activation'
                    ELSE 'Non Gamer Activation' END) AS gamer_activation_flag

             , SUM(olp.total_qty_sold)               AS total_qty_sold
             , LISTAGG(mdp.department, ', ')         AS activating_departments
             , LISTAGG(mdp.is_plussize, ', ')        AS activating_is_plus_size
             , LISTAGG(mdp.ww_wc, ', ')              AS activating_ww_wc
             , LISTAGG(mdp.comfort_flag, ', ')       AS activating_comfort
             , SUM(olp.total_product_revenue)        AS total_product_revenue
        FROM _all_vips av
                 JOIN gfb.gfb_order_line_data_set_place_date olp
                      ON olp.order_id = av.first_activating_order_id
                 JOIN gfb.merch_dim_product mdp
                      ON mdp.business_unit = olp.business_unit
                          AND mdp.region = olp.region
                          AND mdp.country = olp.country
                          AND mdp.product_sku = olp.product_sku
                 LEFT JOIN
             (
                 SELECT o.order_id
                 FROM lake_jfb_view.ultra_merchant."ORDER" o
                          JOIN lake_jfb_view.ultra_merchant.order_classification oc
                               ON oc.order_id = o.order_id
                          JOIN lake_jfb_view.ultra_merchant.order_detail od
                               ON o.order_id = od.order_id
                                   AND od.name = 'gaming_prevention_log_id'
                          JOIN lake_jfb_view.ultra_merchant.gaming_prevention_log gl
                               ON TRY_TO_NUMBER(od.value) = gl.gaming_prevention_log_id
                                   AND gl.gaming_prevention_action = 'void'
                 WHERE order_type_id = 23
                   AND o.date_placed >= CAST('2019-06-01' AS DATE)
             ) gamer ON gamer.order_id = olp.order_id
        WHERE olp.order_classification = 'product order'
        GROUP BY av.customer_id
               , (CASE
                      WHEN olp.is_prepaid_creditcard = 1 THEN 'ppcc'
                      ELSE olp.creditcard_type END)
               , olp.billing_country
               , olp.billing_state
               , olp.billing_city
               , olp.region
               , olp.order_id
               , olp.order_date
               , (CASE
                      WHEN gamer.order_id IS NOT NULL THEN 'Gamer Activation'
                      ELSE 'Non Gamer Activation' END)
    ) a
        LEFT JOIN lake_jfb_view.ultra_merchant.order_shipping os
                  ON os.order_id = a.order_id
        LEFT JOIN
    (
        SELECT os.order_id
             , MAX(os.date_delivered) AS date_delivered
        FROM lake_jfb_view.ultra_merchant.order_shipment os
        WHERE os.delivered = 1
        GROUP BY os.order_id
    ) del ON del.order_id = a.order_id
QUALIFY RANK() OVER (PARTITION BY a.customer_id, a.order_id ORDER BY os.order_shipping_id ASC ) = 1;

CREATE OR REPLACE TEMPORARY TABLE _activating_gateway AS
SELECT DISTINCT av.customer_id
              , dg.gateway_name
              , dg.gateway_type
              , m.channel
              , ss.platform   AS activating_device
              , ss.utm_medium AS activating_medium
              , ss.utm_source AS activating_source
FROM _all_vips av
         JOIN reporting_base_prod.shared.session ss
              ON ss.meta_original_session_id = av.first_activating_session_id
                  AND ss.store_id = av.store_id
         JOIN reporting_base_prod.shared.dim_gateway dg
              ON dg.dm_gateway_id = ss.dm_gateway_id
                  AND dg.effective_start_datetime <= ss.session_local_datetime
                  AND dg.effective_end_datetime > ss.session_local_datetime
         JOIN reporting_base_prod.shared.media_source_channel_mapping AS m
              ON m.media_source_hash = ss.media_source_hash
         WHERE  ss.is_in_segment = TRUE;


CREATE OR REPLACE TEMPORARY TABLE _lead_registration AS
SELECT av.customer_id
    , CAST(fme.event_start_local_datetime AS DATE)                             AS registration_date
    , (CASE
           WHEN ss.is_quiz_registration_action = 1 THEN 'quiz registration'
           WHEN ss.is_skip_quiz_registration_action = 1 THEN 'skip quiz registration'
           WHEN ss.is_speedy_registration_action = 1 THEN 'speedy registration'
   END)                                                                        AS registration_type
   , DATEDIFF(DAY, CAST(fme.event_start_local_datetime AS DATE), av.first_activating_date) + 1 AS days_between_registration_activation
    , ss.utm_campaign                                                           AS registration_utm_campaign
    , ss.utm_medium                                                            AS registration_utm_medium
    , ss.utm_source                                                          AS registration_utm_source
FROM edw_prod.data_model_jfb.fact_membership_event fme
        JOIN _all_vips av
             ON av.customer_id = fme.customer_id
        LEFT JOIN reporting_base_prod.shared.session ss
                  ON ss.meta_original_session_id = fme.session_id
WHERE fme.membership_event_type = 'Registration'
 AND fme.membership_state = 'Lead'
 AND ss.is_in_segment = TRUE
QUALIFY RANK() OVER (PARTITION BY av.customer_id ORDER BY registration_date ASC) = 1;


CREATE OR REPLACE TEMPORARY TABLE _cross_brand_vips AS
SELECT ac1.customer_id
    , (CASE
           WHEN COUNT(ac1.customer_id) = 1 THEN 'N'
           ELSE 'Y' END)     AS is_cross_brand
    , TRIM(LISTAGG((CASE
                   WHEN ac1.business_unit = ac2.business_unit THEN ''
                   ELSE ac2.business_unit || ', ' END))) AS crossed_brands
FROM _all_vips ac1
        JOIN
    (
        SELECT DISTINCT UPPER(ds.store_brand) AS business_unit
                      , LOWER(dc.email)       AS email
        FROM edw_prod.analytics_base.customer_lifetime_value_ltd clv
                 JOIN edw_prod.data_model_jfb.dim_customer dc
                      ON dc.customer_id = clv.customer_id
                           AND dc.is_test_customer = 0
                 JOIN edw_prod.data_model_jfb.dim_store ds
                      ON ds.store_id = clv.store_id
    ) ac2
    ON ac2.email = ac1.email
GROUP BY ac1.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _friend_referral AS
SELECT DISTINCT av.customer_id
              , (CASE
                     WHEN dc.is_friend_referral = 1 THEN 'Y'
                     ELSE 'N' END) AS is_friend_referral
FROM _all_vips av
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = av.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _activating_discount_rate AS
SELECT olp.business_unit
     , olp.region
     , av.customer_id
     , SUM(olp.order_line_subtotal)                           AS subtotal
     , SUM(olp.total_discount)                                AS total_discount
     , SUM(olp.total_discount) / SUM(olp.order_line_subtotal) AS discount_rate
     , SUM(olp.total_product_revenue)                         AS total_product_revenue
FROM _all_vips av
         JOIN gfb.gfb_order_line_data_set_place_date olp
              ON olp.order_id = av.first_activating_order_id
WHERE olp.order_classification = 'product order'
  AND olp.order_type = 'vip activating'
GROUP BY olp.business_unit
       , olp.region
       , av.customer_id
HAVING SUM(olp.order_line_subtotal) > 0;


CREATE OR REPLACE TEMPORARY TABLE _activating_discount_bucket AS
SELECT adr.customer_id
     , (CASE
            WHEN adr.discount_rate >= a.avg_discount_rate AND
                 adr.discount_rate <= a.avg_discount_rate + a.std_discount_rate
                THEN 'Y'
            ELSE 'N' END) AS more_discount_activating
     , (CASE
            WHEN adr.discount_rate >= a.avg_discount_rate - a.std_discount_rate AND
                 adr.discount_rate <= a.avg_discount_rate
                THEN 'Y'
            ELSE 'N' END) AS less_discount_activating
     , (CASE
            WHEN adr.discount_rate > a.avg_discount_rate + a.std_discount_rate
                THEN 'Y'
            ELSE 'N' END) AS most_discount_activating
     , (CASE
            WHEN adr.discount_rate < a.avg_discount_rate - a.std_discount_rate
                THEN 'Y'
            ELSE 'N' END) AS least_discount_activating
FROM _activating_discount_rate adr
         JOIN
     (
         SELECT a.business_unit
              , a.region
              , AVG(a.discount_rate)    AS avg_discount_rate
              , STDDEV(a.discount_rate) AS std_discount_rate
         FROM _activating_discount_rate a
         WHERE a.discount_rate < 1
         GROUP BY a.business_unit
                , a.region
     ) a ON a.business_unit = adr.business_unit
         AND a.region = adr.region;


CREATE OR REPLACE TEMPORARY TABLE _activating_promo_group AS
SELECT a.customer_id
     , LISTAGG(a.promo_group, ', ') AS activating_promo_group
     , (CASE
            WHEN activating_promo_group LIKE '%Activating BOGO%'
                THEN 'Y'
            ELSE 'N' END)           AS activating_bogo_promo
     , (CASE
            WHEN activating_promo_group LIKE '%Price Bucket%'
                THEN 'Y'
            ELSE 'N' END)           AS activating_price_bucket_promo
FROM (
         SELECT av.customer_id
              , pds.promo_1_group AS promo_group
         FROM _all_vips av
                  JOIN gfb.gfb011_promo_data_set pds
                       ON pds.order_id = av.first_activating_order_id
         WHERE pds.promo_code_1 IS NOT NULL
           AND (
                 (pds.business_unit = 'FABKIDS' AND pds.promo_1_group IN ('Activating BOGO'))
                 OR
                 (pds.business_unit != 'FABKIDS' AND pds.promo_1_group IN ('Activating BOGO', 'Price Bucket'))
             )
         UNION
         SELECT av.customer_id
              , pds.promo_2_group AS promo_group
         FROM _all_vips av
                  JOIN gfb.gfb011_promo_data_set pds
                       ON pds.order_id = av.first_activating_order_id
         WHERE pds.promo_code_2 IS NOT NULL
           AND (
                 (pds.business_unit = 'FABKIDS' AND pds.promo_2_group IN ('Activating BOGO'))
                 OR
                 (pds.business_unit != 'FABKIDS' AND pds.promo_2_group IN ('Activating BOGO', 'Price Bucket'))
             )
     ) a
GROUP BY a.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _loyalty_tier AS
SELECT lt.customer_id
     , lt.membership_reward_tier
FROM edw_prod.data_model_jfb.fact_activation_loyalty_tier lt
         JOIN _all_vips av
              ON av.customer_id = lt.customer_id
WHERE lt.is_current = 1;


CREATE OR REPLACE TEMPORARY TABLE _current_month_credit_billing_status AS
SELECT
    a.customer_id
    ,a.credit_billing_month
    ,IFF(a.credit_billing_status like '%,%','Success Credit Billing',a.credit_billing_status) AS credit_billing_status
FROM
(SELECT av.customer_id
              , DATE_TRUNC(MONTH, DATEADD(DAY, -1, CURRENT_DATE())) AS credit_billing_month
              , LISTAGG((CASE
                     WHEN dos.order_status IN ('Success', 'Pending') THEN 'Success Credit Billing'
                     ELSE 'Failed Credit Billing' END),',')              AS credit_billing_status
FROM edw_prod.data_model_jfb.fact_order fo
         JOIN gfb.vw_store st
              ON st.store_id = fo.store_id
         JOIN edw_prod.data_model_jfb.dim_order_status dos
              ON dos.order_status_key = fo.order_status_key
         JOIN _all_vips av
              ON av.customer_id = fo.customer_id
         JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc
              ON dosc.order_sales_channel_key = fo.order_sales_channel_key
                  AND dosc.is_border_free_order = 0
                  AND dosc.is_ps_order = 0
                  AND dosc.is_test_order = 0
WHERE dos.order_status IN ('Success', 'Pending', 'Cancelled', 'Failure')
  AND dosc.order_classification_l1 IN ('Billing Order')
  AND DATE_TRUNC(MONTH, CAST(fo.order_local_datetime AS DATE)) = DATE_TRUNC(MONTH, DATEADD(DAY, -1, CURRENT_DATE()))
GROUP BY av.customer_id
       , DATE_TRUNC(MONTH, DATEADD(DAY, -1, CURRENT_DATE()))) a;

CREATE OR REPLACE TEMPORARY TABLE _activating_promo AS
SELECT b.customer_id
     , RTRIM(REPLACE(b.activating_promo_codes, ' ,', ''), ', ') AS activating_promo_codes
     , RTRIM(REPLACE(b.activating_promo_names, ' ,', ''), ', ') AS activating_promo_names
FROM (
         SELECT a.customer_id
              , COALESCE(promo_1_promotion_code, '') || ', ' ||
                COALESCE(promo_2_promotion_code, '') AS activating_promo_codes
              , COALESCE(promo_1_promotion_name, '') || ', ' ||
                COALESCE(promo_2_promotion_name, '') AS activating_promo_names
         FROM (
                  SELECT ap.customer_id
                       , LISTAGG(ap.promo_1_promotion_code, ', ') AS promo_1_promotion_code
                       , LISTAGG(ap.promo_1_promotion_name, ', ') AS promo_1_promotion_name
                       , LISTAGG(ap.promo_2_promotion_code, ', ') AS promo_2_promotion_code
                       , LISTAGG(ap.promo_2_promotion_name, ', ') AS promo_2_promotion_name
                  FROM (
                           SELECT DISTINCT av.customer_id
                                         , dp_1.promotion_code AS promo_1_promotion_code
                                         , dp_1.promotion_name AS promo_1_promotion_name
                                         , dp_2.promotion_code AS promo_2_promotion_code
                                         , dp_2.promotion_name AS promo_2_promotion_name
                           FROM _all_vips av
                                    JOIN gfb.gfb_order_line_data_set_place_date olp
                                         ON olp.order_id = av.first_activating_order_id
                                    LEFT JOIN gfb.dim_promo dp_1
                                              ON dp_1.promo_id = olp.promo_id_1
                                    LEFT JOIN gfb.dim_promo dp_2
                                              ON dp_2.promo_id = olp.promo_id_2
                           WHERE olp.order_classification = 'product order'
                             AND olp.order_type = 'vip activating'
                       ) ap
                  GROUP BY ap.customer_id
              ) a
     ) b;


CREATE OR REPLACE TEMPORARY TABLE _customer_order_size AS
SELECT DISTINCT olp.customer_id
              , olp.order_id
              , olp.order_date
              , (CASE
                     WHEN mdp.department LIKE '%FOOTWEAR%' OR mdp.department LIKE '%SHOE%'
                         THEN 'FOOTWEAR'
                     ELSE 'APPAREL' END) AS department_norm
              , psm.clean_size           AS size
              , psm.core_size_flag
FROM gfb.gfb_order_line_data_set_place_date olp
         JOIN _all_vips av
              ON av.customer_id = olp.customer_id
         JOIN gfb.merch_dim_product mdp
              ON mdp.business_unit = olp.business_unit
                  AND mdp.region = olp.region
                  AND mdp.country = olp.country
                  AND mdp.product_sku = olp.product_sku
                  AND (
                             mdp.department LIKE '%FOOTWEAR%'
                         OR mdp.department LIKE '%APPAREL%'
                         OR mdp.department LIKE '%SHOE%'
                     )
         JOIN gfb.view_product_size_mapping psm
              ON psm.product_sku = olp.product_sku
                  AND psm.region = olp.region
                  AND psm.size = olp.dp_size
    QUALIFY
            ROW_NUMBER() OVER (PARTITION BY olp.customer_id, department_norm ORDER BY olp.order_date DESC) = 1;


CREATE OR REPLACE TEMPORARY TABLE _upgraded_vip AS
SELECT a.customer_id
FROM (
         SELECT DISTINCT av.customer_id
                       , m.price
         FROM _all_vips av
                  JOIN lake_consolidated.ultra_merchant_history.membership m
                       ON edw_prod.stg.udf_unconcat_brand(m.customer_id) = av.customer_id
                           AND m.effective_end_datetime >= '2022-01-01'
         GROUP BY av.customer_id
                , m.price
     ) a
GROUP BY a.customer_id
HAVING COUNT(a.customer_id) > 1;


CREATE OR REPLACE TEMPORARY TABLE _upgraded_vip_flag AS
SELECT DISTINCT uv.customer_id
              , ud.upgrade_date
FROM _upgraded_vip uv
         JOIN
     (
         SELECT av.customer_id
              , m.price
              , MIN(m.effective_start_datetime) AS upgrade_date
         FROM _all_vips av
                  JOIN lake_consolidated.ultra_merchant_history.membership m
                       ON edw_prod.stg.udf_unconcat_brand(m.customer_id) = av.customer_id
                           AND m.effective_end_datetime >= '2022-01-01'
                           AND m.price = 49.95
         GROUP BY av.customer_id
                , m.price
     ) ud ON ud.customer_id = uv.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _sms_customer_base AS
SELECT (CASE
            WHEN company_id = '1391' THEN 'FabKids'
            WHEN company_id IN ('1027', '2914') THEN 'Fabletics'
            WHEN company_id = '1304' THEN 'JustFab'
            WHEN company_id = '1089' THEN 'Savage X'
            WHEN company_id = '1305' THEN 'ShoeDazzle'
    END)                           AS business_unit,
       phone,
       email,
       source,
       type,
       visitor_id,
       creative_id,
       creative_name,
       creative_type,
       creative_subtype,
       TRY_TO_TIMESTAMP(timestamp) AS timestamp,
       client_id,
       CASE
           WHEN CONTAINS(LOWER(client_id), 'c:') THEN TRY_TO_NUMERIC(LTRIM(SPLIT(client_id, '-')[0], 'c:'))
           WHEN CONTAINS(LOWER(client_id), '_') THEN TRY_TO_NUMERIC(LTRIM(SPLIT(client_id, '_')[0], ' '))
           ELSE TRY_TO_NUMERIC(client_id)
           END                     AS customer_id,
       CASE
           WHEN CONTAINS(LOWER(client_id), 's:') THEN TRY_TO_NUMERIC(LTRIM(SPLIT(client_id, '-')[1], ' s:'))
           WHEN CONTAINS(LOWER(client_id), '_') THEN TRY_TO_NUMERIC(RTRIM(SPLIT(client_id, '_')[1], ' '))
           END                     AS session_id,
       message_id,
       message_name,
       message_type,
       message_subtype,
       message_start,
       meta_create_datetime,
       meta_update_datetime
FROM lake.media.attentive_attentive_sms_legacy
WHERE company_id IN ('1391', '1304', '1305')
  AND phone != ''
  AND customer_id IS NOT NULL
  AND type IN ('JOIN', 'OPT_OUT');


CREATE OR REPLACE TEMPORARY TABLE _sms_optin_status AS
SELECT DISTINCT a.customer_id
              , FIRST_VALUE(a.type) OVER (PARTITION BY a.customer_id ORDER BY a.timestamp DESC ) AS optin_status
FROM _sms_customer_base a;


CREATE OR REPLACE TEMPORARY TABLE _price_history AS
SELECT av.customer_id                  AS customer_id,
       m.store_id,
--       MAX(m.effective_start_datetime) AS effective_start_datetime,
--       MAX(m.effective_end_datetime)   AS effective_end_datetime,
       MAX(m.price)                    AS price
FROM lake_jfb.ultra_merchant.membership AS m
         JOIN _all_vips av
              ON edw_prod.stg.udf_unconcat_brand(m.customer_id) = av.customer_id
                  AND m.store_id = av.store_id
--                  AND m.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'
GROUP BY av.customer_id,
         m.store_id;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb_dim_vip AS
SELECT DISTINCT av.*
              , ao.activating_offers
              , aco.activating_payment_method
              , aco.activating_city
              , aco.activating_state
              , aco.activating_country
              , aco.activating_region
              , aco.activating_item_count
              , aco.activating_departments
              , aco.activating_plus_size
              , aco.activating_ww_wc
              , (CASE
                     WHEN aco.activating_state IN
                          ('TX', 'OK', 'AR', 'LA', 'MS', 'AL', 'TN', 'KY', 'WV', 'FL', 'GA', 'SC', 'NC', 'VA', 'MD',
                           'DE', 'DC') THEN 'South'
                     WHEN aco.activating_state IN ('CA', 'OR', 'WA', 'NV', 'UT', 'AZ', 'NM', 'CO', 'WY', 'MT', 'ID')
                         THEN 'West'
                     WHEN aco.activating_state IN
                          ('KS', 'NE', 'SD', 'ND', 'MN', 'IA', 'MO', 'IL', 'WI', 'MI', 'IN', 'OH') THEN 'Midwest'
                     WHEN aco.activating_state IN ('ME', 'PA', 'NY', 'VT', 'NJ', 'CT', 'MA', 'RI', 'NH')
                         THEN 'Northeast'
                     WHEN aco.activating_state IN ('HI', 'AK') THEN 'Pacific'
                     WHEN aco.activating_region = 'EU' THEN aco.activating_country
                     ELSE 'Unknown' END)                                                                  AS customer_region
              , aco.activating_comfort
              , aco.activating_order_shipping_option
              , aco.days_of_delivery
              , aco.gamer_activation_flag
              , aco.total_product_revenue                                                                 AS first_activating_product_revenue
              , ag.gateway_name
              , ag.gateway_type
              , ag.channel
              , ag.activating_device
              , ag.activating_medium
              , ag.activating_source
              , qr.registration_type
              , qr.days_between_registration_activation
              , qr.registration_date
              , (CASE
                     WHEN av.business_unit IN ('JUSTFAB', 'SHOEDAZZLE') AND qr.days_between_registration_activation >= 8
                         THEN 'Aged Lead Activation'
                     WHEN av.business_unit IN ('FABKIDS') AND qr.days_between_registration_activation >= 30
                         THEN 'Aged Lead Activation'
                     ELSE 'Non Aged Lead Activation' END)                                                 AS aged_lead_activation_flag
              , qr.registration_utm_medium
              , qr.registration_utm_source
              , qr.registration_utm_campaign
              , cbv.is_cross_brand
              , (CASE
                     WHEN cbv.crossed_brands LIKE '%FABKIDS%'
                         THEN 'Y'
                     ELSE 'N' END)                                                                        AS is_fk_cross_brand
              , (CASE
                     WHEN cbv.crossed_brands LIKE '%JUSTFAB%'
                         THEN 'Y'
                     ELSE 'N' END)                                                                        AS is_jf_cross_brand
              , (CASE
                     WHEN cbv.crossed_brands LIKE '%SHOEDAZZLE%'
                         THEN 'Y'
                     ELSE 'N' END)                                                                        AS is_sd_cross_brand
              , (CASE
                     WHEN cbv.crossed_brands LIKE '%SAVAGE X%'
                         THEN 'Y'
                     ELSE 'N' END)                                                                        AS is_sx_cross_brand
              , (CASE
                     WHEN cbv.crossed_brands LIKE '%FABLETICS%'
                         THEN 'Y'
                     ELSE 'N' END)                                                                        AS is_fl_cross_brand
              , fr.is_friend_referral
              , adb.least_discount_activating
              , adb.less_discount_activating
              , adb.more_discount_activating
              , adb.most_discount_activating
              , COALESCE(apg.activating_bogo_promo, 'N')                                                  AS activating_bogo_promo
              , COALESCE(apg.activating_price_bucket_promo, 'N')                                          AS activating_price_bucket_promo
              , lt.membership_reward_tier
              , (CASE
                     WHEN cbs.customer_id IS NULL THEN 'No Credit Billing - ' ||
                                                       CAST(DATE_TRUNC(MONTH, DATEADD(DAY, -1, CURRENT_DATE())) AS VARCHAR(100))
                     WHEN cbs.customer_id IS NOT NULL THEN cbs.credit_billing_status || ' - ' ||
                                                           CAST(cbs.credit_billing_month AS VARCHAR(100))
    END)                                                                                                  AS current_month_credit_billing_status
              , NTILE(10)
                      OVER (PARTITION BY av.first_activating_cohort ORDER BY av.cash_gross_profit ASC)    AS cash_gross_margin_decile_ltd
              , NTILE(10)
                      OVER (PARTITION BY av.first_activating_cohort ORDER BY av.product_gross_profit ASC) AS gaap_gross_margin_decile_ltd
              , m.price                                                                                   AS membership_price
              , ap.activating_promo_codes
              , ap.activating_promo_names
              , COALESCE(cosf.size, 'unknown')                                                            AS footwear_size
              , COALESCE(cosf.core_size_flag, 'unknown')                                                  AS footwear_core_size_flag
              , COALESCE(cosa.size, 'unknown')                                                            AS apparel_size
              , COALESCE(cosa.core_size_flag, 'unknown')                                                  AS apparel_core_size_flag
              , (CASE
                     WHEN uvf.customer_id IS NOT NULL THEN 1
                     ELSE 0 END)                                                                          AS upgrade_vip_flag
              , uvf.upgrade_date
              , COALESCE(sos.optin_status, 'Unknown')                                                     AS sms_optin_status
FROM _all_vips av
         LEFT JOIN _activating_offer ao
                   ON ao.customer_id = av.customer_id
         LEFT JOIN _activating_order aco
                   ON aco.customer_id = av.customer_id
         LEFT JOIN _activating_gateway ag
                   ON ag.customer_id = av.customer_id
         LEFT JOIN _lead_registration qr
                   ON qr.customer_id = av.customer_id
         LEFT JOIN _cross_brand_vips cbv
                   ON cbv.customer_id = av.customer_id
         LEFT JOIN _friend_referral fr
                   ON fr.customer_id = av.customer_id
         LEFT JOIN _activating_discount_bucket adb
                   ON adb.customer_id = av.customer_id
         LEFT JOIN _activating_promo_group apg
                   ON apg.customer_id = av.customer_id
         LEFT JOIN _loyalty_tier lt
                   ON lt.customer_id = av.customer_id
         LEFT JOIN _current_month_credit_billing_status cbs
                   ON cbs.customer_id = av.customer_id
         left JOIN _price_history m
              ON m.customer_id = av.customer_id
                  AND m.store_id = av.store_id
         LEFT JOIN _activating_promo ap
                   ON ap.customer_id = av.customer_id
         LEFT JOIN _customer_order_size cosf
                   ON cosf.customer_id = av.customer_id
                       AND cosf.department_norm = 'FOOTWEAR'
         LEFT JOIN _customer_order_size cosa
                   ON cosa.customer_id = av.customer_id
                       AND cosa.department_norm = 'APPAREL'
         LEFT JOIN _upgraded_vip_flag uvf
                   ON uvf.customer_id = av.customer_id
         LEFT JOIN _sms_optin_status sos
                   ON sos.customer_id = av.customer_id;


CREATE OR REPLACE TRANSIENT TABLE gfb.gfb_dim_vip_cohort_monthly AS
SELECT dv.business_unit
     , dv.region
     , dv.country
     , dv.customer_id
     , clvm.month_date
     , MAX(clvm.vip_cohort_month_date) AS vip_cohort
FROM gfb.gfb_dim_vip dv
         JOIN edw_prod.analytics_base.customer_lifetime_value_monthly_jfb clvm
              ON clvm.meta_original_customer_id = dv.customer_id
                  AND clvm.store_id = dv.store_id
GROUP BY dv.business_unit
       , dv.region
       , dv.country
       , dv.customer_id
       , clvm.month_date;
