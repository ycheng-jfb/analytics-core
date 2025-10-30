SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, CURRENT_DATE()));
SET end_date = CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _all_customers AS
SELECT st.store_brand                  AS store_brand_name
     , st.store_brand_abbr
     , (CASE
            WHEN dc.specialty_country_code = 'GB' THEN 'UK'
            WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
            ELSE st.store_country END) AS store_country_abbr
     , st.store_region                 AS store_region_abbr
     , st.store_id
     , clv.customer_id
     , clv.first_activating_cohort     AS vip_cohort
     , clv.first_activating_date       AS first_activation_date
     , DATEDIFF(MONTH, clv.first_activating_date, COALESCE(clv.recent_vip_cancellation_date, CURRENT_DATE())) +
       1                               AS current_vip_tenure
     , DATEDIFF(DAY, clv.registration_date, COALESCE(clv.first_activating_date, CURRENT_DATE())) +
       1                               AS lead_tenure
     , (CASE
            WHEN clv.current_membership_status = 'VIP'
                THEN 1
            ELSE 0 END)                AS is_active
     , LOWER(dc.email)                 AS email
     , 'All VIPs'                      AS custom_segment_category
     , 'All VIPs'                      AS custom_segment
     , clv.membership_price
     , clv.sms_optin_status
     , clv.upgrade_vip_flag
FROM reporting_prod.gfb.gfb_dim_vip clv
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = clv.customer_id
                  AND dc.is_test_customer = 0
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = clv.store_id
                  AND clv.first_activating_cohort IS NOT NULL
                  AND clv.first_activating_cohort != '1900-01-01'
                  AND clv.first_activating_cohort >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _most_recent_activating_order AS
SELECT customer_id
     , MAX(order_id) AS max_activating_order
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date
WHERE order_type = 'vip activating'
  AND order_classification = 'product order'
GROUP BY customer_id;

CREATE OR REPLACE TEMPORARY TABLE _activating_order AS
SELECT DISTINCT ol.customer_id
              , CASE WHEN sub_brand = 'JFB' THEN mdp.business_unit ELSE sub_brand END AS sub_brand
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date ol
         JOIN reporting_prod.gfb.merch_dim_product mdp
              ON ol.business_unit = mdp.business_unit
                  AND ol.region = mdp.region
                  AND ol.country = mdp.country
                  AND ol.product_sku = mdp.product_sku
         JOIN _most_recent_activating_order mrao ON ol.customer_id = mrao.customer_id
    AND ol.order_id = mrao.max_activating_order
WHERE promo_code_1 IN ('LEADDAY1ALLBOOTS10NEW',
                       'FIRSTPAIR10DAY8',
                       'SHOES10MEN',
                       '75OFF',
                       'D37_SHOES10',
                       'FIRSTPAIR10DAY8MEN',
                       'D37_SHOES10_MEN',
                       'SHOES10GIRLS',
                       'FIRSTPAIR10DAY8GIRLS',
                       'SHOES10BOYS',
                       'FIRSTPAIR10DAY8BOYS',
                       'D37_SHOES10_BOYS')
  AND department_detail IN ('FOOTWEAR', 'GIRLS SHOES', 'BOYS SHOES', 'MENS - SHOES', 'KIDS - BOYS', 'KIDS - GIRLS');

CREATE OR REPLACE TEMPORARY TABLE _activating_sub_brand AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Acquisition Order Sub-Brand' AS custom_segment_category
     , ao.sub_brand                  AS custom_segment
FROM _all_customers ac
         JOIN _activating_order ao ON ac.customer_id = ao.customer_id;

-- EMP New vs Converted VIPs based on nmp_migration tag & activation date
CREATE OR REPLACE TEMPORARY TABLE _emp_new_vs_converted_vips AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'EMP New vs Converted VIPs' AS custom_segment_category
     , 'Converted VIPs'            AS custom_segment
FROM lake_jfb_view.ultra_merchant.membership_detail md
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.membership_id = md.membership_id
         JOIN _all_customers ac
              ON ac.customer_id = m.customer_id
WHERE md.name = 'nmp_migration'

UNION

SELECT fa.customer_id
     , DATE_TRUNC('month', activation_local_datetime) AS vip_cohort
     , 'EMP New vs Converted VIPs'                    AS custom_segment_category
     , 'New VIPs'                                     AS custom_segment
FROM edw_prod.data_model_jfb.fact_activation fa
         JOIN edw_prod.data_model_jfb.dim_store ds ON fa.store_id = ds.store_id
WHERE activation_local_datetime >= '2023-07-18'
  AND UPPER(ds.store_brand) = 'JUSTFAB'
  AND ds.store_region = 'NA'
  AND fa.is_current = 'true'

UNION

SELECT fa.customer_id
     , DATE_TRUNC('month', activation_local_datetime) AS vip_cohort
     , 'EMP New vs Converted VIPs'                    AS custom_segment_category
     , 'New VIPs'                                     AS custom_segment
FROM edw_prod.data_model_jfb.fact_activation fa
         JOIN edw_prod.data_model_jfb.dim_store ds ON fa.store_id = ds.store_id
WHERE activation_local_datetime >= '2023-09-26'
  AND UPPER(ds.store_brand) IN ('FABKIDS')
  AND ds.store_region = 'NA'
  AND fa.is_current = 'true'

UNION

SELECT fa.customer_id
     , DATE_TRUNC('month', activation_local_datetime) AS vip_cohort
     , 'EMP New vs Converted VIPs'                    AS custom_segment_category
     , 'New VIPs'                                     AS custom_segment
FROM edw_prod.data_model_jfb.fact_activation fa
         JOIN edw_prod.data_model_jfb.dim_store ds ON fa.store_id = ds.store_id
WHERE activation_local_datetime >= '2023-09-21'
  AND UPPER(ds.store_brand) IN ('SHOEDAZZLE')
  AND ds.store_region = 'NA'
  AND fa.is_current = 'true';


CREATE OR REPLACE TEMPORARY TABLE _first_activating_order AS
SELECT olp.customer_id
     , olp.order_id
     , olp.is_prepaid_creditcard
     , RANK() OVER (PARTITION BY olp.customer_id ORDER BY olp.order_id ASC) AS activating_order_rank
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
WHERE olp.order_classification = 'product order'
  AND olp.order_type = 'vip activating'
    QUALIFY activating_order_rank = 1;


CREATE OR REPLACE TEMPORARY TABLE _prepaid_credit_card AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Prepaid Credit Card'                AS custom_segment_category
     , (CASE
            WHEN fao.is_prepaid_creditcard = 1 THEN 'PPCC Activation'
            ELSE 'Non-PPCC Activation' END) AS custom_segment
FROM _all_customers ac
         JOIN _first_activating_order fao
              ON fao.customer_id = ac.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _afterpay_vip AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Activated with Afterpay' AS custom_segment_category
     , (CASE
            WHEN dpm.raw_creditcard_type = 'Afterpay' THEN 'Y'
            ELSE 'N' END)        AS custom_segment
FROM edw_prod.data_model_jfb.fact_order fol
         JOIN _first_activating_order fao
              ON fao.order_id = fol.order_id
         JOIN _all_customers ac
              ON ac.customer_id = fao.customer_id
         JOIN edw_prod.data_model_jfb.dim_payment dpm
              ON dpm.payment_key = fol.payment_key;


CREATE OR REPLACE TEMPORARY TABLE _free_trial_da15752 AS
--Due to promo setup mistake, dim_customer does not have these customer flagged as free trial
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Free Trial' AS custom_segment_category
     , 'Free Trial' AS custom_segment
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
         JOIN _all_customers ac
              ON ac.customer_id = olp.customer_id
WHERE olp.order_classification = 'product order'
  AND olp.order_type = 'vip activating'
  AND (
            olp.promo_code_1 IN ('GENFPL95OFF', 'GENFPL90OFF')
        OR
            olp.promo_code_2 IN ('GENFPL95OFF', 'GENFPL90OFF')
    )
  AND olp.order_date >= '2020-12-14'
  AND olp.order_date < '2020-12-21';


CREATE OR REPLACE TEMPORARY TABLE _free_trial AS
SELECT *
FROM _free_trial_da15752 ftd15752

UNION

SELECT ac.customer_id
     , ac.vip_cohort
     , 'Free Trial'                    AS custom_segment_category
     , (CASE
            WHEN dc.is_free_trial = 1 THEN 'Free Trial'
            ELSE 'Not Free Trial' END) AS custom_segment
FROM _all_customers ac
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = ac.customer_id
WHERE ac.customer_id NOT IN (
    SELECT DISTINCT a.customer_id
    FROM _free_trial_da15752 a
);


CREATE OR REPLACE TEMPORARY TABLE _vip_repeat_order AS
SELECT a.customer_id
     , a.vip_cohort
     , a.custom_segment_category
     , (CASE
            WHEN a.orders = 1 THEN '1'
            WHEN a.orders <= 4 THEN '2-4'
            WHEN a.orders <= 7 THEN '4-7'
            WHEN a.orders > 7 THEN '8+'
    END) AS custom_segment
FROM (
         SELECT ac.customer_id
              , ac.vip_cohort
              , 'Count of Repeat Purchases since ' || CAST($start_date AS VARCHAR(50)) AS custom_segment_category
              , COUNT(DISTINCT olp.order_id)                                           AS orders
         FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
                  JOIN _all_customers ac
                       ON ac.customer_id = olp.customer_id
         WHERE olp.order_classification = 'product order'
           AND olp.order_type = 'vip repeat'
           AND olp.order_date >= $start_date
           AND olp.order_date < $end_date
         GROUP BY ac.customer_id
                , ac.vip_cohort
     ) a
UNION

SELECT a.customer_id
     , a.vip_cohort
     , a.custom_segment_category
     , 'All Customers' AS custom_segment
FROM (
         SELECT ac.customer_id
              , ac.vip_cohort
              , 'Count of Repeat Purchases since ' || CAST($start_date AS VARCHAR(50)) AS custom_segment_category
              , COUNT(DISTINCT olp.order_id)                                           AS orders
         FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
                  JOIN _all_customers ac
                       ON ac.customer_id = olp.customer_id
         WHERE olp.order_classification = 'product order'
           AND olp.order_type = 'vip repeat'
           AND olp.order_date >= $start_date
           AND olp.order_date < $end_date
         GROUP BY ac.customer_id
                , ac.vip_cohort
     ) a;



CREATE OR REPLACE TEMPORARY TABLE _membership_price AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Membership Price'                                    AS custom_segment_category
     , (CASE
            WHEN ac.store_region_abbr = 'EU' AND m.price = 399 THEN '39.95'
            WHEN ac.store_region_abbr = 'EU' AND m.price = 349 THEN '39.95'
            WHEN ac.upgrade_vip_flag = 1 THEN '39.95 to 49.95'
            ELSE CAST(ROUND(m.price, 2) AS VARCHAR(20)) END) AS custom_segment
FROM _all_customers ac
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
WHERE ac.store_brand_abbr != 'FK'

UNION

SELECT ac.customer_id
     , ac.vip_cohort
     , 'Membership Price'                                     AS custom_segment_category
     , (CASE
            WHEN optin.customer_id IS NOT NULL THEN 'opt-in originally $29.95'
            WHEN ac.store_region_abbr = 'EU' AND m.price = 399 THEN '39.95'
            WHEN ac.store_region_abbr = 'EU' AND m.price = 349 THEN '39.95'
            WHEN ac.upgrade_vip_flag = 1 THEN '39.95 to 49.95'
            ELSE CAST(ROUND(m.price, 2) AS VARCHAR(100)) END) AS custom_segment
FROM _all_customers ac
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
         LEFT JOIN
     (
         SELECT DISTINCT ac.customer_id
         FROM _all_customers ac
                  JOIN lake_jfb_view.ultra_merchant.membership m
                       ON m.customer_id = ac.customer_id
                  JOIN lake_jfb_view.ultra_merchant.membership_detail md
                       ON md.membership_id = m.membership_id
         WHERE ac.store_brand_abbr = 'FK'
           AND md.name = 'vip-3995-upgrade-complete'
           AND md.value = 1
     ) optin ON optin.customer_id = ac.customer_id
WHERE ac.store_brand_abbr = 'FK';


CREATE OR REPLACE TEMPORARY TABLE _gamers AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Gamers'                   AS custom_segment_category
     , (CASE
            WHEN gamers.customer_id IS NOT NULL THEN 'Gamer'
            ELSE 'Non-Gamer' END) AS custom_segment
FROM _all_customers ac
         LEFT JOIN
     (
         SELECT DISTINCT cd.customer_id
         FROM lake_jfb_view.ultra_merchant.customer_detail cd
         WHERE cd.name = 'gaming_account'
           AND cd.value = 'true'
     ) gamers ON gamers.customer_id = ac.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _customer_state AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Customer State'              AS custom_segment_category
     , (CASE
            WHEN UPPER(customer_state.state) = 'CA' THEN '1. CA'
            WHEN UPPER(customer_state.state) IN ('WA', 'PA', 'MD', 'TX', 'DC') THEN '2. WA,PA,MD,TX,DC'
            WHEN customer_state.state IS NOT NULL THEN '3. Rest of US'
            ELSE '4. Not in US' END) AS custom_segment
FROM _all_customers ac
         LEFT JOIN
     (
         SELECT DISTINCT c.customer_id
                       , (CASE
                              WHEN LOWER(a.state) = 'cal' THEN 'CA'
                              WHEN LOWER(a.state) = 'ohi' THEN 'OH'
                              WHEN LOWER(a.state) = 'flo' THEN 'FL'
                              WHEN LOWER(a.state) = 'ill' THEN 'IL'
                              WHEN LOWER(a.state) = 'ari' THEN 'AZ'
                              ELSE UPPER(a.state) END) AS state
         FROM lake_jfb_view.ultra_merchant.customer c
                  JOIN reporting_prod.gfb.vw_store st
                       ON st.store_id = c.store_id
                  JOIN lake_jfb_view.ultra_merchant.membership m
                       ON m.customer_id = c.customer_id
                  JOIN lake_jfb_view.ultra_merchant.address a
                       ON a.address_id = c.default_address_id
                           AND a.country_code = 'US'
     ) customer_state ON customer_state.customer_id = ac.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _outstanding_membership_credit AS
SELECT c.customer_id
     , c.vip_cohort
     , 'Outstanding Membership Credit' AS custom_segment_category
     , (CASE
            WHEN COALESCE(a.credit_count, 0) = 0 THEN '0'
            WHEN COALESCE(a.credit_count, 0) = 1 THEN '1'
            WHEN COALESCE(a.credit_count, 0) > 1 THEN '1+'
    END)                               AS custom_segment
FROM _all_customers c
         LEFT JOIN
     (SELECT ac.customer_id
           , ac.vip_cohort
           , COUNT(DISTINCT dc.credit_id) AS credit_count
      FROM edw_prod.data_model_jfb.dim_credit dc
               JOIN lake_jfb_view.ultra_merchant.store_credit sc
                    ON sc.store_credit_id = dc.credit_id
                        AND dc.source_credit_id_type = 'store_credit_id'
               JOIN _all_customers ac
                    ON ac.customer_id = sc.customer_id
               JOIN lake_jfb_view.ultra_merchant.statuscode scod
                    ON scod.statuscode = sc.statuscode
      WHERE dc.credit_type = 'Fixed Credit'
        AND dc.credit_reason = 'Membership Credit'
        AND scod.label = 'Active'
        AND ac.store_region_abbr = 'EU'
      GROUP BY ac.customer_id
             , ac.vip_cohort
      UNION
      SELECT fa.customer_id,
             ac.vip_cohort,
             COUNT(DISTINCT mtrr.membership_token_id) AS outstanding_membership_credit
      FROM edw_prod.data_model_jfb.fact_activation fa
               LEFT JOIN
           (SELECT DISTINCT edw_prod.stg.udf_unconcat_brand(m.customer_id) AS customer_id,
                            mt.membership_token_id,
                            mt.DATETIME_EXPIRED
            FROM lake_jfb.ultra_merchant_history.membership_token mt
                     JOIN lake_jfb_view.ultra_merchant.membership m ON m.membership_id = mt.membership_id
                     JOIN lake_jfb_view.ultra_merchant.membership_token_reason mtr
                          ON mtr.membership_token_reason_id = mt.membership_token_reason_id
                     JOIN lake_jfb_view.ultra_merchant.statuscode sc ON sc.statuscode = mt.statuscode
            WHERE sc.label = 'Active'
              AND mtr.label IN ('Token Billing', 'Converted Membership Credit', 'Refund - Converted Credit')
           ) mtrr ON edw_prod.stg.udf_unconcat_brand(mtrr.customer_id) = fa.customer_id
               LEFT JOIN edw_prod.data_model_jfb.dim_customer dc
                         ON fa.customer_id = dc.customer_id
               JOIN _all_customers ac
                    ON ac.customer_id = mtrr.customer_id
      WHERE ac.store_region_abbr != 'EU'
        AND mtrr.DATETIME_EXPIRED > CURRENT_DATE()
      GROUP BY fa.customer_id, ac.vip_cohort
     ) a ON a.customer_id = c.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _customer_purchased_products AS
SELECT DISTINCT ac.customer_id
              , ac.vip_cohort
              , ac.store_brand_name
              , ac.store_region_abbr
              , ac.store_country_abbr
              , fol.product_sku
              , mfd.department
              , mfd.department_detail
              , (CASE
                     WHEN fol.clearance_flag = 'clearance' THEN 'Y'
                     ELSE 'N' END) AS is_clearance_flag
              , fol.order_type     AS is_activating
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date fol
         JOIN _all_customers ac
              ON ac.customer_id = fol.customer_id
         JOIN reporting_prod.gfb.merch_dim_product mfd
              ON LOWER(mfd.business_unit) = LOWER(fol.business_unit)
                  AND LOWER(mfd.region) = LOWER(fol.region)
                  AND LOWER(mfd.country) = LOWER(fol.country)
                  AND mfd.product_sku = fol.product_sku
WHERE fol.order_classification = 'product order'
  AND fol.order_type IN ('vip activating', 'vip repeat')
  AND fol.order_date >= $start_date
  AND fol.order_date < $end_date;


CREATE OR REPLACE TEMPORARY TABLE _activating_product_deparment AS
SELECT a.customer_id
     , a.vip_cohort
     , 'Product Department Buyers - Activating' AS custom_segment_category
     , LISTAGG(a.department, ', ')              AS custom_segment
FROM (
         SELECT DISTINCT cpp.customer_id
                       , cpp.vip_cohort
                       , LEFT(cpp.department, 3) AS department
         FROM _customer_purchased_products cpp
         WHERE cpp.is_activating = 'vip activating'
         ORDER BY cpp.customer_id
                , LEFT(cpp.department, 3)
     ) a
GROUP BY a.customer_id
       , a.vip_cohort;


CREATE OR REPLACE TEMPORARY TABLE _garment_weekender_bag_vips AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'VIPs through Garment Weekender Bag'            AS custom_segment_category
     , (CASE
            WHEN (mfd.style_name IN ('Garment Convertible Weekender Bag', 'Garment Weekender Bag'))
                THEN 'Garment Weekender Bag VIPs'
            ELSE 'Non Garment Weekender Bag VIPs' END) AS custom_segment
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date fol
         JOIN _all_customers ac
              ON ac.customer_id = fol.customer_id
         JOIN reporting_prod.gfb.merch_dim_product mfd
              ON LOWER(mfd.business_unit) = LOWER(fol.business_unit)
                  AND LOWER(mfd.region) = LOWER(fol.region)
                  AND LOWER(mfd.country) = LOWER(fol.country)
                  AND mfd.product_sku = fol.product_sku
WHERE fol.order_classification = 'product order'
  AND fol.order_type IN ('vip activating')
  AND fol.order_date >= '2023-05-01';


CREATE OR REPLACE TEMPORARY TABLE _current_membership_state AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Current Membership State' AS custom_segment_category
     , fma.membership_state       AS custom_segment
FROM edw_prod.data_model_jfb.fact_membership_event fma
         JOIN _all_customers ac
              ON ac.customer_id = fma.customer_id
WHERE fma.is_current = 1;


CREATE OR REPLACE TEMPORARY TABLE _cross_brand_vip AS
SELECT a.customer_id
     , a.vip_cohort
     , 'Cross Brand VIP'  AS custom_segment_category
     , (CASE
            WHEN a.cross_brand_indicator = 1 THEN 'N'
            ELSE 'Y' END) AS custom_segment
FROM (
         SELECT ac1.customer_id
              , ac1.vip_cohort
              , COUNT(ac1.customer_id) AS cross_brand_indicator
         FROM _all_customers ac1
                  JOIN _all_customers ac2
                       ON ac2.email = ac1.email
         GROUP BY ac1.customer_id
                , ac1.vip_cohort
     ) a;


CREATE OR REPLACE TEMPORARY TABLE _opt_out_vip AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Opt Out VIP'      AS custom_segment_category
     , (CASE
            WHEN scoi.opt_in = 0 THEN 'Y'
            ELSE 'N' END) AS custom_segment
FROM _all_customers ac
         LEFT JOIN reporting_prod.gfb.view_customer_opt_info scoi
                   ON scoi.customer_id = ac.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _mobile_app_vip AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Mobile App User'  AS custom_segment_category
     , (CASE
            WHEN a.customer_id IS NOT NULL THEN 'Y'
            ELSE 'N' END) AS custom_segment
FROM _all_customers ac
         LEFT JOIN
     (
         SELECT DISTINCT ac.customer_id
         FROM _all_customers ac
                  JOIN reporting_base_prod.shared.session_single_view_media smd
                       ON edw_prod.stg.udf_unconcat_brand(smd.customer_id) = ac.customer_id
                  JOIN reporting_prod.gfb.vw_store st
                       ON st.store_id = smd.store_id
                           AND st.store_type = 'Mobile App'
         WHERE smd.sessionlocaldatetime >= DATEADD(YEAR, -1, $end_date)
     ) a ON a.customer_id = ac.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _apparel_plus_vip AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Apparel Plus VIP'                AS custom_segment_category
     , (CASE
            WHEN plus.customer_id IS NOT NULL THEN 'Apparel Plus'
            ELSE 'Non Apparel Plus' END) AS custom_segment
FROM _all_customers ac
         LEFT JOIN
     (
         SELECT DISTINCT cpp.customer_id
         FROM _customer_purchased_products cpp
         WHERE cpp.department_detail = 'APPAREL PLUS'
     ) plus ON plus.customer_id = ac.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _aged_lead_promo AS
SELECT fol.customer_id
     , ac.vip_cohort
     , spc.custom_segment_category
     , spc.custom_segment
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date fol
         JOIN lake_view.sharepoint.gfb_customer_segment_promo_code spc
              ON LOWER(spc.business_unit) = LOWER(fol.business_unit)
                  AND LOWER(spc.region) = LOWER(fol.region)
                  AND (
                             spc.promo_code = fol.promo_code_1
                         OR
                             spc.promo_code = fol.promo_code_2
                     )
                  AND (fol.order_date BETWEEN spc.start_date AND spc.end_date)
         JOIN _all_customers ac
              ON ac.customer_id = fol.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _customers_with_session AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'VIP With Session'                                   AS custom_segment_category
     , (CASE
            WHEN s.customer_id IS NOT NULL THEN 'VIP With Session In First 5 Days'
            ELSE 'VIP Without Session In First 5 Days' END) AS custom_segment
FROM _all_customers ac
         LEFT JOIN reporting_base_prod.shared.session s
                   ON edw_prod.stg.udf_unconcat_brand(s.customer_id) = ac.customer_id
                       AND s.session_local_datetime >= DATE_TRUNC(MONTH, $end_date)
                       AND DAYOFMONTH(s.session_local_datetime) < 6
                       AND DATE_TRUNC(MONTH, s.session_local_datetime) >= ac.vip_cohort;


CREATE OR REPLACE TEMPORARY TABLE _single_item_activating AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Single Item Activating'                  AS custom_segment_category
     , (CASE
            WHEN SUM(olp.total_qty_sold) = 1 THEN 'Single Item Activating'
            ELSE 'Multiple Item Activating' END) AS custom_segment
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
         JOIN _first_activating_order fao
              ON fao.order_id = olp.order_id
         JOIN _all_customers ac
              ON ac.customer_id = fao.customer_id
GROUP BY ac.customer_id
       , ac.vip_cohort;


CREATE OR REPLACE TEMPORARY TABLE _customer_bucket_segment AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Customer Bucket Group'                 AS custom_segment_category
     , TO_VARCHAR(MOD(ac.customer_id, 20) + 1) AS custom_segment
FROM _all_customers ac
         JOIN edw_prod.data_model_jfb.dim_customer dc ON ac.customer_id = dc.customer_id
WHERE dc.email NOT ILIKE '%@reacquired.local%'
  AND dc.is_test_customer = 0
;


CREATE OR REPLACE TEMPORARY TABLE _offer_test_lookalike_2021 AS
SELECT ac.customer_id
     , ac.vip_cohort
     , '2021 Offer Test Lookalike' AS custom_segment_category
     , (CASE
            WHEN a.customer_id IS NOT NULL THEN 'Y'
            ELSE 'N' END)          AS custom_segment
FROM _all_customers ac
         LEFT JOIN
     (
         SELECT olp.customer_id
              , olp.order_id
              , olp.order_date

              , SUM(olp.total_qty_sold)                                        AS total_qty_sold
              , SUM(olp.total_product_revenue)                                 AS total_product_revenue
              , SUM(olp.total_product_revenue) * 1.0 / SUM(olp.total_qty_sold) AS aur
         FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
                  JOIN _first_activating_order fao
                       ON fao.order_id = olp.order_id
         WHERE olp.order_classification = 'product order'
           AND olp.order_type = 'vip activating'
         GROUP BY olp.customer_id
                , olp.order_id
                , olp.order_date
         HAVING SUM(olp.total_qty_sold) > 1
            AND aur >= 15
     ) a ON a.customer_id = ac.customer_id;


-- CREATE OR REPLACE TEMPORARY TABLE _jfna_2021_new_offer_test AS
-- SELECT edw_prod.stg.udf_unconcat_brand(s.customer_id) AS customer_id
--      , ac.vip_cohort
--      , 'JFNA 2021 New Offer Test'                     AS custom_segment_category
--      , 'Test'                                         AS custom_segment
-- FROM reporting_base_prod.shared.session s
--          LEFT JOIN reporting_base_prod.shared.dim_gateway_test_site dgg
--                    ON dgg.dm_gateway_test_site_id = s.dm_gateway_test_site_id
--                        AND dgg.effective_start_datetime <= s.session_local_datetime
--                        AND dgg.effective_end_datetime > s.session_local_datetime
--          JOIN _all_customers ac
--               ON ac.customer_id = edw_prod.stg.udf_unconcat_brand(s.customer_id)
--                   AND ac.first_activation_date >= '2021-08-24'
-- WHERE (
--         (s.dm_gateway_id = 1297110 AND dgg.gateway_test_id = 6062110 AND s.dm_site_id = 3900710)
--         OR
--         (s.dm_gateway_id = 1297210 AND dgg.gateway_test_id = 6062210 AND s.dm_site_id = 3901510)
--         OR
--         (s.dm_gateway_id = 1298810 AND dgg.gateway_test_id = 6062310 AND s.dm_site_id = 3903610)
--         OR
--         (s.dm_gateway_id = 1298910 AND dgg.gateway_test_id = 6062410 AND s.dm_site_id = 3905110)
--         OR
--         (s.dm_gateway_id = 338010 AND dgg.gateway_test_id = 6072510 AND s.dm_site_id = 3903710)
--         OR
--         (s.dm_gateway_id = 338110 AND dgg.gateway_test_id = 6072610 AND s.dm_site_id = 3903810)
--         OR
--         (s.dm_gateway_id = 414710 AND dgg.gateway_test_id = 6072710 AND s.dm_site_id = 3903710)
--         OR
--         (s.dm_gateway_id = 414810 AND dgg.gateway_test_id = 6072810 AND s.dm_site_id = 3903810)
--         OR
--         (s.dm_gateway_id = 1299410 AND dgg.gateway_test_id = 6062510 AND s.dm_site_id = 3900810)
--         OR
--         (s.dm_gateway_id = 1299510 AND dgg.gateway_test_id = 6062610 AND s.dm_site_id = 3901610)
--         OR
--         (s.dm_gateway_id = 1297110 AND dgg.gateway_test_id = 6118510 AND s.dm_site_id = 3957210)
--         OR
--         (s.dm_gateway_id = 1297210 AND dgg.gateway_test_id = 6118810 AND s.dm_site_id = 3957410)
--     )
--   AND CAST(s.session_local_datetime AS DATE) >= '2021-08-24'
--   AND s.membership_state = 'Prospect'

-- UNION

-- SELECT edw_prod.stg.udf_unconcat_brand(s.customer_id) AS customer_id
--      , ac.vip_cohort
--      , 'JFNA 2021 New Offer Test'                     AS custom_segment_category
--      , 'Control'                                      AS custom_segment
-- FROM reporting_base_prod.shared.session s
--          LEFT JOIN reporting_base_prod.shared.dim_gateway_test_site dgg
--                    ON dgg.dm_gateway_test_site_id = s.dm_gateway_test_site_id
--                        AND dgg.effective_start_datetime <= s.session_local_datetime
--                        AND dgg.effective_end_datetime > s.session_local_datetime
--          JOIN _all_customers ac
--               ON ac.customer_id = edw_prod.stg.udf_unconcat_brand(s.customer_id)
--                   AND ac.first_activation_date >= '2021-08-24'
-- WHERE (
--         (s.dm_gateway_id = 1297110 AND dgg.gateway_test_id = 6062110 AND s.dm_site_id = 3900210)
--         OR
--         (s.dm_gateway_id = 1297210 AND dgg.gateway_test_id = 6062210 AND s.dm_site_id = 3901410)
--         OR
--         (s.dm_gateway_id = 338010 AND dgg.gateway_test_id = 6072510 AND s.dm_site_id = 3092910)
--         OR
--         (s.dm_gateway_id = 338110 AND dgg.gateway_test_id = 6072610 AND s.dm_site_id = 3903410)
--         OR
--         (s.dm_gateway_id = 414710 AND dgg.gateway_test_id = 6072710 AND s.dm_site_id = 3902910)
--         OR
--         (s.dm_gateway_id = 414810 AND dgg.gateway_test_id = 6072810 AND s.dm_site_id = 3903410)
--         OR
--         (s.dm_gateway_id = 1193610 AND dgg.gateway_test_id = 6056010 AND s.dm_site_id = 3908110)
--         OR
--         (s.dm_gateway_id = 1193710 AND dgg.gateway_test_id = 6056110 AND s.dm_site_id = 3908210)
--         OR
--         (s.dm_gateway_id = 1299410 AND dgg.gateway_test_id = 6062510 AND s.dm_site_id = 3900210)
--         OR
--         (s.dm_gateway_id = 1299510 AND dgg.gateway_test_id = 6062610 AND s.dm_site_id = 3901410)
--         OR
--         (s.dm_gateway_id = 1297110 AND dgg.gateway_test_id = 6118510 AND s.dm_site_id = 3947410)
--         OR
--         (s.dm_gateway_id = 1297210 AND dgg.gateway_test_id = 6118810 AND s.dm_site_id = 3947610)
--     )
--   AND CAST(s.session_local_datetime AS DATE) >= '2021-08-24'
--   AND s.membership_state = 'Prospect';


-- JF Segment Membership Price Test (HIGHEST LEVEL - incl 70/BOGO)
CREATE OR REPLACE TEMPORARY TABLE _jfus_2021_membership_price_test AS
SELECT pds.customer_id
     , ac.vip_cohort
     , 'JFNA 2021 Membership Price Test' AS custom_segment_category
     , 'Test'                            AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2021-10-26'
                  AND ac.store_brand_name = 'JustFab'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 49.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2021-10-26'
  AND pds.business_unit = 'JUSTFAB'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'BOGOANYSTYLE' OR pds.promo_code_2 = 'BOGOANYSTYLE' OR pds.promo_code_1 = 'LEADDAY1270OFF' OR
       pds.promo_code_2 = 'LEADDAY1270OFF')

UNION

SELECT pds.customer_id
     , ac.vip_cohort
     , 'JFNA 2021 Membership Price Test' AS custom_segment_category
     , 'Control'                         AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2021-10-26'
                  AND ac.store_brand_name = 'JustFab'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 39.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2021-10-26'
  AND pds.business_unit = 'JUSTFAB'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'BOGOANYSTYLE' OR pds.promo_code_2 = 'BOGOANYSTYLE' OR pds.promo_code_1 = 'LEADDAY1270OFF' OR
       pds.promo_code_2 = 'LEADDAY1270OFF');


-- JF Segment Membership Price Test - 70% off only
CREATE OR REPLACE TEMPORARY TABLE _jfus_2021_membership_price_test_70 AS
SELECT pds.customer_id
     , ac.vip_cohort
     , 'JFNA 2021 Membership Price Test - 70% off' AS custom_segment_category
     , 'Test'                                      AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2021-10-26'
                  AND ac.store_brand_name = 'JustFab'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 49.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2021-10-26'
  AND pds.business_unit = 'JUSTFAB'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'LEADDAY1270OFF' OR pds.promo_code_2 = 'LEADDAY1270OFF')

UNION

SELECT pds.customer_id
     , ac.vip_cohort
     , 'JFNA 2021 Membership Price Test - 70% off' AS custom_segment_category
     , 'Control'                                   AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2021-10-26'
                  AND ac.store_brand_name = 'JustFab'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 39.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2021-10-26'
  AND pds.business_unit = 'JUSTFAB'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'LEADDAY1270OFF' OR pds.promo_code_2 = 'LEADDAY1270OFF');


-- JF Segment Membership Price Test - BOGO only
CREATE OR REPLACE TEMPORARY TABLE _jfus_2021_membership_price_test_bogo AS
SELECT pds.customer_id
     , ac.vip_cohort
     , 'JFNA 2021 Membership Price Test - BOGO' AS custom_segment_category
     , 'Test'                                   AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2021-10-26'
                  AND ac.store_brand_name = 'JustFab'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 49.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2021-10-26'
  AND pds.business_unit = 'JUSTFAB'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'BOGOANYSTYLE' OR pds.promo_code_2 = 'BOGOANYSTYLE')

UNION

SELECT pds.customer_id
     , ac.vip_cohort
     , 'JFNA 2021 Membership Price Test - BOGO' AS custom_segment_category
     , 'Control'                                AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2021-10-26'
                  AND ac.store_brand_name = 'JustFab'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 39.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2021-10-26'
  AND pds.business_unit = 'JUSTFAB'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'BOGOANYSTYLE' OR pds.promo_code_2 = 'BOGOANYSTYLE');


-- SD Segment Membership Price Test (HIGHEST LEVEL - incl 1from10/70off)
CREATE OR REPLACE TEMPORARY TABLE _sdus_2021_membership_price_test AS
SELECT pds.customer_id
     , ac.vip_cohort
     , 'SDNA 2021 Membership Price Test' AS custom_segment_category
     , 'Test'                            AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2021-11-11'
                  AND ac.store_brand_name = 'ShoeDazzle'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 49.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2021-11-11'
  AND pds.business_unit = 'SHOEDAZZLE'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'POSTREG_LEAD_75OFF' OR pds.promo_code_2 = 'POSTREG_LEAD_75OFF'
    OR pds.promo_code_1 = 'MASTER75OFFSITEWIDE' OR pds.promo_code_2 = 'MASTER75OFFSITEWIDE'
    OR pds.promo_code_1 = '70OFFD1D2' OR pds.promo_code_2 = '70OFFD1D2')

UNION

SELECT pds.customer_id
     , ac.vip_cohort
     , 'SDNA 2021 Membership Price Test' AS custom_segment_category
     , 'Control'                         AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2021-11-11'
                  AND ac.store_brand_name = 'ShoeDazzle'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 39.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2021-11-11'
  AND pds.business_unit = 'SHOEDAZZLE'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'POSTREG_LEAD_75OFF' OR pds.promo_code_2 = 'POSTREG_LEAD_75OFF'
    OR pds.promo_code_1 = 'MASTER75OFFSITEWIDE' OR pds.promo_code_2 = 'MASTER75OFFSITEWIDE'
    OR pds.promo_code_1 = '70OFFD1D2' OR pds.promo_code_2 = '70OFFD1D2');


-- SD Segment Membership Price Test - 70% off only
CREATE OR REPLACE TEMPORARY TABLE _sdus_2021_membership_price_test_70 AS
SELECT pds.customer_id
     , ac.vip_cohort
     , 'SDNA 2021 Membership Price Test - 70off' AS custom_segment_category
     , 'Test'                                    AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2021-11-11'
                  AND ac.store_brand_name = 'ShoeDazzle'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 49.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2021-11-11'
  AND pds.business_unit = 'SHOEDAZZLE'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'MASTER75OFFSITEWIDE' OR pds.promo_code_2 = 'MASTER75OFFSITEWIDE'
    OR pds.promo_code_1 = '70OFFD1D2' OR pds.promo_code_2 = '70OFFD1D2')

UNION

SELECT pds.customer_id
     , ac.vip_cohort
     , 'SDNA 2021 Membership Price Test - 70off' AS custom_segment_category
     , 'Control'                                 AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2021-11-11'
                  AND ac.store_brand_name = 'ShoeDazzle'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 39.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2021-11-11'
  AND pds.business_unit = 'SHOEDAZZLE'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'MASTER75OFFSITEWIDE' OR pds.promo_code_2 = 'MASTER75OFFSITEWIDE'
    OR pds.promo_code_1 = '70OFFD1D2' OR pds.promo_code_2 = '70OFFD1D2');


-- SD Segment Membership Price Test - 1from10 only
CREATE OR REPLACE TEMPORARY TABLE _sdus_2021_membership_price_test_1from10 AS
SELECT pds.customer_id
     , ac.vip_cohort
     , 'SDNA 2021 Membership Price Test - 1from10' AS custom_segment_category
     , 'Test'                                      AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2021-11-11'
                  AND ac.store_brand_name = 'ShoeDazzle'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 49.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2021-11-11'
  AND pds.business_unit = 'SHOEDAZZLE'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'POSTREG_LEAD_75OFF' OR pds.promo_code_2 = 'POSTREG_LEAD_75OFF')

UNION

SELECT pds.customer_id
     , ac.vip_cohort
     , 'SDNA 2021 Membership Price Test - 1from10' AS custom_segment_category
     , 'Control'                                   AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2021-11-11'
                  AND ac.store_brand_name = 'ShoeDazzle'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 39.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2021-11-11'
  AND pds.business_unit = 'SHOEDAZZLE'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'POSTREG_LEAD_75OFF' OR pds.promo_code_2 = 'POSTREG_LEAD_75OFF');


-- JF NA Membership Price Test Re-Launch starting 1-13-22
CREATE OR REPLACE TEMPORARY TABLE _jfus_2022_membership_price_test AS
SELECT pds.customer_id
     , ac.vip_cohort
     , 'JFNA 2022 Membership Price Test' AS custom_segment_category
     , 'Test'                            AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2022-01-13'
                  AND ac.store_brand_name = 'JustFab'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 49.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2022-01-13'
  AND pds.business_unit = 'JUSTFAB'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'LEADDAY1ALLBOOTS10' OR pds.promo_code_2 = 'LEADDAY1ALLBOOTS10')

UNION

SELECT pds.customer_id
     , ac.vip_cohort
     , 'JFNA 2022 Membership Price Test' AS custom_segment_category
     , 'Control'                         AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2022-01-13'
                  AND ac.store_brand_name = 'JustFab'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 39.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2022-01-13'
  AND pds.business_unit = 'JUSTFAB'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'LEADDAY1ALLBOOTS10' OR pds.promo_code_2 = 'LEADDAY1ALLBOOTS10');


-- SD NA Membership Price Test Re-Launch starting 1-19-22
CREATE OR REPLACE TEMPORARY TABLE _sdus_2022_membership_price_test AS
SELECT pds.customer_id
     , ac.vip_cohort
     , 'SDNA 2022 Membership Price Test' AS custom_segment_category
     , 'Test'                            AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2022-01-19'
                  AND ac.store_brand_name = 'ShoeDazzle'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 49.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2022-01-19'
  AND pds.business_unit = 'SHOEDAZZLE'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'POSTREG_LEAD_75OFF' OR pds.promo_code_2 = 'POSTREG_LEAD_75OFF')

UNION

SELECT pds.customer_id
     , ac.vip_cohort
     , 'SDNA 2022 Membership Price Test' AS custom_segment_category
     , 'Control'                         AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2022-01-19'
                  AND ac.store_brand_name = 'ShoeDazzle'
                  AND ac.store_region_abbr = 'NA'
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
                  AND m.price = 39.95
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2022-01-19'
  AND pds.business_unit = 'SHOEDAZZLE'
  AND pds.region = 'NA'
  AND (pds.promo_code_1 = 'POSTREG_LEAD_75OFF' OR pds.promo_code_2 = 'POSTREG_LEAD_75OFF');


-- JFEU New Offer Test starting 1/25/2022
CREATE OR REPLACE TEMPORARY TABLE _jfeu_2022_new_offer_test AS
SELECT pds.customer_id
     , ac.vip_cohort
     , 'JFEU 2022 New Offer Test' AS custom_segment_category
     , 'Test'                     AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2022-01-25'
                  AND ac.store_brand_name = 'JustFab'
                  AND ac.store_region_abbr = 'EU'
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2022-01-25'
  AND pds.business_unit = 'JUSTFAB'
  AND pds.region = 'EU'
  AND (pds.promo_code_1 = 'LEADDAY1ALLBOOTS15' OR pds.promo_code_2 = 'LEADDAY1ALLBOOTS15')

UNION

SELECT pds.customer_id
     , ac.vip_cohort
     , 'JFEU 2022 New Offer Test' AS custom_segment_category
     , 'Control'                  AS custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
                  AND ac.first_activation_date >= '2022-01-25'
                  AND ac.store_brand_name = 'JustFab'
                  AND ac.store_region_abbr = 'EU'
WHERE pds.order_type = 'vip activating'
  AND pds.order_date >= '2022-01-25'
  AND pds.business_unit = 'JUSTFAB'
  AND pds.region = 'EU'
  AND (pds.promo_code_1 = 'LEADDAY1ALLBOOTS10' OR pds.promo_code_2 = 'LEADDAY1ALLBOOTS10');


-- FK NA Membership Price Test starting 3-08-22
--CREATE OR REPLACE TEMPORARY TABLE _fk_2022_new_offer_test AS
--SELECT edw_prod.stg.udf_unconcat_brand(s.customer_id) AS customer_id
--     , ac.vip_cohort
--     , 'FK 2022 Membership Price Test'                AS custom_segment_category
--     , 'Test'                                         AS custom_segment
--FROM reporting_base_prod.shared.session s
--         LEFT JOIN reporting_base_prod.shared.dim_gateway_test_site dgg
--                   ON dgg.dm_gateway_test_site_id = s.dm_gateway_test_site_id
--                       AND dgg.effective_start_datetime <= s.session_local_datetime
--                       AND dgg.effective_end_datetime > s.session_local_datetime
--         JOIN _all_customers ac
--              ON ac.customer_id = edw_prod.stg.udf_unconcat_brand(s.customer_id)
--                  AND ac.first_activation_date >= '2022-03-08'
--                  AND ac.first_activation_date < '2022-06-28'
--         JOIN lake_jfb_view.ultra_merchant.membership m
--              ON m.customer_id = ac.customer_id
--WHERE (
--        (s.dm_gateway_id = 1196610 AND dgg.gateway_test_id = 6423410 AND s.dm_site_id = 4197210)
--        OR
--        (s.dm_gateway_id = 1195710 AND dgg.gateway_test_id = 6423310 AND s.dm_site_id = 4197310)
--    )
--  AND CAST(s.session_local_datetime AS DATE) >= '2022-03-08'
--  AND s.membership_state = 'Prospect'
--
--UNION
--
--SELECT edw_prod.stg.udf_unconcat_brand(s.customer_id) AS customer_id
--     , ac.vip_cohort
--     , 'FK 2022 Membership Price Test'                AS custom_segment_category
--     , 'Control'                                      AS custom_segment
--FROM reporting_base_prod.shared.session s
--         LEFT JOIN reporting_base_prod.shared.dim_gateway_test_site dgg
--                   ON dgg.dm_gateway_test_site_id = s.dm_gateway_test_site_id
--                       AND dgg.effective_start_datetime <= s.session_local_datetime
--                       AND dgg.effective_end_datetime > s.session_local_datetime
--         JOIN _all_customers ac
--              ON ac.customer_id = edw_prod.stg.udf_unconcat_brand(s.customer_id)
--                  AND ac.first_activation_date >= '2022-03-08'
--                  AND ac.first_activation_date < '2022-06-28'
--         JOIN lake_jfb_view.ultra_merchant.membership m
--              ON m.customer_id = ac.customer_id
--WHERE (
--        (s.dm_gateway_id = 1196610 AND dgg.gateway_test_id = 6423410 AND s.dm_site_id = 4197110)
--        OR
--        (s.dm_gateway_id = 1195710 AND dgg.gateway_test_id = 6423310 AND s.dm_site_id = 4177110)
--    )
--  AND CAST(s.session_local_datetime AS DATE) >= '2022-03-08'
--  AND s.membership_state = 'Prospect';


CREATE OR REPLACE TEMPORARY TABLE _jfb_2022_membership_price AS
SELECT dv.customer_id
     , dv.vip_cohort
     , 'JFB 2022 Membership Price' AS custom_segment_category
     , 'TY 49.95'                  AS custom_segment
FROM _all_customers dv
WHERE dv.membership_price IN (49.95)
  AND dv.store_brand_name IN ('JustFab')
  AND dv.store_region_abbr = 'NA'
  AND dv.first_activation_date >= '2022-01-13'
--     and dv.FIRST_ACTIVATION_DATE < '2022-04-01'

UNION

SELECT dv.customer_id
     , dv.vip_cohort
     , 'JFB 2022 Membership Price' AS custom_segment_category
     , 'TY 49.95'                  AS custom_segment
FROM _all_customers dv
WHERE dv.membership_price IN (49.95)
  AND dv.store_brand_name IN ('ShoeDazzle')
  AND dv.store_region_abbr = 'NA'
  AND dv.first_activation_date >= '2022-01-19'
--     and dv.FIRST_ACTIVATION_DATE < '2022-04-01'

UNION

SELECT dv.customer_id
     , dv.vip_cohort
     , 'JFB 2022 Membership Price' AS custom_segment_category
     , (CASE
            WHEN dv.membership_price = 39.95 THEN '39.95'
            WHEN dv.membership_price = 49.95 THEN '49.95'
    END)                           AS custom_segment
FROM _all_customers dv
WHERE dv.membership_price IN (39.95, 49.95)
  AND dv.store_brand_name IN ('FabKids')
  AND dv.store_region_abbr = 'NA'
  AND dv.first_activation_date >= '2022-03-08'

UNION

SELECT dv.customer_id
     , (CASE
            WHEN dv.vip_cohort >= '2021-01-01' AND dv.vip_cohort < '2022-01-01'
                THEN DATEADD(YEAR, 1, dv.vip_cohort)
            ELSE dv.vip_cohort END) AS vip_cohort
     , 'JFB 2022 Membership Price'  AS custom_segment_category
     , 'LY 39.95'                   AS custom_segment
FROM _all_customers dv
WHERE dv.store_brand_name IN ('JustFab')
  AND dv.store_region_abbr = 'NA'
  AND dv.first_activation_date >= '2021-01-13'
  AND dv.first_activation_date < '2022-01-01'
  AND dv.membership_price = 39.95

UNION

SELECT dv.customer_id
     , (CASE
            WHEN dv.vip_cohort >= '2021-01-01' AND dv.vip_cohort < '2022-01-01'
                THEN DATEADD(YEAR, 1, dv.vip_cohort)
            ELSE dv.vip_cohort END) AS vip_cohort
     , 'JFB 2022 Membership Price'  AS custom_segment_category
     , 'LY 39.95'                   AS custom_segment
FROM _all_customers dv
WHERE dv.store_brand_name IN ('ShoeDazzle')
  AND dv.store_region_abbr = 'NA'
  AND dv.first_activation_date >= '2021-01-19'
  AND dv.first_activation_date < '2022-01-01'
  AND dv.membership_price = 39.95;


--CREATE OR REPLACE TEMPORARY TABLE _sdna_2021_new_offer_test AS
--SELECT edw_prod.stg.udf_unconcat_brand(s.customer_id) AS customer_id
--     , ac.vip_cohort
--     , 'SDNA 2021 New Offer Test'                     AS custom_segment_category
--     , 'Test'                                         AS custom_segment
--FROM reporting_base_prod.shared.session s
--         LEFT JOIN reporting_base_prod.shared.dim_gateway_test_site dgg
--                   ON dgg.dm_gateway_test_site_id = s.dm_gateway_test_site_id
--                       AND dgg.effective_start_datetime <= s.session_local_datetime
--                       AND dgg.effective_end_datetime > s.session_local_datetime
--         JOIN _all_customers ac
--              ON ac.customer_id = edw_prod.stg.udf_unconcat_brand(s.customer_id)
--                  AND ac.first_activation_date >= '2021-09-08'
--                  AND ac.first_activation_date < '2021-09-29'
--                  AND ac.store_brand_name = 'ShoeDazzle'
--                  AND ac.store_region_abbr = 'NA'
--WHERE (
--        (s.dm_gateway_id = 1299010 AND dgg.gateway_test_id = 6075210 AND s.dm_site_id = 3919510)
--        OR
--        (s.dm_gateway_id = 1299110 AND dgg.gateway_test_id = 6086910 AND s.dm_site_id = 3919710)
--        OR
--        (s.dm_gateway_id = 1299610 AND dgg.gateway_test_id = 6075410 AND s.dm_site_id = 3919410)
--        OR
--        (s.dm_gateway_id = 1299710 AND dgg.gateway_test_id = 6075510 AND s.dm_site_id = 3919610)
--        OR
--        (s.dm_gateway_id = 337910 AND dgg.gateway_test_id = 6094110 AND s.dm_site_id = 3920010)
--        OR
--        (s.dm_gateway_id = 337610 AND dgg.gateway_test_id = 6094210 AND s.dm_site_id = 3920110)
--        OR
--        (s.dm_gateway_id = 424310 AND dgg.gateway_test_id = 6094310 AND s.dm_site_id = 3920010)
--        OR
--        (s.dm_gateway_id = 405810 AND dgg.gateway_test_id = 6094410 AND s.dm_site_id = 3920110)
--    )
--  AND CAST(s.session_local_datetime AS DATE) >= '2021-09-08'
--  AND CAST(s.session_local_datetime AS DATE) < '2021-09-29'
--  AND s.membership_state = 'Prospect'
--
--UNION
--
--SELECT edw_prod.stg.udf_unconcat_brand(s.customer_id) AS customer_id
--     , ac.vip_cohort
--     , 'SDNA 2021 New Offer Test'                     AS custom_segment_category
--     , 'Control'                                      AS custom_segment
--FROM reporting_base_prod.shared.session s
--         LEFT JOIN reporting_base_prod.shared.dim_gateway_test_site dgg
--                   ON dgg.dm_gateway_test_site_id = s.dm_gateway_test_site_id
--                       AND dgg.effective_start_datetime <= s.session_local_datetime
--                       AND dgg.effective_end_datetime > s.session_local_datetime
--         JOIN _all_customers ac
--              ON ac.customer_id = edw_prod.stg.udf_unconcat_brand(s.customer_id)
--                  AND ac.first_activation_date >= '2021-09-08'
--                  AND ac.first_activation_date < '2021-09-29'
--                  AND ac.store_brand_name = 'ShoeDazzle'
--                  AND ac.store_region_abbr = 'NA'
--WHERE (
--        (s.dm_gateway_id = 1299010 AND dgg.gateway_test_id = 6075210 AND s.dm_site_id = 3908510)
--        OR
--        (s.dm_gateway_id = 1299110 AND dgg.gateway_test_id = 6086910 AND s.dm_site_id = 3908610)
--        OR
--        (s.dm_gateway_id = 1299610 AND dgg.gateway_test_id = 6075410 AND s.dm_site_id = 3908510)
--        OR
--        (s.dm_gateway_id = 1299710 AND dgg.gateway_test_id = 6075510 AND s.dm_site_id = 3908610)
--        OR
--        (s.dm_gateway_id = 337910 AND dgg.gateway_test_id = 6094110 AND s.dm_site_id = 3908910)
--        OR
--        (s.dm_gateway_id = 337610 AND dgg.gateway_test_id = 6094210 AND s.dm_site_id = 3905710)
--        OR
--        (s.dm_gateway_id = 424310 AND dgg.gateway_test_id = 6094310 AND s.dm_site_id = 3908910)
--        OR
--        (s.dm_gateway_id = 405810 AND dgg.gateway_test_id = 6094410 AND s.dm_site_id = 3905710)
--        OR
--        (s.dm_gateway_id = 1299210 AND dgg.gateway_test_id = 6084810 AND s.dm_site_id = 3919810)
--        OR
--        (s.dm_gateway_id = 1299310 AND dgg.gateway_test_id = 6087010 AND s.dm_site_id = 3919910)
--    )
--  AND CAST(s.session_local_datetime AS DATE) >= '2021-09-08'
--  AND CAST(s.session_local_datetime AS DATE) < '2021-09-29'
--  AND s.membership_state = 'Prospect';


CREATE OR REPLACE TEMPORARY TABLE _return_exchange_customer AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Return/Exchange Customer' AS custom_segment_category
     , (CASE
            WHEN a.customer_id IS NOT NULL THEN 'Yes'
            ELSE 'No' END)        AS custom_segment
FROM _all_customers ac
         LEFT JOIN
     (
         SELECT olp.customer_id
         FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
         WHERE olp.order_classification = 'exchange'
           AND olp.order_date >= $start_date

         UNION

         SELECT olp.customer_id
         FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
         WHERE olp.order_classification = 'product order'
           AND olp.return_date >= $start_date
     ) a ON a.customer_id = ac.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _friend_refer AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Friend Referral'   AS custom_segment_category
     , (CASE
            WHEN dc.is_friend_referral = 1 THEN 'Yes'
            ELSE 'No' END) AS custom_segment
FROM _all_customers ac
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = ac.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _purchase_fk_on_jf AS
SELECT a.customer_id
     , a.vip_cohort
     , a.custom_segment_category
     , (CASE
            WHEN a.purchased_fk_flag = 1 THEN 'Purchased'
            ELSE 'Not Purchased' END) AS custom_segment
FROM (
         SELECT DISTINCT ac.customer_id
                       , ac.vip_cohort
                       , 'Purchased FK on JF' AS custom_segment_category

                       , MAX(CASE
                                 WHEN mdp.department LIKE '%FABKIDS%'
                                     THEN 1
                                 ELSE 0 END)  AS purchased_fk_flag
         FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
                  JOIN reporting_prod.gfb.merch_dim_product mdp
                       ON mdp.business_unit = olp.business_unit
                           AND mdp.region = olp.region
                           AND mdp.country = olp.country
                           AND mdp.product_sku = olp.product_sku
                           AND mdp.business_unit = 'JUSTFAB'
                  JOIN _all_customers ac
                       ON ac.customer_id = olp.customer_id
         WHERE olp.order_classification = 'product order'
           AND olp.business_unit = 'JUSTFAB'
           AND olp.order_date >= $start_date
         GROUP BY ac.customer_id
                , ac.vip_cohort
     ) a;

-- FK Passwordless Test from 9-2022 to 11-2022
-- CREATE OR REPLACE TEMPORARY TABLE _passwordless_test AS
-- SELECT fa.customer_id
--     ,
--        CASE WHEN ab_test_segment = 1 THEN 'Control' WHEN ab_test_segment = 2 THEN 'Variant' END AS test_group,
--        DATE_TRUNC('MONTH', fa.source_activation_local_datetime::DATE)                           AS cohort
-- FROM reporting_base_prod.shared.session_ab_test_start AS s
--          JOIN reporting_base_prod.shared.session AS n ON n.session_id = s.session_id
--          JOIN edw_prod.data_model_jfb.fact_registration AS fr
--               ON fr.session_id = s.meta_original_session_id
--          JOIN edw_prod.data_model_jfb.fact_activation AS fa
--               ON fa.customer_id = edw_prod.stg.udf_unconcat_brand(s.customer_id)
-- WHERE s.ab_test_key IN ('PasswordlessRegFKKI3242_v7', 'PasswordlessRegFKKI3242_v3', 'PasswordlessRegFKKI3242_v10')
--   AND test_start_membership_state = 'Prospect'
--   AND fa.source_activation_local_datetime >= registration_local_datetime
--   AND is_bot = FALSE
--   AND is_test_customer_account = FALSE;

-- CREATE OR REPLACE TEMPORARY TABLE _passwordless_test_group AS
-- SELECT ac.customer_id,
--        ac.vip_cohort,
--        'FK 2022 Passwordless Test' AS custom_segment_category,
--        pt.test_group               AS custom_segment
-- FROM _passwordless_test pt
--          JOIN _all_customers ac ON pt.customer_id = ac.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _snoozed_vip AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'Snoozed VIP'                    AS customer_segment_category
     , (CASE
            WHEN sn.customer_id IS NOT NULL THEN 'Snoozed VIP'
            ELSE 'Not Snoozed VIP' END) AS custom_segment
FROM _all_customers ac
         LEFT JOIN
     (
         SELECT DISTINCT vd.customer_id
         FROM _all_customers vd
                  JOIN lake_jfb_view.ultra_merchant.membership m
                       ON m.customer_id = vd.customer_id
                  JOIN lake_jfb_view.ultra_merchant.membership_snooze ms
                       ON ms.membership_id = m.membership_id
         WHERE vd.first_activation_date >= '2020-05-01' -- we had snooze as an offer before then, but we don’t want to mix those in with what we’re trying to measure
     ) sn ON sn.customer_id = ac.customer_id
WHERE ac.first_activation_date >= '2020-05-01';

CREATE OR REPLACE TEMPORARY TABLE _intro_offer_vips AS
SELECT pds.customer_id,
       ac.vip_cohort,
       'VIPs through Intro Offers' AS custom_segment_category,
       CASE
           WHEN promo_1_promotion_name ILIKE '%75\\%%' ESCAPE '\\'
               OR promo_1_promotion_code ILIKE '%75\\%%' ESCAPE '\\' THEN '75% off'
           WHEN (promo_1_promotion_name ILIKE ANY ('%$%10%', '%10%$%')
               OR promo_1_promotion_code ILIKE ANY ('%$%10%', '%10%$%'))
               AND
                (NOT promo_1_promotion_name ILIKE ANY ('%OFF%', '%CREDIT%') OR (promo_1_promotion_name ILIKE '%OFFER%'))
               AND NOT promo_1_promotion_name ILIKE '%BIRTHDAY%'
               AND promo_1_promotion_code NOT ILIKE '%ENDOWMENT%' THEN '10$ shoes'
           WHEN promo_1_promotion_code = 'GENSITEWIDE75OFF'
               OR promo_1_promotion_code = 'MASTER40OFFSITEWIDEB' THEN '50% off'
           WHEN promo_1_promotion_name ILIKE ANY ('%free%trial%', '%100\\%%') ESCAPE '\\'
               OR promo_1_promotion_code ILIKE ANY ('%free%trial%', '%100\\%%') ESCAPE '\\' THEN 'free trial'
           END                     AS _custom_segment
FROM reporting_prod.gfb.gfb011_promo_data_set pds
         JOIN _all_customers ac
              ON ac.customer_id = pds.customer_id
WHERE (promo_1_promotion_name ILIKE ANY ('%75\\%%', '%$%10%', '%10%$%', '%50\\%%', '%free%trial%', '%100\\%%') ESCAPE
       '\\'
           OR
       promo_1_promotion_code ILIKE ANY ('%75\\%%', '%$%10%', '%10%$%', '%50\\%%', '%free%trial%', '%100\\%%') ESCAPE
       '\\')
  AND order_type ILIKE 'vip activating'
  AND pds.region = 'NA'
  AND _custom_segment IS NOT NULL
  AND ac.vip_cohort >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _lead_registration_type AS
SELECT edw_prod.stg.udf_unconcat_brand(s.customer_id)                                   AS customer_id
     , ac.vip_cohort
     , 'Lead Registration Type'                                                         AS custom_segment_category
     , (CASE
            WHEN s.is_quiz_registration_action = TRUE THEN 'Quiz Lead'
            WHEN s.is_skip_quiz_registration_action = TRUE THEN 'Skip Quiz Lead'
            WHEN s.is_speedy_registration_action = TRUE THEN 'Speedy Sign-Up Lead' END) AS custom_segment
FROM reporting_base_prod.shared.session s
         JOIN _all_customers ac ON edw_prod.stg.udf_unconcat_brand(s.customer_id) = ac.customer_id AND
                                   ac.vip_cohort >= DATE_TRUNC(MONTH, s.session_local_datetime::DATE)
WHERE ac.store_region_abbr = 'NA'
  AND s.is_bot = FALSE
  AND s.is_test_customer_account = FALSE
  AND s.session_local_datetime >= $start_date
  AND (s.is_quiz_registration_action = TRUE OR s.is_skip_quiz_registration_action = TRUE OR
       s.is_speedy_registration_action = TRUE);

CREATE OR REPLACE TEMPORARY TABLE _fk_sd_lead_vip_migration AS
SELECT ac.customer_id
     , ac.vip_cohort
     , 'SD 2025 VIP Migration' AS custom_segment_category
     , CASE
           WHEN md.name ILIKE 'SD_VIP_migration_control' THEN 'SD VIP Migration Control'
           WHEN md.name ILIKE 'SD_VIP_migration_variant' THEN 'SD VIP Migration Variant'
    END                        AS custom_segment
FROM lake_jfb_view.ultra_merchant.membership_detail md
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.membership_id = md.membership_id
         JOIN _all_customers ac
              ON ac.customer_id = m.customer_id
WHERE md.name ILIKE 'SD_VIP_migration%'

UNION

SELECT ac.customer_id
     , ac.vip_cohort
     , 'SD 2025 Lead Migration' AS custom_segment_category
     , CASE
           WHEN md.name ILIKE 'SD_LEAD_migration_control' THEN 'SD Lead Migration Control'
           WHEN md.name ILIKE 'SD_LEAD_migration_variant' THEN 'SD Lead Migration Variant'
    END                         AS custom_segment
FROM lake_jfb_view.ultra_merchant.membership_detail md
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.membership_id = md.membership_id
         JOIN _all_customers ac
              ON ac.customer_id = m.customer_id
WHERE md.name ILIKE 'SD_LEAD_migration%'

UNION

SELECT ac.customer_id
     , ac.vip_cohort
     , 'FK 2025 VIP Migration' AS custom_segment_category
     , CASE
           WHEN md.name ILIKE 'FK_VIP_migration_control' THEN 'FK VIP Migration Control'
           WHEN md.name ILIKE 'FK_VIP_migration_variant' THEN 'FK VIP Migration Variant'
    END                        AS custom_segment
FROM lake_jfb_view.ultra_merchant.membership_detail md
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.membership_id = md.membership_id
         JOIN _all_customers ac
              ON ac.customer_id = m.customer_id
WHERE md.name ILIKE 'FK_VIP_migration%'

UNION

SELECT ac.customer_id
     , ac.vip_cohort
     , 'FK 2025 Lead Migration' AS custom_segment_category
     , CASE
           WHEN md.name ILIKE 'FK_LEAD_migration_control' THEN 'FK Lead Migration Control'
           WHEN md.name ILIKE 'FK_LEAD_migration_variant' THEN 'FK Lead Migration Variant'
    END                         AS custom_segment
FROM lake_jfb_view.ultra_merchant.membership_detail md
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.membership_id = md.membership_id
         JOIN _all_customers ac
              ON ac.customer_id = m.customer_id
WHERE md.name ILIKE 'FK_Lead_migration%';

CREATE OR REPLACE TEMPORARY TABLE _JF_2025_acquisition_offers AS
SELECT DISTINCT pd.customer_id
    , ac.vip_cohort
    , 'JF 2025 Acquisition Offers' AS custom_segment_category
    , CASE
            WHEN pd.promo_code_by_order ILIKE ANY ('%LEADDAY1ALLBOOTS10NEW%','%SHOES10MEN%','%SHOES10GIRLS%','%SHOES10BOYS%') then '$10 Shoes'
            WHEN (pd.promo_code_by_order ILIKE ANY ('%75OFF%')) then '75% Off'
       END AS custom_segment
FROM gfb.gfb011_promo_data_set pd
         JOIN _all_customers ac
              ON ac.customer_id = pd.customer_id
WHERE pd.business_unit = 'JUSTFAB'
    AND pd.region = 'NA'
    AND pd.order_type = 'vip activating'
    AND pd.promo_code_by_order ILIKE ANY ('%75OFF%','%LEADDAY1ALLBOOTS10NEW%','%SHOES10MEN%','%SHOES10GIRLS%','%SHOES10BOYS%')
    AND NOT(pd.promo_code_by_order ILIKE ANY ('%D8_75OFF%', '%D37_75OFF%','%FIRSTPAIR10DAY8%','GENSITEWIDE75OFF'))
    AND pd.order_date >= '2025-04-01';

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb001_customer_seg AS
SELECT ac.customer_id
     , ac.vip_cohort
     , ac.custom_segment_category
     , ac.custom_segment
FROM _all_customers ac

UNION

SELECT *
FROM _prepaid_credit_card

UNION
SELECT *
FROM _afterpay_vip

UNION

SELECT *
FROM _free_trial

UNION

SELECT *
FROM _vip_repeat_order

UNION

SELECT *
FROM _membership_price

UNION

SELECT *
FROM _gamers

UNION

SELECT *
FROM _customer_state

UNION

SELECT *
FROM _outstanding_membership_credit

UNION

SELECT *
FROM _activating_product_deparment

UNION

SELECT *
FROM _current_membership_state

UNION

SELECT ac.customer_id
     , ac.vip_cohort
     , 'VIP Tenure'                                              AS custom_segment_category
     , (CASE
            WHEN ac.current_vip_tenure > 12 THEN '13+'
            ELSE CAST(ac.current_vip_tenure AS VARCHAR(10)) END) AS custom_segment
FROM _all_customers ac
WHERE ac.current_vip_tenure > 0

UNION

SELECT ac.customer_id
     , ac.vip_cohort
     , 'Lead Tenure'                                             AS custom_segment_category
     , (CASE
            WHEN ac.lead_tenure > 7 AND ac.lead_tenure < 16 THEN 'D8-D15'
            WHEN ac.lead_tenure >= 16 AND ac.lead_tenure < 31 THEN 'D16-D30'
            WHEN ac.lead_tenure >= 31 AND ac.lead_tenure < 61 THEN 'M2'
            WHEN ac.lead_tenure >= 61 AND ac.lead_tenure < 91 THEN 'M3'
            WHEN ac.lead_tenure >= 91 AND ac.lead_tenure < 121 THEN 'M4'
            WHEN ac.lead_tenure >= 121 AND ac.lead_tenure < 151 THEN 'M5'
            WHEN ac.lead_tenure >= 151 THEN 'M6+'
            ELSE 'D' || CAST(ac.lead_tenure AS VARCHAR(10)) END) AS custom_segment
FROM _all_customers ac
WHERE ac.lead_tenure > 0

UNION

SELECT *
FROM _cross_brand_vip

UNION

SELECT *
FROM _opt_out_vip

UNION

SELECT *
FROM _mobile_app_vip

UNION

SELECT *
FROM _apparel_plus_vip

UNION

SELECT *
FROM _customers_with_session

UNION

SELECT *
FROM _single_item_activating

-- union
--
-- select
--     *
-- from _jfeu_crm_email_list

UNION

SELECT *
FROM _customer_bucket_segment

UNION

SELECT *
FROM _offer_test_lookalike_2021

-- UNION

-- SELECT *
-- FROM _jfna_2021_new_offer_test

UNION

SELECT *
FROM _jfus_2021_membership_price_test

UNION

SELECT *
FROM _jfus_2021_membership_price_test_70

UNION

SELECT *
FROM _jfus_2021_membership_price_test_bogo

UNION

SELECT *
FROM _sdus_2021_membership_price_test

UNION

SELECT *
FROM _sdus_2021_membership_price_test_70

UNION

SELECT *
FROM _sdus_2021_membership_price_test_1from10

UNION

SELECT *
FROM _jfus_2022_membership_price_test

UNION

SELECT *
FROM _sdus_2022_membership_price_test

UNION

SELECT *
FROM _jfeu_2022_new_offer_test

--UNION
--
--SELECT *
--FROM _fk_2022_new_offer_test

UNION

SELECT *
FROM _jfb_2022_membership_price

--UNION
--
--SELECT *
--FROM _sdna_2021_new_offer_test

UNION

SELECT *
FROM _aged_lead_promo

UNION

SELECT *
FROM _return_exchange_customer

UNION

SELECT *
FROM _friend_refer

UNION

SELECT *
FROM _purchase_fk_on_jf

-- UNION

-- SELECT *
-- FROM _passwordless_test_group

UNION

SELECT *
FROM _snoozed_vip

UNION

SELECT a.customer_id
     , a.vip_cohort
     , 'SMS Optin'        AS customer_segment_category
     , a.sms_optin_status AS custom_segment
FROM _all_customers a

UNION

SELECT *
FROM _emp_new_vs_converted_vips

UNION

SELECT *
FROM _intro_offer_vips

UNION

SELECT *
FROM _garment_weekender_bag_vips

UNION

SELECT *
FROM _lead_registration_type

UNION

SELECT *
FROM _activating_sub_brand

UNION

SELECT *
FROM _fk_sd_lead_vip_migration

UNION

SELECT ac.customer_id
     , ac.vip_cohort
     , 'All VIPs by Brand' AS custom_segment_category
     , (CASE
            WHEN store_brand_name = 'JustFab' THEN 'JUSTFAB'
            WHEN store_brand_name = 'ShoeDazzle' THEN 'SHOEDAZZLE'
            WHEN store_brand_name = 'FabKids' THEN 'FABKIDS' END) AS custom_segment
FROM _all_customers ac

UNION

SELECT *
FROM _JF_2025_acquisition_offers;;

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb001_customer_seg_count AS
SELECT UPPER(ac.store_brand_name)     AS store_brand_name
     , UPPER(ac.store_country_abbr)   AS store_country_abbr
     , UPPER(ac.store_region_abbr)    AS store_region_abbr
     , ac.vip_cohort
     , cs.custom_segment_category
     , cs.custom_segment
     , gamers.custom_segment          AS is_gamer_flag
     , ppcc.custom_segment            AS is_ppcc_activating
     , ac.is_active
     , COUNT(DISTINCT cs.customer_id) AS customer_count
FROM reporting_prod.gfb.gfb001_customer_seg cs
         JOIN _all_customers ac
              ON ac.customer_id = cs.customer_id
         LEFT JOIN reporting_prod.gfb.gfb001_customer_seg gamers
                   ON gamers.customer_id = cs.customer_id
                       AND gamers.custom_segment_category = 'Gamers'
         LEFT JOIN reporting_prod.gfb.gfb001_customer_seg ppcc
                   ON ppcc.customer_id = cs.customer_id
                       AND ppcc.custom_segment_category = 'Prepaid Credit Card'
WHERE cs.custom_segment_category NOT IN ('Gamers')
GROUP BY UPPER(ac.store_brand_name)
       , UPPER(ac.store_country_abbr)
       , UPPER(ac.store_region_abbr)
       , ac.vip_cohort
       , cs.custom_segment_category
       , cs.custom_segment
       , gamers.custom_segment
       , ppcc.custom_segment
       , ac.is_active;
