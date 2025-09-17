SET start_date = CAST('2019-01-01' AS DATE);
SET end_date = CURRENT_DATE();


CREATE OR REPLACE TEMPORARY TABLE _store AS
SELECT *
FROM edw_prod.data_model_jfb.dim_store st
WHERE st.store_brand_abbr IN ('JF', 'SD')
  AND st.store_full_name NOT LIKE '%(DM)%'
  AND st.store_full_name NOT LIKE '%Wholesale%'
  AND st.store_full_name NOT LIKE '%Heels.com%'
  AND st.store_full_name NOT LIKE '%Retail%'
  AND st.store_full_name NOT LIKE '%Sample%'
  AND st.store_full_name NOT LIKE '%SWAG%'
  AND st.store_full_name NOT LIKE '%PS%';


CREATE OR REPLACE TEMPORARY TABLE _membership_status AS
SELECT DISTINCT fme.customer_id
              , fme.membership_state
FROM edw_prod.data_model_jfb.fact_membership_event fme
         JOIN _store st
              ON st.store_id = fme.store_id
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = fme.customer_id
                  AND dc.is_test_customer = 0
WHERE fme.is_current = 1;


CREATE OR REPLACE TEMPORARY TABLE _waitlist AS
SELECT st.store_brand                         AS business_unit
     , st.store_region                        AS region
     , (CASE
            WHEN dc.specialty_country_code = 'GB' THEN 'UK'
            WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
            ELSE UPPER(st.store_country) END) AS country
     , dp.product_sku
     , dp.sku
     , mdp.department
     , mdp.department_detail
     , mdp.subcategory
     , mdp.color
     , psm.clean_size                         AS size
     , mdp.image_url
     , mdp.large_img_url
     , wl.membership_product_wait_list_id
     , dc.customer_id
     , wl.product_id
     , wlt.label                              AS wait_list_type
     , sc.label                               AS wait_list_status
     , CAST(wl.datetime_added AS DATE)        AS waitlist_date
     , mdp.collection
     , mdp.latest_launch_date
     , mdp.previous_launch_date
     , mdp.og_retail
     , mdp.current_vip_retail
     , mdp.style_name
     , mdp.qty_pending                        AS qty_on_order
     , ms.membership_state
     , mdp.waitlist_with_known_eta_flag
     , mdp.waitlist_start_datetime
     , mdp.waitlist_end_datetime
FROM lake_jfb_view.ultra_merchant.membership_product_wait_list wl
         JOIN lake_jfb_view.ultra_merchant.membership_product_wait_list_type wlt
              ON wlt.membership_product_wait_list_type_id = wl.membership_product_wait_list_type_id
         JOIN lake_jfb_view.ultra_merchant.statuscode sc
              ON sc.statuscode = wl.statuscode
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.membership_id = wl.membership_id
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = m.customer_id
                  AND dc.is_test_customer = 0
         JOIN _store st
              ON st.store_id = dc.store_id
         JOIN edw_prod.data_model_jfb.dim_product dp
              ON dp.product_id = wl.product_id
         LEFT JOIN reporting_prod.gfb.merch_dim_product mdp
                   ON LOWER(mdp.business_unit) = LOWER(st.store_brand)
                       AND LOWER(mdp.region) = LOWER(st.store_region)
                       AND LOWER(mdp.country) = LOWER(st.store_country)
                       AND mdp.product_sku = dp.product_sku
         LEFT JOIN reporting_prod.gfb.view_product_size_mapping psm
                   ON psm.product_sku = dp.product_sku
                       AND LOWER(psm.region) = LOWER(st.store_region)
                       AND LOWER(psm.size) = LOWER(dp.size)
         JOIN _membership_status ms
              ON ms.customer_id = dc.customer_id
WHERE sc.label != 'Cancelled'
  AND CAST(wl.datetime_added AS DATE) >= $start_date
  AND CAST(wl.datetime_added AS DATE) < $end_date;


CREATE OR REPLACE TEMPORARY TABLE _waitlist_auto_order AS
SELECT DISTINCT w.membership_product_wait_list_id
              , wlo.order_id
              , ols.ship_date
FROM _waitlist w
         JOIN lake_jfb_view.ultra_merchant.membership_product_wait_list_order_log wlo
              ON wlo.membership_product_wait_list_id = w.membership_product_wait_list_id
         LEFT JOIN reporting_prod.gfb.gfb_order_line_data_set_ship_date ols
                   ON ols.order_id = wlo.order_id
WHERE w.wait_list_type = 'Auto Order';


CREATE OR REPLACE TEMPORARY TABLE _waitlist_email_order AS
SELECT DISTINCT a.membership_product_wait_list_id
              , a.order_id
              , a.ship_date
FROM (
         SELECT w.membership_product_wait_list_id
              , olp.order_id
              , ols.ship_date
              , RANK() OVER (PARTITION BY olp.order_id ORDER BY olp.order_date ASC) AS order_rank
         FROM _waitlist w
                  JOIN reporting_prod.gfb.gfb_order_line_data_set_place_date olp
                       ON olp.product_id = w.product_id
                           AND olp.customer_id = w.customer_id
                           AND olp.order_date > w.waitlist_date
                  LEFT JOIN reporting_prod.gfb.gfb_order_line_data_set_ship_date ols
                            ON ols.order_line_id = olp.order_line_id
         WHERE w.wait_list_type = 'Email'
           AND olp.order_classification = 'product order'
     ) a
WHERE a.order_rank = 1;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb018_waitlist_data_set AS
SELECT w.*

     , (CASE
            WHEN COALESCE(wao.membership_product_wait_list_id, weo.membership_product_wait_list_id) IS NULL
                THEN 'Not Purchased'
            ELSE 'Purchased' END)             AS is_purchased
     , COALESCE(wao.ship_date, weo.ship_date) AS ship_date
FROM _waitlist w
         LEFT JOIN _waitlist_auto_order wao
                   ON wao.membership_product_wait_list_id = w.membership_product_wait_list_id
         LEFT JOIN _waitlist_email_order weo
                   ON weo.membership_product_wait_list_id = w.membership_product_wait_list_id
