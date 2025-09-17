SET full_refresh_date = DATEADD(YEAR, -2, DATE_TRUNC(YEAR, CURRENT_DATE()));
SET incremental_refresh_date = DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_DATE()));
SET start_date = IFF(DAYOFWEEK(CURRENT_DATE) = 0, $full_refresh_date, $incremental_refresh_date);
SET update_datetime = CURRENT_TIMESTAMP;

CREATE OR REPLACE TEMPORARY TABLE _store AS
SELECT st.store_id,
       st.store_brand,
       st.store_region,
       st.store_country,
       st.store_brand_abbr
FROM edw_prod.data_model_jfb.dim_store st
WHERE st.store_brand_abbr IN ('JF', 'SD', 'FK')
  AND st.store_full_name NOT LIKE '%Wholesale%'
  AND st.store_full_name NOT LIKE '%Heels.com%'
  AND st.store_full_name NOT LIKE '%Retail%'
  AND st.store_full_name NOT LIKE '%Sample%'
  AND st.store_full_name NOT LIKE '%SWAG%'
  AND st.store_full_name NOT LIKE '%PS%';

CREATE OR REPLACE TEMPORARY TABLE _session_data AS
SELECT edw_prod.stg.udf_unconcat_brand(se.customer_id) AS customer_id,
       se.meta_original_session_id                     AS session_id,
       s.store_brand,
       s.store_region,
       (CASE
            WHEN dc.specialty_country_code = 'GB' THEN 'UK'
            WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
            ELSE s.store_country END)                  AS store_country,
       se.utm_source,
       se.utm_medium,
       se.utm_content,
       se.utm_campaign,
       dc.how_did_you_hear,
       se.session_local_datetime::DATE                 AS session_date
FROM reporting_base_prod.shared.session se
     JOIN _store s
          ON se.store_id = s.store_id
     LEFT JOIN edw_prod.data_model_jfb.dim_customer dc
               ON edw_prod.stg.udf_unconcat_brand(se.customer_id) = dc.customer_id
WHERE utm_source IS NOT NULL
  AND utm_medium IS NOT NULL
  AND is_bot = FALSE
  AND se.is_in_segment=TRUE
  AND DATEADD(DAY, 1, session_local_datetime::DATE) >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _order_info AS
SELECT ol.business_unit                              AS business_unit,
       ol.region                                     AS region,
       ol.country                                    AS country,
       ol.product_sku,
       ol.sku,
       ol.order_date                                 AS order_date,
       ol.clearance_flag                             AS clearance_flag,
       ol.clearance_price                            AS clearance_price,
       ol.customer_id                                AS order_customer_id,
       ol.session_id                                 AS order_session_id,
       ol.dp_size,
       --Sales
       COUNT(DISTINCT ol.order_id)                   AS order_count,
       SUM(ol.total_qty_sold)                        AS total_qty_sold,
       SUM(ol.total_product_revenue)                 AS total_product_revenue,
       SUM(ol.total_cogs)                            AS total_cogs,

       SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                           AS activating_product_revenue,
       SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_qty_sold
               ELSE 0 END)                           AS activating_qty_sold,
       SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                           AS activating_cogs,
       COUNT(DISTINCT (CASE
                           WHEN ol.order_type = 'vip activating'
                               THEN ol.order_id END))
                                                     AS activating_order_count,
       SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                           AS repeat_product_revenue,
       SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_qty_sold
               ELSE 0 END)                           AS repeat_qty_sold,
       SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                           AS repeat_cogs,
       COUNT(DISTINCT (CASE
                           WHEN ol.order_type != 'vip activating'
                               THEN ol.order_id END))
                                                     AS repeat_order_count,
       SUM(CASE
               WHEN ol.clearance_flag = 'regular' THEN ol.total_discount
               ELSE COALESCE(mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount,
                             ol.total_discount) END) AS total_discount,
       SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' THEN ol.total_discount
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag != 'regular' THEN COALESCE(
                               mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount,
                               ol.total_discount)
               ELSE 0 END)                           AS activating_discount,
       SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' THEN ol.total_discount
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag != 'regular' THEN COALESCE(
                               mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount,
                               ol.total_discount)
               ELSE 0 END)                           AS repeat_discount,
       SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_product_revenue
               ELSE 0 END)                           AS lead_product_revenue,
       SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_qty_sold
               ELSE 0 END)                           AS lead_qty_sold,
       SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_cogs
               ELSE 0 END)                           AS lead_cogs
FROM gfb.gfb_order_line_data_set_place_date ol
     LEFT JOIN gfb.merch_dim_product mdp
               ON mdp.business_unit = ol.business_unit
                   AND mdp.region = ol.region
                   AND mdp.country = ol.country
                   AND mdp.product_sku = ol.product_sku
WHERE ol.order_classification = 'product order'
  AND ol.order_date >= $start_date
GROUP BY ol.business_unit,
         ol.region,
         ol.country,
         ol.product_sku,
         ol.sku,
         ol.dp_size,
         ol.order_date,
         ol.clearance_flag,
         ol.clearance_price,
         ol.session_id,
         ol.customer_id,
         ol.session_id;

CREATE OR REPLACE TEMPORARY TABLE _sales_info_place_date AS
SELECT COALESCE(UPPER(se.store_brand), ol.business_unit) AS business_unit,
       COALESCE(se.store_region, ol.region)              AS region,
       COALESCE(se.store_country, ol.country)            AS country,
       ol.product_sku,
       ol.sku,
       ol.clearance_flag,
       ol.clearance_price,
       ol.dp_size,
       se.utm_medium,
       se.utm_source,
       se.utm_campaign,
       se.utm_content,
       se.how_did_you_hear,
       COALESCE(se.session_date, ol.order_date)          AS date,
       COUNT(DISTINCT se.session_id)                     AS session_count,
       SUM(ol.order_count)                               AS order_count,
       SUM(ol.total_qty_sold)                            AS total_qty_sold,
       SUM(ol.total_product_revenue)                     AS total_product_revenue,
       SUM(ol.total_cogs)                                AS total_cogs,
       SUM(ol.total_discount)                            AS total_discount,
       SUM(ol.activating_product_revenue)                AS activating_product_revenue,
       SUM(ol.activating_cogs)                           AS activating_cogs,
       SUM(ol.activating_qty_sold)                       AS activating_qty_sold,
       SUM(ol.activating_order_count)                    AS activating_order_count,
       SUM(ol.activating_discount)                       AS activating_discount,
       SUM(ol.repeat_product_revenue)                    AS repeat_product_revenue,
       SUM(ol.repeat_cogs)                               AS repeat_cogs,
       SUM(ol.repeat_qty_sold)                           AS repeat_qty_sold,
       SUM(ol.repeat_order_count)                        AS repeat_order_count,
       SUM(ol.repeat_discount)                           AS repeat_discount,
       SUM(ol.lead_product_revenue)                      AS lead_product_revenue,
       SUM(ol.lead_cogs)                                 AS lead_cogs,
       SUM(ol.lead_qty_sold)                             AS lead_qty_sold
FROM _session_data se
     FULL JOIN
     _order_info AS ol
     ON se.session_id = ol.order_session_id
         AND se.customer_id = ol.order_customer_id
GROUP BY COALESCE(UPPER(se.store_brand), ol.business_unit),
         COALESCE(se.store_region, ol.region),
         COALESCE(se.store_country, ol.country),
         ol.product_sku,
         ol.sku,
         ol.clearance_flag,
         ol.clearance_price,
         ol.dp_size,
         se.utm_medium,
         se.utm_source,
         se.utm_campaign,
         se.utm_content,
         se.how_did_you_hear,
         COALESCE(se.session_date, ol.order_date);

DELETE
FROM gfb.gfb069_fk_social_report
WHERE IFF(DAYOFWEEK(CURRENT_DATE) = 0, TRUE, date >= $start_date);

INSERT INTO gfb.gfb069_fk_social_report
SELECT DISTINCT mdp.*,
                main_data.date,
                main_data.business_unit AS business_unit_for_join,
                main_data.region        AS region_for_join,
                main_data.country       AS country_for_join,
                main_data.clearance_flag,
                main_data.clearance_price,
                main_data.sku,
                main_data.dp_size,
                main_data.utm_source,
                main_data.utm_medium,
                main_data.utm_campaign,
                main_data.utm_content,
                main_data.how_did_you_hear,
                main_data.total_qty_sold,
                main_data.total_product_revenue,
                main_data.total_cogs,
                main_data.activating_product_revenue,
                main_data.activating_qty_sold,
                main_data.activating_cogs,
                main_data.repeat_product_revenue,
                main_data.repeat_qty_sold,
                main_data.repeat_cogs,
                main_data.total_discount,
                main_data.activating_discount,
                main_data.repeat_discount,
                main_data.lead_qty_sold,
                main_data.lead_product_revenue,
                main_data.lead_cogs,
                main_data.order_count,
                main_data.activating_order_count,
                main_data.repeat_order_count,
                main_data.session_count,
                $update_datetime        AS update_datetime
FROM _sales_info_place_date main_data
     LEFT JOIN gfb.merch_dim_product mdp
               ON mdp.business_unit = main_data.business_unit
                   AND mdp.region = main_data.region
                   AND mdp.country = main_data.country
                   AND mdp.product_sku = main_data.product_sku
WHERE main_data.date >= $start_date;
