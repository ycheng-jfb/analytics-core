SET start_date = DATEADD(YEAR, -2, DATE_TRUNC(YEAR, CURRENT_DATE()));
SET end_date = CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _sales_info_place_date AS
SELECT ol.business_unit                                         AS business_unit,
       mdp.sub_brand,
       ol.region                                                AS region,
       ol.bundle_product_id,
       ol.order_date                                            AS order_date,
       ol.product_sku, --Sales
       COUNT(DISTINCT ol.bundle_order_line_id)                  AS bundle_unit_sold,
       SUM(ol.total_product_revenue)                            AS total_product_revenue,
       SUM(ol.total_cogs)                                       AS total_cogs,
       SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS activating_product_revenue,
       COUNT(DISTINCT CASE
                          WHEN ol.order_type = 'vip activating'
                              THEN ol.bundle_order_line_id END) AS activating_bundle_unit_sold,
       SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS activating_cogs,
       SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS repeat_product_revenue,
       COUNT(DISTINCT CASE
                          WHEN ol.order_type != 'vip activating'
                              THEN ol.bundle_order_line_id
           END)                                                 AS repeat_bundle_unit_sold,
       SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS repeat_cogs,
       SUM(ol.total_discount)                                   AS total_discount,
       SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_discount
               ELSE 0 END)                                      AS activating_discount,
       SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_discount
               ELSE 0 END)                                      AS repeat_discount,
       SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS lead_product_revenue,
       COUNT(DISTINCT CASE
                          WHEN ol.order_type = 'ecom'
                              THEN ol.bundle_order_line_id
           END)                                                 AS lead_bundle_unit_sold,
       SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS lead_cogs
FROM gfb.gfb_order_line_data_set_place_date ol
    LEFT JOIN gfb.merch_dim_product mdp
        ON mdp.business_unit = ol.business_unit
            AND mdp.region = ol.region
            AND mdp.country = ol.country
            AND mdp.product_sku = ol.product_sku
WHERE ol.order_classification = 'product order'
  AND ol.order_date >= $start_date
  AND ol.order_date < $end_date
  AND ol.bundle_product_id IS NOT NULL
  AND ol.business_unit = 'FABKIDS'
GROUP BY ol.business_unit,
        mdp.sub_brand,
        ol.region,
        ol.bundle_product_id,
        ol.product_sku,
        ol.order_date;

CREATE OR REPLACE TEMPORARY TABLE _fk_bundle_info AS
SELECT DISTINCT business_unit,
                sub_brand,
                region,
                bundle_product_id,
                fk_bundle_site_name,
                fk_vip_retail,
                fk_active_inactive
FROM gfb.gfb_dim_bundle
WHERE business_unit = 'FABKIDS'
  AND fk_active_inactive IS NOT NULL;

CREATE OR REPLACE TEMPORARY TABLE _fk_bundle_sales AS
SELECT s.business_unit,
       s.sub_brand,
       s.region,
       s.product_sku,
       s.order_date,
       s.bundle_product_id                                                                                                      AS outfit,
       f.fk_bundle_site_name                                                                                                    AS outfit_name,
       f.fk_vip_retail                                                                                                          AS outfit_vip_retail,
       f.fk_active_inactive                                                                                                     AS outfit_active_inactive,
       s.bundle_unit_sold                                                                                                       AS outfit_bundle_unit_sold,
       s.total_product_revenue                                                                                                  AS outfit_total_product_revenue,
       s.total_cogs                                                                                                             AS outfit_total_cogs,
       s.repeat_bundle_unit_sold                                                                                                AS repeat_bundle_unit_sold,
       s.repeat_product_revenue                                                                                                 AS repeat_bundle_product_revenue,
       s.repeat_cogs                                                                                                            AS repeat_bundle_cogs,
       ROW_NUMBER() OVER (PARTITION BY s.business_unit, s.region, s.product_sku,s.order_date ORDER BY f.fk_active_inactive ASC) AS row_num
FROM _sales_info_place_date s
     JOIN _fk_bundle_info f
          ON f.business_unit = s.business_unit
              AND f.sub_brand = s.sub_brand
              AND f.region = s.region
              AND f.bundle_product_id = s.bundle_product_id;

CREATE OR REPLACE TEMPORARY TABLE _all_sales_info AS
SELECT mdsp.business_unit,
       mdsp.sub_brand,
       mdsp.region,
       mdsp.department_detail,
       mdsp.subcategory,
       mdsp.product_sku,
       mdsp.large_img_url,
       mdsp.gender,
       mdsp.style_name,
       mdsp.color,
       mdsp.date,
       mdsp.clearance_flag,
       mdsp.clearance_price,
       mdsp.current_vip_retail,
       mdsp.msrp,
       mdsp.show_room,
       SUM(mdsp.total_qty_sold)             AS total_qty_sold,
       SUM(mdsp.total_product_revenue)      AS total_product_revenue,
       SUM(mdsp.total_cogs)                 AS total_cogs,
       SUM(mdsp.repeat_qty_sold)            AS repeat_qty_sold,
       SUM(mdsp.repeat_product_revenue)     AS repeat_product_revenue,
       SUM(mdsp.repeat_cogs)                AS repeat_cogs,
       SUM(mdsp.activating_qty_sold)        AS activating_qty_sold,
       SUM(mdsp.activating_product_revenue) AS activating_product_revenue,
       SUM(mdsp.activating_cogs)            AS activating_cogs,
       SUM(mdsp.qty_onhand)                 AS qty_onhand,
       SUM(mdsp.qty_available_to_sell)      AS qty_available_to_sell
FROM gfb.dos_107_merch_data_set_by_place_date mdsp
WHERE business_unit = 'FABKIDS'
  AND date >= $start_date
  AND date < $end_date
GROUP BY mdsp.business_unit,
        mdsp.sub_brand,
        mdsp.region,
        mdsp.department_detail,
        mdsp.subcategory,
        mdsp.product_sku,
        mdsp.large_img_url,
        mdsp.gender,
        mdsp.style_name,
        mdsp.color,
        mdsp.date,
        mdsp.clearance_flag,
        mdsp.clearance_price,
        mdsp.current_vip_retail,
        mdsp.msrp,
        mdsp.show_room;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb062_fk_showroom_recap AS
SELECT DISTINCT mdsp.business_unit,
                mdsp.sub_brand,
                mdsp.region,
                mdsp.department_detail,
                mdsp.subcategory,
                mdsp.product_sku,
                mdsp.large_img_url,
                mdsp.gender,
                mdsp.style_name,
                mdsp.color,
                mdsp.date,
                mdsp.clearance_flag,
                mdsp.clearance_price,
                mdsp.current_vip_retail,
                mdsp.msrp,
                mdsp.show_room,
                mdsp.total_qty_sold,
                mdsp.total_product_revenue,
                mdsp.total_cogs,
                mdsp.repeat_qty_sold,
                mdsp.repeat_product_revenue,
                mdsp.repeat_cogs,
                mdsp.activating_qty_sold,
                mdsp.activating_product_revenue,
                mdsp.activating_cogs,
                mdsp.qty_onhand,
                mdsp.qty_available_to_sell,
                b.outfit,
                b.outfit_name,
                b.outfit_vip_retail,
                b.outfit_active_inactive,
                b.row_num,
                COALESCE(b.outfit_bundle_unit_sold, 0)       AS outfit_bundle_unit_sold,
                COALESCE(b.outfit_total_product_revenue, 0)  AS outfit_total_product_revenue,
                COALESCE(b.outfit_total_cogs, 0)             AS outfit_total_cogs,
                COALESCE(b.repeat_bundle_unit_sold, 0)       AS repeat_bundle_unit_sold,
                COALESCE(b.repeat_bundle_product_revenue, 0) AS repeat_bundle_product_revenue,
                COALESCE(b.repeat_bundle_cogs, 0)            AS repeat_bundle_cogs
FROM _all_sales_info mdsp
     LEFT JOIN _fk_bundle_sales b
               ON b.business_unit = mdsp.business_unit
                   AND b.sub_brand = mdsp.sub_brand
                   AND b.region = mdsp.region
                   AND b.product_sku = mdsp.product_sku
                   AND b.order_date = mdsp.date
WHERE mdsp.business_unit = 'FABKIDS';
