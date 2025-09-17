set start_date = DATEADD(YEAR, -2, DATE_TRUNC(YEAR, CURRENT_DATE()));


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb082_01_mm_return_rate AS
SELECT COALESCE(a.sub_brand, b.sub_brand)                 AS sub_brand,
       COALESCE(a.department_detail, b.department_detail) AS department_detail,
       COALESCE(a.subcategory, b.subcategory)             AS subcategory,
       COALESCE(a.subclass, b.subclass)                   AS subclass,
       COALESCE(a.gender, b.gender)                       AS gender,
       COALESCE(a.product_sku, b.product_sku)             AS product_sku,
       COALESCE(a.style_name, b.style_name)               AS style_name,
       COALESCE(a.order_date, b.order_date)               AS order_date,
       COALESCE(a.order_type, b.order_type)               AS order_type,
       SUM(a.return_count)                                AS return_count,
       SUM(b.total_qty_sold)                              AS total_qty_sold
FROM (SELECT sub_brand,
             department_detail,
             subcategory,
             subclass,
             gender,
             product_sku,
             style_name,
             order_date,
             order_type,
             COUNT(DISTINCT order_line_id) AS return_count
      FROM reporting_prod.gfb.gfb082_mm_returns_data
      WHERE sub_brand != 'JFB'
      GROUP BY sub_brand,
             department_detail,
             subcategory,
             subclass,
             gender,
             product_sku,
             style_name,
             order_date,
             order_type) a
         FULL JOIN
     (SELECT mdp.sub_brand,
             mdp.department_detail,
             mdp.subcategory,
             mdp.subclass,
             mdp.gender,
             ol.product_sku,
             mdp.style_name,
             ol.order_date,
             IFF(ol.order_type != 'vip activating', 'Nonactivating', 'Activating') AS order_type,
             SUM(total_qty_sold)                                                   AS total_qty_sold
      FROM reporting_prod.gfb.gfb_order_line_data_set_place_date ol
               JOIN reporting_prod.gfb.merch_dim_product mdp
                    ON ol.business_unit = mdp.business_unit
                        AND ol.region = mdp.region
                        AND ol.country = mdp.country
                        AND ol.product_sku = mdp.product_sku
      WHERE mdp.sub_brand != 'JFB'
        and ol.order_date >= $start_date
      GROUP BY mdp.sub_brand,
             mdp.department_detail,
             mdp.subcategory,
             mdp.subclass,
             mdp.gender,
             ol.product_sku,
             mdp.style_name,
             ol.order_date,
             IFF(ol.order_type != 'vip activating', 'Nonactivating', 'Activating')) b
     ON a.sub_brand = b.sub_brand
         AND a.product_sku = b.product_sku
         AND a.style_name = b.style_name
         AND a.order_date = b.order_date
         AND a.order_type = b.order_type
GROUP BY COALESCE(a.sub_brand, b.sub_brand),
       COALESCE(a.department_detail, b.department_detail),
       COALESCE(a.subcategory, b.subcategory),
       COALESCE(a.subclass, b.subclass),
       COALESCE(a.gender, b.gender),
       COALESCE(a.product_sku, b.product_sku),
       COALESCE(a.style_name, b.style_name),
       COALESCE(a.order_date, b.order_date),
       COALESCE(a.order_type, b.order_type);
