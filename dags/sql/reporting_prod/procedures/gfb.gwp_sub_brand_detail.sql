SET start_date = (SELECT DATEADD('day',-7,CURRENT_DATE()));

SET end_date = CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _membership_gwp_orders AS 
SELECT DISTINCT order_id
             ,olp.PRODUCT_SKU       AS GWP_product_sku
             ,olp.product_name      AS GWP_product_name
             ,dp.image_url          AS GWP_image_url
FROM gfb.gfb_order_line_data_set_place_date olp
JOIN edw_prod.data_model_jfb.dim_product dp
           ON dp.product_id = olp.product_id
WHERE olp.product_type_name = 'Membership GWP'
AND olp.order_date >= $start_date
AND olp.order_date < $end_date;

DELETE
FROM gfb.gwp_sub_brand_detail
WHERE order_date >= $start_date;

INSERT INTO gfb.gwp_sub_brand_detail
SELECT  olp.business_unit
    ,olp.region
    ,olp.country
    ,gp.GWP_product_sku
    ,gp.GWP_product_name
    ,gp.GWP_image_url
    ,olp.order_id
    ,olp.order_date
    ,SUM(CASE WHEN olp.product_type_name = 'Membership GWP' THEN 1 ELSE 0 END)                                                               AS GWP
    ,SUM(CASE WHEN mdp.sub_brand = 'JFB' THEN 1 ELSE 0 END)                                                                                  AS JFB
    ,SUM(CASE WHEN mdp.sub_brand IN ('DREAM PAIRS', 'DREAM PAIRS KIDS', 'NORTIV8', 'BRUNO MARC') THEN 1 ELSE 0 END)                          AS MM
    ,SUM(CASE WHEN mdp.sub_brand IN ('TIJN', 'HASHMERE', 'BAGSMART', 'OROLAY', 'A-DAM', 'POM PECHE', 'NEVER HAVE I EVER') THEN 1 ELSE 0 END) AS THIRD_PARTY
FROM gfb.gfb_order_line_data_set_place_date olp
JOIN _membership_gwp_orders gp ON gp.order_id = olp.order_id
JOIN gfb.merch_dim_product mdp 
    ON mdp.business_unit = olp.business_unit
    AND mdp.region = olp.region
    AND mdp.country = olp.country
    AND mdp.product_sku = olp.product_sku
WHERE olp.order_classification = 'product order'
    AND olp.order_type = 'vip repeat'
    AND olp.order_date >= $start_date
    AND olp.order_date < $end_date
GROUP BY  olp.business_unit
    ,olp.region
    ,olp.country
    ,gp.GWP_product_sku
    ,gp.GWP_product_name
    ,gp.GWP_image_url
    ,olp.order_id
    ,olp.order_date;
