SET start_date = CAST('2019-01-01' AS DATE);


CREATE OR REPLACE TEMPORARY TABLE _apparel_customer AS
SELECT DISTINCT fol.customer_id
FROM gfb.gfb_order_line_data_set_place_date fol
         JOIN gfb.merch_dim_product mdp
              ON mdp.business_unit = fol.business_unit
                  AND mdp.region = fol.region
                  AND mdp.country = fol.country
                  AND mdp.product_sku = fol.product_sku
WHERE fol.order_classification = 'product order'
  AND mdp.department LIKE '%APPAREL%'
  AND fol.order_date >= $start_date;


CREATE OR REPLACE TEMPORARY TABLE _final_data AS
SELECT a.store_brand_abbr
     , a.store_region_abbr
     , a.country
     , a.month_date
     , a.order_classification
     , a.department
     , a.subcategory
     , a.is_apparel_customer
     , b.department               AS purchase_together_department
     , b.subcategory              AS purchase_together_subcategory
     , COUNT(*)                   AS frequncy
     , COUNT(DISTINCT a.order_id) AS orders
FROM (
         SELECT DISTINCT fol.business_unit                 AS store_brand_abbr
                       , fol.region                        AS store_region_abbr
                       , fol.country                       AS country
                       , fol.order_id
                       , DATE_TRUNC(MONTH, fol.order_date) AS month_date
                       , (CASE
                              WHEN fol.order_type = 'vip activating' THEN 'VIP Activating Order'
                              ELSE 'Repeat Order' END)     AS order_classification
                       , mdp.department
                       , mdp.subcategory
                       , (CASE
                              WHEN ac.customer_id IS NOT NULL THEN 1
                              ELSE 0 END)                  AS is_apparel_customer
         FROM gfb.gfb_order_line_data_set_place_date fol
                  LEFT JOIN _apparel_customer ac
                            ON ac.customer_id = fol.customer_id
                  JOIN gfb.merch_dim_product mdp
                       ON mdp.business_unit = fol.business_unit
                           AND mdp.region = fol.region
                           AND mdp.country = fol.country
                           AND mdp.product_sku = fol.product_sku
         WHERE fol.order_classification = 'product order'
           AND fol.order_date >= $start_date
     ) a
         JOIN
     (
         SELECT DISTINCT fol.business_unit                 AS store_brand_abbr
                       , fol.region                        AS store_region_abbr
                       , fol.order_id
                       , DATE_TRUNC(MONTH, fol.order_date) AS month_date
                       , mdp.department
                       , mdp.subcategory
         FROM gfb.gfb_order_line_data_set_place_date fol
                  JOIN gfb.merch_dim_product mdp
                       ON mdp.business_unit = fol.business_unit
                           AND mdp.region = fol.region
                           AND mdp.country = fol.country
                           AND mdp.product_sku = fol.product_sku
         WHERE fol.order_classification = 'product order'
           AND fol.order_date >= $start_date
     ) b ON b.order_id = a.order_id
         AND (b.department != a.department OR b.subcategory != a.subcategory)
GROUP BY a.store_brand_abbr
       , a.store_region_abbr
       , a.country
       , a.month_date
       , a.order_classification
       , a.department
       , a.subcategory
       , a.is_apparel_customer
       , b.department
       , b.subcategory;


--remove duplicate
CREATE OR REPLACE TEMPORARY TABLE _desired_row AS
SELECT DISTINCT (CASE
                     WHEN a.row_num > b.row_num THEN b.row_num
                     ELSE a.row_num END) AS desired_row
FROM (
         SELECT *
              , ROW_NUMBER() OVER (ORDER BY t.store_brand_abbr, t.store_region_abbr,t.country, t.month_date, t.order_classification, t.subcategory, t.purchase_together_subcategory, t.is_apparel_customer, t.frequncy DESC) AS row_num
         FROM _final_data t
     ) a
         JOIN
     (
         SELECT *
              , ROW_NUMBER() OVER (ORDER BY t.store_brand_abbr, t.store_region_abbr,t.country, t.month_date, t.order_classification, t.subcategory, t.purchase_together_subcategory, t.is_apparel_customer, t.frequncy DESC) AS row_num
         FROM _final_data t
     ) b ON a.store_brand_abbr = b.store_brand_abbr
         AND a.store_region_abbr = b.store_region_abbr
         AND a.country = b.country
         AND a.month_date = b.month_date
         AND a.order_classification = b.order_classification
         AND a.department = b.purchase_together_department
         AND a.subcategory = b.purchase_together_subcategory
         AND a.purchase_together_department = b.department
         AND a.purchase_together_subcategory = b.subcategory
         AND a.is_apparel_customer = b.is_apparel_customer;


CREATE OR REPLACE TRANSIENT TABLE gfb.dos_104_merch_basket_analysis AS
SELECT a.*
     , CURRENT_DATE() AS last_update_date
FROM (
         SELECT *
              , ROW_NUMBER() OVER (ORDER BY t.store_brand_abbr, t.store_region_abbr,t.country, t.month_date, t.order_classification, t.subcategory, t.purchase_together_subcategory, t.is_apparel_customer, t.frequncy DESC) AS row_num
         FROM _final_data t
     ) a
WHERE a.row_num IN (SELECT dr.desired_row FROM _desired_row dr);
