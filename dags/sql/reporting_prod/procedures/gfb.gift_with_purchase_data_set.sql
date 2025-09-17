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

CREATE OR REPLACE TEMPORARY TABLE _orders AS
SELECT olp.business_unit
      ,olp.region
      ,olp.country
      ,gp.GWP_product_sku
      ,gp.GWP_product_name
      ,gp.GWP_image_url
      ,olp.order_id
      ,olp.order_date
      ,SUM(olp.total_qty_sold)                                                                                AS total_qty_sold
      ,SUM(olp.tokens_applied)                                                                                AS tokens_applied                                                                 
      ,SUM(olp.total_product_revenue)                                                                         AS total_product_revenue
      ,SUM(olp.cash_collected_amount)                                                                         AS cash_collected
      ,SUM(olp.total_cogs)                                                                                    AS total_cogs
      ,SUM(olp.total_shipping_revenue)                                                                        AS shipping_revenue
      ,SUM(olp.total_shipping_cost)                                                                           AS total_shipping_cost
      ,SUM(olp.total_product_revenue + olp.total_shipping_revenue - olp.total_cogs - olp.total_shipping_cost) AS total_margin
      ,SUM(olp.total_product_revenue - olp.total_cogs)                                                        AS product_margin
      ,SUM(olp.cash_collected_amount + olp.total_shipping_revenue - olp.total_cogs - olp.total_shipping_cost) AS cash_gross_margin
      ,SUM(olp.total_cash_credit_amount + olp.total_non_cash_credit_amount)   AS total_credit_redeemed_amount              
FROM gfb.gfb_order_line_data_set_place_date olp
         JOIN _membership_gwp_orders gp ON gp.order_id=olp.order_id
WHERE olp.order_classification = 'product order'
  AND olp.order_type = 'vip repeat' 
  AND olp.order_date >= $start_date
  AND olp.order_date < $end_date
  AND product_type_name<>'Membership GWP' 
GROUP BY olp.business_unit
      ,olp.region
      ,olp.country
      ,gp.GWP_product_sku
      ,gp.GWP_product_name
      ,gp.GWP_image_url
      ,olp.order_id
      ,olp.order_date;

DELETE
FROM gfb.gift_with_purchase_data_set
WHERE order_date >= $start_date;

INSERT INTO gfb.gift_with_purchase_data_set
SELECT  o.business_unit
      ,o.region
      ,o.country
      ,o.GWP_product_sku
      ,o.GWP_product_name
      ,o.GWP_image_url
      ,o.order_date
      ,(CASE WHEN o.total_qty_sold > 8 THEN '8+'
             ELSE CAST(o.total_qty_sold AS STRING) END)                                                     AS upt_bucket
      ,(CASE WHEN o.total_qty_sold > 1 THEN 'Orders With Multiple Items'
             ELSE 'Orders With Single Item' END)                                                              AS order_qty_filter 
      ,(CASE WHEN o.total_product_revenue <= 100 THEN 'Under $100'
             WHEN o.total_product_revenue <= 150 THEN '$101 to $150'
             WHEN o.total_product_revenue <= 200 THEN '$151 to $200'
             WHEN o.total_product_revenue <= 250 THEN '$201 to $250'
             ELSE '$250+' END)                                                                                 AS aov_bucket
      ,(CASE WHEN o.total_product_revenue + o.shipping_revenue <= 100 THEN 'Under $100'
             WHEN o.total_product_revenue + o.shipping_revenue <= 150 THEN '$101 to $150'
             WHEN o.total_product_revenue + o.shipping_revenue <= 200 THEN '$151 to $200'
             WHEN o.total_product_revenue + o.shipping_revenue <= 250 THEN '$201 to $250'
             ELSE '$250+' END)                                                                                 AS aov_bucket_with_shipping
      ,COUNT(DISTINCT o.order_id)                                                                             AS orders
      ,SUM(o.total_qty_sold)                                                                                AS unit_count
      ,SUM(o.tokens_applied)                                                                                AS tokens_applied                                                                 
      ,SUM(o.total_product_revenue)                                                                         AS total_product_revenue
      ,SUM(o.cash_collected)                                                                         AS cash_collected
      ,SUM(o.total_cogs)                                                                                    AS total_cogs
      ,SUM(o.shipping_revenue)                                                                        AS shipping_revenue
      ,SUM(o.total_shipping_cost)                                                                           AS total_shipping_cost
      ,SUM(total_margin)                                                                                    AS total_margin
      ,SUM(product_margin)                                                                                  AS product_margin
      ,SUM(cash_gross_margin)                                                                               AS cash_gross_margin
      ,SUM(total_credit_redeemed_amount)                                                                    AS total_credit_redeemed_amount              
FROM _orders o 
GROUP BY o.business_unit
      ,o.region
      ,o.country
      ,o.GWP_product_sku
      ,o.GWP_product_name
      ,o.GWP_image_url
      ,o.order_date
      ,(CASE WHEN o.total_qty_sold > 8 THEN '8+'
                     ELSE CAST(o.total_qty_sold AS STRING) END)                                                     
      ,(CASE WHEN o.total_qty_sold > 1 THEN 'Orders With Multiple Items'
             ELSE 'Orders With Single Item' END)                                                              
      ,(CASE WHEN o.total_product_revenue <= 100 THEN 'Under $100'
             WHEN o.total_product_revenue <= 150 THEN '$101 to $150'
             WHEN o.total_product_revenue <= 200 THEN '$151 to $200'
             WHEN o.total_product_revenue <= 250 THEN '$201 to $250'
             ELSE '$250+' END)                                                                                
      ,(CASE WHEN o.total_product_revenue + o.shipping_revenue <= 100 THEN 'Under $100'
             WHEN o.total_product_revenue + o.shipping_revenue <= 150 THEN '$101 to $150'
             WHEN o.total_product_revenue + o.shipping_revenue <= 200 THEN '$151 to $200'
             WHEN o.total_product_revenue + o.shipping_revenue <= 250 THEN '$201 to $250'
             ELSE '$250+' END);    
