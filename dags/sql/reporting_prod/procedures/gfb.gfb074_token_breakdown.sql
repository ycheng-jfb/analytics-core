SET full_refresh_date = DATEADD(YEAR, -3, DATE_TRUNC(YEAR, CURRENT_DATE()));
SET incremental_refresh_date = DATE_TRUNC(YEAR, CURRENT_DATE());
SET start_date = IFF(DAYOFWEEK(CURRENT_DATE) = 0, $full_refresh_date, $incremental_refresh_date);

CREATE OR REPLACE TEMPORARY TABLE _token_sales AS
SELECT 'Token Sales'                                                                  AS token_breakdown,
       ''                                                                             AS non_token_bundles,
       business_unit,
       sub_brand,
       region,
       country,
       department_detail,
       subcategory,
       subclass,
       gender,
       show_room,
       current_vip_retail,
       product_sku,
       date,
       SUM(token_qty_sold)                                                            AS total_qty_sold,
       SUM(token_product_revenue)                                                     AS total_product_revenue,
       SUM(token_product_revenue_with_tariff)                                         AS total_product_revenue_with_tariff,
       SUM(token_cogs)                                                                AS total_cogs,
       SUM(token_discount)                                                            AS total_discount,
       SUM(tokens_applied)                                                            AS tokens_applied,
       SUM(activating_token_qty_sold)                                                 AS activating_qty_sold,
       SUM(activating_token_product_revenue_with_tariff)                              AS activating_product_revenue,
       SUM(activating_token_cogs)                                                     AS activating_cogs,
       SUM(repeat_token_qty_sold)                                                     AS repeat_qty_sold,
       SUM(repeat_token_product_revenue_with_tariff)                                  AS repeat_product_revenue,
       SUM(repeat_token_cogs)                                                         AS repeat_cogs,
       SUM(token_shipping_revenue)                                                    AS total_shipping_revenue,
       SUM(token_shipping_cost)                                                       AS total_shipping_cost,
       SUM(activating_token_shipping_revenue)                                         AS activating_shipping_revenue,
       SUM(activating_token_shipping_cost)                                            AS activating_shipping_cost,
       SUM(repeat_token_shipping_revenue)                                             AS repeat_shipping_revenue,
       SUM(repeat_token_shipping_cost)                                                AS repeat_shipping_cost,
       SUM(CASE WHEN clearance_flag = 'clearance' THEN token_qty_sold END)            AS total_clearance_qty_sold,
       SUM(CASE WHEN clearance_flag = 'clearance' THEN activating_token_qty_sold END) AS activating_clearance_qty_sold,
       SUM(CASE WHEN clearance_flag = 'clearance' THEN repeat_token_qty_sold END)     AS repeat_clearance_qty_sold
FROM gfb.dos_107_merch_data_set_by_place_date
WHERE tokens_applied > 0
  AND date >= $start_date
GROUP BY token_breakdown,
         business_unit,
         sub_brand,
         region,
         country,
         department_detail,
         subcategory,
         subclass,
         gender,
         show_room,
         date,
         non_token_bundles,
         current_vip_retail,
         product_sku;

CREATE OR REPLACE TEMPORARY TABLE _two_for_one_sales AS
SELECT '2 for 1 Token'                                                                  AS token_breakdown,
       ''                                                                               AS non_token_bundles,
       business_unit,
       sub_brand,
       region,
       country,
       department_detail,
       subcategory,
       subclass,
       gender,
       show_room,
       current_vip_retail,
       product_sku,
       date,
       SUM(two_for_one_qty_sold)                                                        AS total_qty_sold,
       SUM(two_for_one_product_revenue)                                                 AS total_product_revenue,
       SUM(two_for_one_product_revenue_with_tariff)                                     AS total_product_revenue_with_tariff,
       SUM(two_for_one_cogs)                                                            AS total_cogs,
       SUM(two_for_one_discount)                                                        AS total_discount,
       SUM(two_for_one_applied)                                                         AS tokens_applied,
       SUM(activating_two_for_one_qty_sold)                                             AS activating_qty_sold,
       SUM(activating_two_for_one_product_revenue_with_tariff)                          AS activating_product_revenue,
       SUM(activating_two_for_one_cogs)                                                 AS activating_cogs,
       SUM(repeat_two_for_one_qty_sold)                                                 AS repeat_qty_sold,
       SUM(repeat_two_for_one_product_revenue_with_tariff)                              AS repeat_product_revenue,
       SUM(repeat_two_for_one_cogs)                                                     AS repeat_cogs,
       SUM(two_for_one_shipping_revenue)                                                AS total_shipping_revenue,
       SUM(two_for_one_shipping_cost)                                                   AS total_shipping_cost,
       SUM(activating_two_for_one_shipping_revenue)                                     AS activating_shipping_revenue,
       SUM(activating_two_for_one_shipping_cost)                                        AS activating_shipping_cost,
       SUM(repeat_two_for_one_shipping_revenue)                                         AS repeat_shipping_revenue,
       SUM(repeat_two_for_one_shipping_cost)                                            AS repeat_shipping_cost,
       SUM(CASE WHEN clearance_flag = 'clearance' THEN two_for_one_qty_sold END)        AS total_clearance_qty_sold,
       SUM(CASE
               WHEN clearance_flag = 'clearance'
                   THEN activating_two_for_one_qty_sold END)                            AS activating_clearance_qty_sold,
       SUM(CASE WHEN clearance_flag = 'clearance' THEN repeat_two_for_one_qty_sold END) AS repeat_clearance_qty_sold
FROM gfb.dos_107_merch_data_set_by_place_date
WHERE two_for_one_applied > 0
  AND date >= $start_date
GROUP BY token_breakdown,
         business_unit,
         sub_brand,
         region,
         country,
         department_detail,
         subcategory,
         subclass,
         gender,
         show_room,
         date,
         non_token_bundles,
         current_vip_retail,
         product_sku;

CREATE OR REPLACE TEMPORARY TABLE _pre_set_bundle_sales AS
SELECT 'Pre Set Bundles Token'                                                                 AS token_breakdown,
       ''                                                                                      AS non_token_bundles,
       business_unit,
       sub_brand,
       region,
       country,
       department_detail,
       subcategory,
       subclass,
       gender,
       show_room,
       current_vip_retail,
       product_sku,
       date,
       SUM(token_qty_sold) - SUM(two_for_one_qty_sold) -
       SUM(one_item_qty_sold) -
       SUM(three_for_one_qty_sold)                                                             AS total_qty_sold,
       SUM(token_product_revenue) - SUM(two_for_one_product_revenue) -
       SUM(one_item_product_revenue) -
       SUM(three_for_one_product_revenue)                                                      AS total_product_revenue,
       SUM(token_product_revenue_with_tariff) - SUM(two_for_one_product_revenue_with_tariff) -
       SUM(one_item_product_revenue_with_tariff) -
       SUM(three_for_one_product_revenue_with_tariff)                                          AS total_product_revenue_with_tariff,
       SUM(token_cogs) - SUM(two_for_one_cogs) - SUM(one_item_cogs) -
       SUM(three_for_one_cogs)                                                                 AS total_cogs,
       SUM(token_discount) - SUM(two_for_one_discount) -
       SUM(one_item_discount) -
       SUM(three_for_one_discount)                                                             AS total_discount,
       SUM(tokens_applied) - SUM(two_for_one_applied) - SUM(one_item_applied) -
       SUM(three_for_one_applied)                                                              AS tokens_applied,
       SUM(activating_token_qty_sold) - SUM(activating_two_for_one_qty_sold) -
       SUM(activating_one_item_qty_sold) -
       SUM(activating_three_for_one_qty_sold)                                                  AS activating_qty_sold,
       SUM(activating_token_product_revenue_with_tariff) - SUM(activating_two_for_one_product_revenue_with_tariff) -
       SUM(activating_one_item_product_revenue_with_tariff) -
       SUM(activating_three_for_one_product_revenue_with_tariff)                               AS activating_product_revenue,
       SUM(activating_token_cogs) - SUM(activating_two_for_one_cogs) -
       SUM(activating_one_item_cogs) -
       SUM(activating_three_for_one_cogs)                                                      AS activating_cogs,
       SUM(repeat_token_qty_sold) - SUM(repeat_two_for_one_qty_sold) -
       SUM(repeat_one_item_qty_sold) -
       SUM(repeat_three_for_one_qty_sold)                                                      AS repeat_qty_sold,
       SUM(repeat_token_product_revenue_with_tariff) - SUM(repeat_two_for_one_product_revenue_with_tariff) -
       SUM(repeat_one_item_product_revenue_with_tariff) -
       SUM(repeat_three_for_one_product_revenue_with_tariff)                                   AS repeat_product_revenue,
       SUM(repeat_token_cogs) - SUM(repeat_two_for_one_cogs) - SUM(repeat_one_item_cogs) -
       SUM(repeat_three_for_one_cogs)                                                          AS repeat_cogs,
       SUM(token_shipping_revenue) - SUM(two_for_one_shipping_revenue) -
       SUM(one_item_shipping_revenue) -
       SUM(three_for_one_shipping_revenue)                                                     AS total_shipping_revenue,
       SUM(token_shipping_cost) - SUM(two_for_one_shipping_cost) -
       SUM(one_item_shipping_cost) -
       SUM(three_for_one_shipping_cost)                                                        AS total_shipping_cost,
       SUM(activating_token_shipping_revenue) - SUM(activating_two_for_one_shipping_revenue) -
       SUM(activating_one_item_shipping_revenue) -
       SUM(activating_three_for_one_shipping_revenue)                                          AS activating_shipping_revenue,
       SUM(activating_token_shipping_cost) - SUM(activating_two_for_one_shipping_cost) -
       SUM(activating_one_item_shipping_cost) -
       SUM(activating_three_for_one_shipping_cost)                                             AS activating_shipping_cost,
       SUM(repeat_token_shipping_revenue) - SUM(repeat_two_for_one_shipping_revenue) -
       SUM(repeat_one_item_shipping_revenue) -
       SUM(repeat_three_for_one_shipping_revenue)                                              AS repeat_shipping_revenue,
       SUM(repeat_token_shipping_cost) - SUM(repeat_two_for_one_shipping_cost) -
       SUM(repeat_one_item_shipping_cost) -
       SUM(repeat_three_for_one_shipping_cost)                                                 AS repeat_shipping_cost,
       SUM(CASE
               WHEN clearance_flag = 'clearance' THEN (token_qty_sold - two_for_one_qty_sold - one_item_qty_sold -
                                                       three_for_one_qty_sold) END)            AS total_clearance_qty_sold,
       SUM(CASE
               WHEN clearance_flag = 'clearance' THEN (activating_token_qty_sold - activating_two_for_one_qty_sold -
                                                       activating_one_item_qty_sold -
                                                       activating_three_for_one_qty_sold) END) AS activating_clearance_qty_sold,
       SUM(CASE
               WHEN clearance_flag = 'clearance' THEN (repeat_token_qty_sold - repeat_two_for_one_qty_sold -
                                                       repeat_one_item_qty_sold -
                                                       repeat_three_for_one_qty_sold) END)     AS repeat_clearance_qty_sold
FROM gfb.dos_107_merch_data_set_by_place_date
WHERE tokens_applied > 0
  AND date >= $start_date
GROUP BY token_breakdown,
         business_unit,
         sub_brand,
         region,
         country,
         department_detail,
         subcategory,
         subclass,
         gender,
         show_room,
         date,
         non_token_bundles,
         current_vip_retail,
         product_sku;

CREATE OR REPLACE TEMPORARY TABLE _non_token_sales AS
SELECT 'Non Token'                                                         AS token_breakdown,
       CASE
           WHEN ol.bundle_product_id = '-1' OR ol.bundle_product_id IS NULL
               THEN 'Non Bundles'
           ELSE 'Bundle Sale'
           END                                                             AS non_token_bundles,
       ol.business_unit                                                    AS business_unit,
       mdp.sub_brand,
       ol.region                                                           AS region,
       ol.country                                                          AS country,
       mdp.department_detail,
       mdp.subcategory,
       mdp.subclass,
       mdp.gender,
       mdp.show_room,
       mdp.current_vip_retail,
       ol.product_sku,
       ol.order_date                                                       AS date,

       --Sales
       SUM(ol.total_qty_sold)                                              AS total_qty_sold,
       SUM(ol.total_product_revenue)                                       AS total_product_revenue,
       SUM(ol.total_product_revenue_with_tariff)                           AS total_product_revenue_with_tariff,
       SUM(ol.total_cogs)                                                  AS total_cogs,
       SUM(ol.total_discount)                                              AS total_discount,
       SUM(ol.tokens_applied)                                              AS tokens_applied,
       SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                 AS activating_qty_sold,
       SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                 AS activating_product_revenue,
       SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                                 AS activating_cogs,
       SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                 AS repeat_qty_sold,
       SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                 AS repeat_product_revenue,
       SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                                 AS repeat_cogs,
       SUM(total_shipping_revenue)                                         AS total_shipping_revenue,
       SUM(total_shipping_cost)                                            AS total_shipping_cost,
       SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN total_shipping_revenue
               ELSE 0 END)                                                 AS activating_shipping_revenue,
       SUM(CASE
               WHEN ol.order_type = 'vip activating' THEN
                   total_shipping_cost
               ELSE 0 END)                                                 AS activating_shipping_cost,
       SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN total_shipping_revenue
               ELSE 0 END)                                                 AS repeat_shipping_revenue,
       SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN total_shipping_cost
               ELSE 0 END)                                                 AS repeat_shipping_cost,
       SUM(CASE WHEN clearance_flag = 'clearance' THEN total_qty_sold END) AS total_clearance_qty_sold,
       SUM(CASE
               WHEN clearance_flag = 'clearance' AND ol.order_type = 'vip activating'
                   THEN total_qty_sold END)                                AS activating_clearance_qty_sold,
       SUM(CASE
               WHEN clearance_flag = 'clearance' AND ol.order_type != 'vip activating'
                   THEN total_qty_sold END)                                AS repeat_clearance_qty_sold
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN gfb.merch_dim_product mdp
              ON mdp.business_unit = ol.business_unit
                  AND mdp.region = ol.region
                  AND mdp.country = ol.country
                  AND mdp.product_sku = ol.product_sku
WHERE ol.order_classification = 'product order'
  AND ol.order_date >= $start_date
  AND ol.tokens_applied = 0
GROUP BY token_breakdown,
         ol.business_unit,
         mdp.sub_brand,
         ol.region,
         ol.country,
         mdp.show_room,
         mdp.subclass,
         mdp.gender,
         ol.order_date,
         mdp.department_detail,
         mdp.subcategory,
         non_token_bundles,
         mdp.current_vip_retail,
         ol.product_sku;

CREATE OR REPLACE TEMPORARY TABLE _one_item_token_sales AS
SELECT 'One Item Token'                                                              AS token_breakdown,
       ''                                                                            AS non_token_bundles,
       business_unit,
       sub_brand,
       region,
       country,
       department_detail,
       subcategory,
       subclass,
       gender,
       show_room,
       current_vip_retail,
       product_sku,
       date,
       SUM(one_item_qty_sold)                                                        AS total_qty_sold,
       SUM(one_item_product_revenue)                                                 AS total_product_revenue,
       SUM(one_item_product_revenue_with_tariff)                                     AS total_product_revenue_with_tariff,
       SUM(one_item_cogs)                                                            AS total_cogs,
       SUM(one_item_discount)                                                        AS total_discount,
       SUM(one_item_applied)                                                         AS tokens_applied,
       SUM(activating_one_item_qty_sold)                                             AS activating_qty_sold,
       SUM(activating_one_item_product_revenue_with_tariff)                          AS activating_product_revenue,
       SUM(activating_one_item_cogs)                                                 AS activating_cogs,
       SUM(repeat_one_item_qty_sold)                                                 AS repeat_qty_sold,
       SUM(repeat_one_item_product_revenue_with_tariff)                              AS repeat_product_revenue,
       SUM(repeat_one_item_cogs)                                                     AS repeat_cogs,
       SUM(one_item_shipping_revenue)                                                AS total_shipping_revenue,
       SUM(one_item_shipping_cost)                                                   AS total_shipping_cost,
       SUM(activating_one_item_shipping_revenue)                                     AS activating_shipping_revenue,
       SUM(activating_one_item_shipping_cost)                                        AS activating_shipping_cost,
       SUM(repeat_one_item_shipping_revenue)                                         AS repeat_shipping_revenue,
       SUM(repeat_one_item_shipping_cost)                                            AS repeat_shipping_cost,
       SUM(CASE WHEN clearance_flag = 'clearance' THEN one_item_qty_sold END)        AS total_clearance_qty_sold,
       SUM(CASE
               WHEN clearance_flag = 'clearance'
                   THEN activating_one_item_qty_sold END)                            AS activating_clearance_qty_sold,
       SUM(CASE WHEN clearance_flag = 'clearance' THEN repeat_one_item_qty_sold END) AS repeat_clearance_qty_sold
FROM gfb.dos_107_merch_data_set_by_place_date
WHERE date >= $start_date
  AND one_item_qty_sold > 0
GROUP BY token_breakdown,
         business_unit,
         sub_brand,
         region,
         country,
         department_detail,
         subcategory,
         subclass,
         gender,
         show_room,
         date,
         non_token_bundles,
         current_vip_retail,
         product_sku;

CREATE OR REPLACE TEMPORARY TABLE _three_for_one_sales AS
SELECT '3 for 1 Token'                                                             AS token_breakdown,
       ''                                                                          AS non_token_bundles,
       business_unit,
       sub_brand,
       region,
       country,
       department_detail,
       subcategory,
       subclass,
       gender,
       show_room,
       current_vip_retail,
       product_sku,
       date,
       SUM(three_for_one_qty_sold)                                                 AS total_qty_sold,
       SUM(three_for_one_product_revenue)                                          AS total_product_revenue,
       SUM(three_for_one_product_revenue_with_tariff)                              AS total_product_revenue_with_tariff,
       SUM(three_for_one_cogs)                                                     AS total_cogs,
       SUM(three_for_one_discount)                                                 AS total_discount,
       SUM(three_for_one_applied)                                                  AS tokens_applied,
       SUM(activating_three_for_one_qty_sold)                                      AS activating_qty_sold,
       SUM(activating_three_for_one_product_revenue_with_tariff)                   AS activating_product_revenue,
       SUM(activating_three_for_one_cogs)                                          AS activating_cogs,
       SUM(repeat_three_for_one_qty_sold)                                          AS repeat_qty_sold,
       SUM(repeat_three_for_one_product_revenue_with_tariff)                       AS repeat_product_revenue,
       SUM(repeat_three_for_one_cogs)                                              AS repeat_cogs,
       SUM(three_for_one_shipping_revenue)                                         AS total_shipping_revenue,
       SUM(three_for_one_shipping_cost)                                            AS total_shipping_cost,
       SUM(activating_three_for_one_shipping_revenue)                              AS activating_shipping_revenue,
       SUM(activating_three_for_one_shipping_cost)                                 AS activating_shipping_cost,
       SUM(repeat_three_for_one_shipping_revenue)                                  AS repeat_shipping_revenue,
       SUM(repeat_three_for_one_shipping_cost)                                     AS repeat_shipping_cost,
       SUM(CASE WHEN clearance_flag = 'clearance' THEN three_for_one_qty_sold END) AS total_clearance_qty_sold,
       SUM(CASE
               WHEN clearance_flag = 'clearance'
                   THEN activating_three_for_one_qty_sold END)                     AS activating_clearance_qty_sold,
       SUM(CASE
               WHEN clearance_flag = 'clearance'
                   THEN repeat_three_for_one_qty_sold END)                         AS repeat_clearance_qty_sold
FROM gfb.dos_107_merch_data_set_by_place_date
WHERE three_for_one_applied > 0
  AND date >= $start_date
GROUP BY token_breakdown,
         business_unit,
         sub_brand,
         region,
         country,
         department_detail,
         subcategory,
         subclass,
         gender,
         show_room,
         date,
         non_token_bundles,
         current_vip_retail,
         product_sku;

DELETE FROM gfb.gfb074_token_breakdown
WHERE date >= $start_date;

INSERT INTO gfb.gfb074_token_breakdown
SELECT *
FROM _token_sales
UNION
SELECT *
FROM _two_for_one_sales
UNION
SELECT *
FROM _pre_set_bundle_sales
UNION
SELECT *
FROM _non_token_sales
UNION
SELECT *
FROM _one_item_token_sales
UNION
SELECT *
FROM _three_for_one_sales;

DELETE FROM gfb.gfb074_token_breakdown
WHERE date < DATE_TRUNC(MONTH, DATEADD('MONTH', -36, CURRENT_DATE));
