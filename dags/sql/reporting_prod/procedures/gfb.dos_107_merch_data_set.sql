SET start_date = DATEADD(YEAR, -3, DATE_TRUNC(YEAR, CURRENT_DATE()));

CREATE OR REPLACE TEMPORARY TABLE _sales_info_place_date AS
SELECT ol.business_unit                                                          AS business_unit
     , ol.region                                                                 AS region
     , ol.country                                                                AS country
     , ol.product_sku
     , ol.order_date                                                             AS order_date
     , ol.clearance_flag                                                         AS clearance_flag
     , ol.clearance_price                                                        AS clearance_price
     , ol.lead_only_flag                                                         AS lead_only_flag
     , CASE
           WHEN prp.product_sku IS NOT NULL THEN 'Post Reg'
    END                                                                          AS post_reg_flag
     , psm.core_size_flag                                                        AS core_size_flag
     --Sales
     , SUM(ol.total_qty_sold)                                                    AS total_qty_sold
     , SUM(ol.total_product_revenue)                                             AS total_product_revenue
     , SUM(ol.total_cogs)                                                        AS total_cogs
     , SUM(ol.total_product_revenue_with_tariff)                                 AS total_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS activating_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS activating_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS activating_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS activating_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS repeat_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS repeat_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS repeat_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS repeat_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.clearance_flag = 'regular' THEN ol.total_discount
               ELSE COALESCE(mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount,
                             ol.total_discount) END)                             AS total_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' THEN ol.total_discount
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag != 'regular' THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS activating_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' THEN ol.total_discount
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag != 'regular' THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS repeat_discount

     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS lead_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS lead_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS lead_cogs
     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS lead_product_revenue_with_tariff
     , SUM(ol.tokens_applied)                                                    AS tokens_applied
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS token_qty_sold
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.token_local_amount
               ELSE 0 END)                                                       AS token_local_amount
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS token_product_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS token_cogs
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS token_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.tokens_applied > 0 THEN ol.total_discount
               ELSE 0 END)                                                       AS token_discount
     , SUM(ol.two_for_one_applied)                                               AS two_for_one_applied
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS two_for_one_qty_sold
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.token_local_amount
               ELSE 0 END)                                                       AS two_for_one_local_amount
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS two_for_one_product_revenue
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS two_for_one_cogs
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS two_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0 THEN ol.total_discount
               ELSE 0 END)                                                       AS two_for_one_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS activating_token_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS activating_token_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS activating_token_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS activating_token_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS repeat_token_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS repeat_token_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS repeat_token_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS repeat_token_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS activating_two_for_one_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS activating_two_for_one_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS activating_two_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS activating_two_for_one_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS repeat_two_for_one_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS repeat_two_for_one_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS repeat_two_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS repeat_two_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' AND ol.tokens_applied > 0
                   THEN ol.total_discount
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag != 'regular' AND ol.tokens_applied > 0
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS activating_token_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' AND ol.tokens_applied > 0
                   THEN ol.total_discount
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag != 'regular' AND ol.tokens_applied > 0
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS repeat_token_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' AND ol.two_for_one_applied > 0
                   THEN ol.total_discount
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag != 'regular' AND ol.two_for_one_applied > 0
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS activating_two_for_one_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' AND ol.two_for_one_applied > 0
                   THEN ol.total_discount
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag != 'regular' AND ol.two_for_one_applied > 0
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS repeat_two_for_one_discount
     , SUM(CASE WHEN bundle_product_id = '-1' THEN ol.tokens_applied ELSE 0 END) AS one_item_applied
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS one_item_qty_sold
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.token_local_amount
               ELSE 0 END)                                                       AS one_item_local_amount
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS one_item_product_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS one_item_cogs
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS one_item_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1' THEN ol.total_discount
               ELSE 0 END)                                                       AS one_item_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS activating_one_item_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS activating_one_item_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS activating_one_item_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS activating_one_item_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS repeat_one_item_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS repeat_one_item_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS repeat_one_item_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS repeat_one_item_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' AND ol.tokens_applied > 0 AND
                    bundle_product_id = '-1'
                   THEN ol.total_discount
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag != 'regular' AND ol.tokens_applied > 0 AND
                    bundle_product_id = '-1'
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS activating_one_item_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' AND ol.tokens_applied > 0 AND
                    bundle_product_id = '-1'
                   THEN ol.total_discount
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag != 'regular' AND ol.tokens_applied > 0 AND
                    bundle_product_id = '-1'
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS repeat_one_item_discount
     , SUM(ol.total_shipping_revenue)                                            AS total_shipping_revenue
     , SUM(ol.total_shipping_cost)                                               AS total_shipping_cost
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS activating_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS activating_shipping_cost
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS repeat_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS repeat_shipping_cost
     , SUM(CASE
               WHEN ol.tokens_applied > 0 THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS token_shipping_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0 THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS token_shipping_cost
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS activating_token_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS activating_token_shipping_cost
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS repeat_token_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS repeat_token_shipping_cost
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0 THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS two_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0 THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS two_for_one_shipping_cost
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS activating_two_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS activating_two_for_one_shipping_cost
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS repeat_two_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS repeat_two_for_one_shipping_cost
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1' THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS one_item_shipping_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1' THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS one_item_shipping_cost
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS activating_one_item_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS activating_one_item_shipping_cost
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS repeat_one_item_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS repeat_one_item_shipping_cost
--16819356
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.tokens_applied
               ELSE 0 END)                                                       AS three_for_one_applied
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS three_for_one_qty_sold
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.token_local_amount
               ELSE 0 END)                                                       AS three_for_one_local_amount
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS three_for_one_product_revenue
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS three_for_one_cogs
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS three_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_discount
               ELSE 0 END)                                                       AS three_for_one_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS activating_three_for_one_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS activating_three_for_one_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS activating_three_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS activating_three_for_one_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS repeat_three_for_one_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS repeat_three_for_one_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS repeat_three_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS repeat_three_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' AND
                    ol.bundle_product_id = 16819356
                   THEN ol.total_discount
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag != 'regular' AND
                    ol.bundle_product_id = 16819356
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS activating_three_for_one_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' AND
                    ol.bundle_product_id = 16819356
                   THEN ol.total_discount
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag != 'regular' AND
                    ol.bundle_product_id = 16819356
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS repeat_three_for_one_discount
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356 THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS three_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356 THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS three_for_one_shipping_cost
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS activating_three_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS activating_three_for_one_shipping_cost
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS repeat_three_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS repeat_three_for_one_shipping_cost
     , SUM(CASE
               WHEN bundle_order_line_id = -1
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS item_only_total_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND bundle_order_line_id = -1
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS item_only_activating_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND bundle_order_line_id = -1
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS item_only_repeat_qty_sold
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN gfb.merch_dim_product mdp
              ON mdp.business_unit = ol.business_unit
                  AND mdp.region = ol.region
                  AND mdp.country = ol.country
                  AND mdp.product_sku = ol.product_sku
         LEFT JOIN gfb.view_product_size_mapping psm
                   ON LOWER(psm.size) = LOWER(ol.dp_size)
                       AND psm.product_sku = mdp.product_sku
                       AND psm.region = ol.region
                       AND psm.country = ol.country
                       AND psm.business_unit = ol.business_unit
         LEFT JOIN lake_view.sharepoint.gfb_post_reg_product prp
                   ON LOWER(prp.store) = LOWER(ol.business_unit)
                       AND LOWER(prp.region) = LOWER(ol.region)
                       AND LOWER(prp.product_sku) = LOWER(ol.product_sku)
                       AND
                      (ol.order_date >= prp.date_added AND ol.order_date <= COALESCE(prp.date_removed, CURRENT_DATE()))
WHERE ol.order_classification = 'product order'
  AND ol.order_date >= $start_date
GROUP BY ol.business_unit
       , ol.region
       , ol.country
       , ol.product_sku
       , ol.order_date
       , ol.clearance_flag
       , ol.clearance_price
       , ol.lead_only_flag
       , psm.core_size_flag
       , (CASE
              WHEN prp.product_sku IS NOT NULL THEN 'Post Reg' END);


CREATE OR REPLACE TEMPORARY TABLE _sales_info_ship_date AS
SELECT ol.business_unit                                                          AS business_unit
     , ol.region                                                                 AS region
     , ol.country                                                                AS country
     , ol.product_sku
     , ol.ship_date                                                              AS ship_date
     , ol.clearance_flag                                                         AS clearance_flag
     , ol.clearance_price                                                        AS clearance_price
     , psm.core_size_flag                                                        AS core_size_flag
     --Sales
     , SUM(ol.total_qty_sold)                                                    AS total_qty_sold
     , SUM(ol.total_product_revenue)                                             AS total_product_revenue
     , SUM(ol.total_cogs)                                                        AS total_cogs
     , SUM(ol.total_product_revenue_with_tariff)                                 AS total_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS activating_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS activating_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS activating_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS activating_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS repeat_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS repeat_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS repeat_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS repeat_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.clearance_flag = 'regular' THEN ol.total_discount
               ELSE COALESCE(mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount,
                             ol.total_discount) END)                             AS total_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' THEN ol.total_discount
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag != 'regular' THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS activating_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' THEN ol.total_discount
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag != 'regular' THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS repeat_discount

     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS lead_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS lead_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS lead_cogs
     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS lead_product_revenue_with_tariff
     , SUM(ol.tokens_applied)                                                    AS tokens_applied
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS token_qty_sold
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.token_local_amount
               ELSE 0 END)                                                       AS token_local_amount
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS token_product_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS token_cogs
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS token_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.tokens_applied > 0 THEN ol.total_discount
               ELSE 0 END)                                                       AS token_discount
     , SUM(ol.two_for_one_applied)                                               AS two_for_one_applied
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS two_for_one_qty_sold
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.token_local_amount
               ELSE 0 END)                                                       AS two_for_one_local_amount
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS two_for_one_product_revenue
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS two_for_one_cogs
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS two_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0 THEN ol.total_discount
               ELSE 0 END)                                                       AS two_for_one_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS activating_token_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS activating_token_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS activating_token_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS activating_token_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS repeat_token_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS repeat_token_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS repeat_token_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS repeat_token_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS activating_two_for_one_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS activating_two_for_one_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS activating_two_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS activating_two_for_one_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS repeat_two_for_one_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS repeat_two_for_one_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS repeat_two_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS repeat_two_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' AND ol.tokens_applied > 0
                   THEN ol.total_discount
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag != 'regular' AND ol.tokens_applied > 0
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS activating_token_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' AND ol.tokens_applied > 0
                   THEN ol.total_discount
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag != 'regular' AND ol.tokens_applied > 0
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS repeat_token_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' AND ol.two_for_one_applied > 0
                   THEN ol.total_discount
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag != 'regular' AND ol.two_for_one_applied > 0
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS activating_two_for_one_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' AND ol.two_for_one_applied > 0
                   THEN ol.total_discount
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag != 'regular' AND ol.two_for_one_applied > 0
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS repeat_two_for_one_discount
     , SUM(CASE WHEN bundle_product_id = '-1' THEN ol.tokens_applied ELSE 0 END) AS one_item_applied
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS one_item_qty_sold
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.token_local_amount
               ELSE 0 END)                                                       AS one_item_local_amount
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS one_item_product_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS one_item_cogs
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS one_item_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1' THEN ol.total_discount
               ELSE 0 END)                                                       AS one_item_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS activating_one_item_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS activating_one_item_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS activating_one_item_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS activating_one_item_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS repeat_one_item_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS repeat_one_item_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS repeat_one_item_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS repeat_one_item_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' AND ol.tokens_applied > 0 AND
                    bundle_product_id = '-1'
                   THEN ol.total_discount
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag != 'regular' AND ol.tokens_applied > 0 AND
                    bundle_product_id = '-1'
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS activating_one_item_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' AND ol.tokens_applied > 0 AND
                    bundle_product_id = '-1'
                   THEN ol.total_discount
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag != 'regular' AND ol.tokens_applied > 0 AND
                    bundle_product_id = '-1'
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS repeat_one_item_discount
     , SUM(ol.total_shipping_revenue)                                            AS total_shipping_revenue
     , SUM(ol.total_shipping_cost)                                               AS total_shipping_cost
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS activating_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS activating_shipping_cost
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS repeat_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS repeat_shipping_cost
     , SUM(CASE
               WHEN ol.tokens_applied > 0 THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS token_shipping_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0 THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS token_shipping_cost
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS activating_token_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS activating_token_shipping_cost
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS repeat_token_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS repeat_token_shipping_cost
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0 THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS two_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0 THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS two_for_one_shipping_cost
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS activating_two_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS activating_two_for_one_shipping_cost
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS repeat_two_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS repeat_two_for_one_shipping_cost
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1' THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS one_item_shipping_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1' THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS one_item_shipping_cost
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS activating_one_item_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS activating_one_item_shipping_cost
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS repeat_one_item_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS repeat_one_item_shipping_cost
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.tokens_applied
               ELSE 0 END)                                                       AS three_for_one_applied
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS three_for_one_qty_sold
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.token_local_amount
               ELSE 0 END)                                                       AS three_for_one_local_amount
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS three_for_one_product_revenue
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS three_for_one_cogs
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS three_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_discount
               ELSE 0 END)                                                       AS three_for_one_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS activating_three_for_one_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS activating_three_for_one_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS activating_three_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS activating_three_for_one_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                       AS repeat_three_for_one_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS repeat_three_for_one_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_cogs
               ELSE 0 END)                                                       AS repeat_three_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                       AS repeat_three_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' AND
                    ol.bundle_product_id = 16819356
                   THEN ol.total_discount
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag != 'regular' AND
                    ol.bundle_product_id = 16819356
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS activating_three_for_one_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' AND
                    ol.bundle_product_id = 16819356
                   THEN ol.total_discount
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag != 'regular' AND
                    ol.bundle_product_id = 16819356
                   THEN COALESCE(
                           mdp.current_vip_retail - ol.order_line_subtotal + ol.total_discount, ol.total_discount)
               ELSE 0 END)                                                       AS repeat_three_for_one_discount
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356 THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS three_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356 THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS three_for_one_shipping_cost
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS activating_three_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS activating_three_for_one_shipping_cost
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                                       AS repeat_three_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                                       AS repeat_three_for_one_shipping_cost
     , SUM(CASE
               WHEN bundle_order_line_id = -1
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS item_only_total_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND bundle_order_line_id = -1
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS item_only_activating_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND bundle_order_line_id = -1
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                       AS item_only_repeat_qty_sold
FROM gfb.gfb_order_line_data_set_ship_date ol
         LEFT JOIN gfb.merch_dim_product mdp
                   ON mdp.business_unit = ol.business_unit
                       AND mdp.region = ol.region
                       AND mdp.country = ol.country
                       AND mdp.product_sku = ol.product_sku
         LEFT JOIN gfb.view_product_size_mapping psm
                   ON LOWER(psm.size) = LOWER(ol.dp_size)
                       AND psm.product_sku = mdp.product_sku
                       AND psm.region = ol.region
                       AND psm.country = ol.country
                       AND psm.business_unit = ol.business_unit
WHERE ol.order_classification = 'product order'
  AND ol.ship_date >= $start_date
GROUP BY ol.business_unit
       , ol.region
       , ol.country
       , ol.product_sku
       , ol.ship_date
       , ol.clearance_flag
       , ol.clearance_price
       , psm.core_size_flag;


CREATE OR REPLACE TEMPORARY TABLE _return_info AS
SELECT ol.business_unit               AS business_unit
     , ol.region                      AS region
     , ol.country                     AS country
     , ol.product_sku
     , ol.return_date                 AS return_date
     , (CASE
            WHEN lop.product_sku IS NOT NULL THEN 'lead only'
            ELSE 'not lead only' END) AS lead_only_flag
     , (CASE
            WHEN prp.product_sku IS NOT NULL THEN 'Post Reg'
    END)                              AS post_reg_flag
     , SUM(ol.total_return_unit)      AS total_return_unit
     , SUM(ol.total_return_dollars)   AS total_return_dollars
     , SUM(ol.total_refund_cost)      AS total_return_cost

     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_return_unit
               ELSE 0 END)            AS activating_return_units
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_return_dollars
               ELSE 0 END)            AS activating_return_dollars

     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_return_unit
               ELSE 0 END)            AS repeat_return_units
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_return_dollars
               ELSE 0 END)            AS repeat_return_dollars
FROM gfb.gfb_order_line_data_set_place_date ol
         LEFT JOIN lake_view.sharepoint.gfb_lead_only_product lop
                   ON LOWER(lop.brand) = LOWER(ol.business_unit)
                       AND LOWER(lop.region) = LOWER(ol.region)
                       AND LOWER(lop.product_sku) = LOWER(ol.product_sku)
                       AND
                      (CAST(ol.return_date AS DATE) >= lop.date_added AND
                       CAST(ol.return_date AS DATE) <= COALESCE(lop.date_removed, CURRENT_DATE()))
         LEFT JOIN lake_view.sharepoint.gfb_post_reg_product prp
                   ON LOWER(prp.store) = LOWER(ol.business_unit)
                       AND LOWER(prp.region) = LOWER(ol.region)
                       AND LOWER(prp.product_sku) = LOWER(ol.product_sku)
                       AND
                      (ol.return_date >= prp.date_added AND
                       ol.return_date <= COALESCE(prp.date_removed, CURRENT_DATE()))
WHERE ol.order_classification IN ('product order', 'exchange', 'reship')
  AND ol.return_date >= $start_date
GROUP BY ol.business_unit
       , ol.region
       , ol.country
       , ol.product_sku
       , ol.return_date
       , (CASE
              WHEN lop.product_sku IS NOT NULL THEN 'lead only'
              ELSE 'not lead only' END)
       , (CASE
              WHEN prp.product_sku IS NOT NULL THEN 'Post Reg' END);


CREATE OR REPLACE TEMPORARY TABLE _inventory_info AS
SELECT CASE
           WHEN id.region = 'EU' AND id.main_brand = 'FABKIDS' THEN 'JUSTFAB'
           ELSE id.main_brand END      AS main_brand
     , id.region                       AS region
     , id.country                      AS country
     , id.product_sku
     , id.inventory_date               AS inventory_date
     , (CASE
            WHEN id.clearance_price IS NOT NULL THEN 'clearance'
            ELSE 'regular' END)        AS clearance_flag
     , CASE
           WHEN id.clearance_price = '#N/A ()' THEN NULL
           ELSE id.clearance_price END AS clearance_price
     , (CASE
            WHEN lop.product_sku IS NOT NULL THEN 'lead only'
            ELSE 'not lead only' END)  AS lead_only_flag
     , (CASE
            WHEN prp.product_sku IS NOT NULL THEN 'Post Reg'
    END)                               AS post_reg_flag

     , SUM(id.qty_onhand)              AS qty_onhand
     , SUM(id.qty_available_to_sell)   AS qty_available_to_sell
     , SUM(id.qty_open_to_buy)         AS qty_open_to_buy
     , SUM(CASE
               WHEN id.warehouse = 'KENTUCKY'
                   THEN id.qty_available_to_sell
               ELSE 0 END)             AS kentucky_qty_available_to_sell
     , SUM(CASE
               WHEN id.warehouse = 'PERRIS'
                   THEN id.qty_available_to_sell
               ELSE 0 END)             AS perris_qty_available_to_sell
     , SUM(CASE
               WHEN id.warehouse = 'CANADA'
                   THEN id.qty_available_to_sell
               ELSE 0 END)             AS canada_qty_available_to_sell
     , SUM(CASE
               WHEN id.region = 'EU'
                   THEN id.qty_available_to_sell
               ELSE 0 END)             AS eu_qty_available_to_sell
     , SUM(CASE
               WHEN id.warehouse = 'UK'
                   THEN id.qty_available_to_sell
               ELSE 0 END)             AS uk_qty_available_to_sell
     , SUM(CASE
               WHEN id.warehouse = 'TIJUANA'
                   THEN id.qty_available_to_sell
               ELSE 0 END)             AS tj_qty_available_to_sell
     , SUM(id.qty_intransit)           AS qty_intransit
     , COUNT(DISTINCT CASE
                          WHEN id.qty_available_to_sell > 0 THEN id.dp_size
    END)                               AS sizes_available_in_stock
FROM gfb.gfb_inventory_data_set id
         LEFT JOIN lake_view.sharepoint.gfb_lead_only_product lop
                   ON LOWER(lop.brand) = LOWER(id.business_unit)
                       AND LOWER(lop.region) = LOWER(id.region)
                       AND LOWER(lop.product_sku) = LOWER(id.product_sku)
                       AND
                      (CAST(id.inventory_date AS DATE) >= lop.date_added AND
                       CAST(id.inventory_date AS DATE) < COALESCE(lop.date_removed, CURRENT_DATE()))
         LEFT JOIN lake_view.sharepoint.gfb_post_reg_product prp
                   ON LOWER(prp.store) = LOWER(id.business_unit)
                       AND LOWER(prp.region) = LOWER(id.region)
                       AND LOWER(prp.product_sku) = LOWER(id.product_sku)
                       AND
                      (id.inventory_date >= prp.date_added AND
                       id.inventory_date <= COALESCE(prp.date_removed, CURRENT_DATE()))
WHERE id.inventory_date >= $start_date
GROUP BY (CASE
              WHEN id.region = 'EU' AND id.main_brand = 'FABKIDS' THEN 'JUSTFAB'
              ELSE id.main_brand END)
       , id.region
       , id.country
       , id.product_sku
       , id.inventory_date
       , (CASE
              WHEN id.clearance_price IS NOT NULL THEN 'clearance'
              ELSE 'regular' END)
       , id.clearance_price
       , (CASE
              WHEN lop.product_sku IS NOT NULL THEN 'lead only'
              ELSE 'not lead only' END)
       , (CASE
              WHEN prp.product_sku IS NOT NULL THEN 'Post Reg' END);


CREATE OR REPLACE TEMPORARY TABLE _product_sales_days AS
SELECT ol.region
     , ol.product_sku
     , MIN(ol.order_date)                                    AS first_sale_date
     , MAX(ol.order_date)                                    AS last_sale_date
     , DATEDIFF(DAY, MIN(ol.order_date), MAX(ol.order_date)) AS days_of_selling
FROM gfb.gfb_order_line_data_set_place_date ol
WHERE ol.order_classification = 'product order'
GROUP BY ol.region
       , ol.product_sku;


CREATE OR REPLACE TEMPORARY TABLE _current_out_of_stock AS
SELECT a.region
     , a.product_sku
     , MAX(a.count_sizes)                                            AS count_sizes
     , MIN(a.count_0_inventory_size)                                 AS count_0_inventory_size
     , DIV0(MIN(a.count_0_inventory_size) * 1.0, MAX(a.count_sizes)) AS percent_0_inventory_size
FROM (
         SELECT a.business_unit
              , a.region
              , a.product_sku
              , COUNT(DISTINCT a.size)                  AS count_sizes
              , COUNT(DISTINCT CASE
                                   WHEN a.total_inventory = 0
                                       THEN a.size END) AS count_0_inventory_size
         FROM gfb.gfb015_out_of_stock_by_size a
         GROUP BY a.business_unit
                , a.region
                , a.product_sku
     ) a
GROUP BY a.region
       , a.product_sku;


CREATE OR REPLACE TEMPORARY TABLE _ghost_product AS
SELECT DISTINCT a.business_unit
              , a.region
              , a.product_sku
FROM gfb.gfb048_ghosting_proposal_data_set a;


CREATE OR REPLACE TEMPORARY TABLE _bundle_product AS
SELECT db.business_unit
     , db.region
     , db.product_sku
     , db.group_code
     , LISTAGG(DISTINCT db.bundle_product_id, ', ') AS list_of_bundle_product_ids
FROM gfb.gfb_dim_bundle db
WHERE db.group_code = 'BUNDLE'
  AND is_bundle_active = 1
  AND is_active = 1
GROUP BY db.business_unit, db.region, db.product_sku, db.group_code;

CREATE OR REPLACE TEMPORARY TABLE _main_brand AS
SELECT id.product_sku                                                                          AS product_sku
     , FIRST_VALUE(main_brand) OVER (PARTITION BY id.product_sku ORDER BY inventory_date DESC) AS main_brand
FROM gfb.gfb_inventory_data_set id
         JOIN gfb.merch_dim_product mdp
              ON id.business_unit = mdp.business_unit
                  AND id.region = mdp.region
                  AND id.country = mdp.country
                  AND id.product_sku = mdp.product_sku
WHERE UPPER(id.business_unit) IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS')
  AND mdp.sub_brand IN ('JFB')
UNION
SELECT product_sku, sub_brand AS main_brand
FROM gfb.merch_dim_product
WHERE sub_brand NOT IN ('JFB');

CREATE OR REPLACE TEMPORARY TABLE _prepack AS
SELECT DISTINCT item1 AS product_sku
              , compontents_number
FROM lake_view.sharepoint.gfb_fk_merch_outfit_attributes
WHERE outfit_vs_box_vs_pack = 'Prepack';

CREATE OR REPLACE TRANSIENT TABLE gfb.dos_107_merch_data_set_by_place_date AS
SELECT DISTINCT mdp.*
              , mb.main_brand
              , p.compontents_number
              , CASE
                    WHEN main_data.date IS NULL AND mdp.latest_launch_date >= DATE_TRUNC(MONTH, CURRENT_DATE())
                        THEN mdp.latest_launch_date::DATE
                    ELSE main_data.date END                                                                                      AS date
              , main_data.clearance_flag
              , main_data.clearance_price
              , main_data.lead_only_flag
              , main_data.post_reg_flag
              , main_data.core_size_flag
              , main_data.total_qty_sold
              , main_data.total_product_revenue
              , main_data.total_cogs
              , main_data.activating_product_revenue
              , main_data.activating_qty_sold
              , main_data.activating_cogs
              , main_data.repeat_product_revenue
              , main_data.repeat_qty_sold
              , main_data.repeat_cogs
              , main_data.total_discount
              , main_data.activating_discount
              , main_data.repeat_discount
              , main_data.lead_qty_sold
              , main_data.lead_product_revenue
              , main_data.lead_cogs
              , main_data.total_product_revenue_with_tariff
              , main_data.repeat_product_revenue_with_tariff
              , main_data.activating_product_revenue_with_tariff
              , main_data.lead_product_revenue_with_tariff
              , main_data.tokens_applied
              , main_data.token_qty_sold
              , main_data.token_local_amount
              , main_data.token_product_revenue
              , main_data.token_product_revenue_with_tariff
              , main_data.token_cogs
              , main_data.token_discount
              , main_data.two_for_one_applied
              , main_data.two_for_one_qty_sold
              , main_data.two_for_one_product_revenue
              , main_data.two_for_one_product_revenue_with_tariff
              , main_data.two_for_one_cogs
              , main_data.two_for_one_discount
              , main_data.activating_token_qty_sold
              , main_data.activating_token_product_revenue
              , main_data.activating_token_product_revenue_with_tariff
              , main_data.activating_token_cogs
              , main_data.activating_token_discount
              , main_data.repeat_token_qty_sold
              , main_data.repeat_token_product_revenue
              , main_data.repeat_token_product_revenue_with_tariff
              , main_data.repeat_token_cogs
              , main_data.repeat_token_discount
              , main_data.activating_two_for_one_qty_sold
              , main_data.activating_two_for_one_product_revenue
              , main_data.activating_two_for_one_product_revenue_with_tariff
              , main_data.activating_two_for_one_cogs
              , main_data.activating_two_for_one_discount
              , main_data.repeat_two_for_one_qty_sold
              , main_data.repeat_two_for_one_product_revenue
              , main_data.repeat_two_for_one_product_revenue_with_tariff
              , main_data.repeat_two_for_one_cogs
              , main_data.repeat_two_for_one_discount

              , main_data.total_return_unit
              , main_data.total_return_dollars
              , main_data.activating_return_units
              , main_data.activating_return_dollars
              , main_data.repeat_return_units
              , main_data.repeat_return_dollars
              , main_data.total_return_cost
              , main_data.one_item_applied
              , main_data.one_item_qty_sold
              , main_data.one_item_cogs
              , main_data.one_item_product_revenue
              , main_data.one_item_product_revenue_with_tariff
              , main_data.one_item_discount
              , main_data.activating_one_item_qty_sold
              , main_data.activating_one_item_product_revenue
              , main_data.activating_one_item_product_revenue_with_tariff
              , main_data.activating_one_item_cogs
              , main_data.activating_one_item_discount
              , main_data.repeat_one_item_qty_sold
              , main_data.repeat_one_item_product_revenue
              , main_data.repeat_one_item_product_revenue_with_tariff
              , main_data.repeat_one_item_cogs
              , main_data.repeat_one_item_discount
              , main_data.total_shipping_revenue
              , main_data.total_shipping_cost
              , main_data.activating_shipping_revenue
              , main_data.activating_shipping_cost
              , main_data.repeat_shipping_revenue
              , main_data.repeat_shipping_cost
              , main_data.token_shipping_revenue
              , main_data.token_shipping_cost
              , main_data.activating_token_shipping_revenue
              , main_data.activating_token_shipping_cost
              , main_data.repeat_token_shipping_revenue
              , main_data.repeat_token_shipping_cost
              , main_data.two_for_one_shipping_revenue
              , main_data.two_for_one_shipping_cost
              , main_data.activating_two_for_one_shipping_revenue
              , main_data.activating_two_for_one_shipping_cost
              , main_data.repeat_two_for_one_shipping_revenue
              , main_data.repeat_two_for_one_shipping_cost
              , main_data.one_item_shipping_revenue
              , main_data.one_item_shipping_cost
              , main_data.activating_one_item_shipping_revenue
              , main_data.activating_one_item_shipping_cost
              , main_data.repeat_one_item_shipping_revenue
              , main_data.repeat_one_item_shipping_cost
              , main_data.three_for_one_applied
              , main_data.three_for_one_qty_sold
              , main_data.three_for_one_local_amount
              , main_data.three_for_one_product_revenue
              , main_data.three_for_one_cogs
              , main_data.three_for_one_product_revenue_with_tariff
              , main_data.three_for_one_discount
              , main_data.activating_three_for_one_product_revenue
              , main_data.activating_three_for_one_qty_sold
              , main_data.activating_three_for_one_cogs
              , main_data.activating_three_for_one_product_revenue_with_tariff
              , main_data.repeat_three_for_one_product_revenue
              , main_data.repeat_three_for_one_qty_sold
              , main_data.repeat_three_for_one_cogs
              , main_data.repeat_three_for_one_product_revenue_with_tariff
              , main_data.activating_three_for_one_discount
              , main_data.repeat_three_for_one_discount
              , main_data.three_for_one_shipping_revenue
              , main_data.three_for_one_shipping_cost
              , main_data.activating_three_for_one_shipping_revenue
              , main_data.activating_three_for_one_shipping_cost
              , main_data.repeat_three_for_one_shipping_revenue
              , main_data.repeat_three_for_one_shipping_cost
              , main_data.item_only_total_qty_sold
              , main_data.item_only_activating_qty_sold
              , main_data.item_only_repeat_qty_sold

              , (CASE
                     WHEN (mb.main_brand IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS') AND mb.main_brand = mdp.business_unit)
                         OR (mdp.region = 'EU' AND mb.main_brand IN ('SHOEDAZZLE', 'FABKIDS'))
                         OR (mdp.sub_brand != 'JFB')
                         THEN main_data.qty_onhand
                     ELSE 0 END)                                                                                                 AS qty_onhand
              , (CASE
                     WHEN (mb.main_brand IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS') AND mb.main_brand = mdp.business_unit)
                         OR (mdp.region = 'EU' AND mb.main_brand IN ('SHOEDAZZLE', 'FABKIDS'))
                         OR (mdp.sub_brand != 'JFB')
                         THEN main_data.qty_available_to_sell
                     ELSE 0 END)                                                                                                 AS qty_available_to_sell
              , (CASE
                     WHEN (mb.main_brand IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS') AND mb.main_brand = mdp.business_unit)
                         OR (mdp.region = 'EU' AND mb.main_brand IN ('SHOEDAZZLE', 'FABKIDS'))
                         OR (mdp.sub_brand != 'JFB')
                         THEN main_data.open_to_buy_qty
                     ELSE 0 END)                                                                                                 AS open_to_buy_qty
              , main_data.kentucky_qty_available_to_sell
              , main_data.perris_qty_available_to_sell
              , main_data.canada_qty_available_to_sell
              , main_data.eu_qty_available_to_sell
              , main_data.uk_qty_available_to_sell
              , main_data.tj_qty_available_to_sell
              , main_data.qty_intransit
              , main_data.sizes_available_in_stock

              , (CASE
                     WHEN mdp.latest_launch_date > DATE_TRUNC(MONTH, main_data.date) THEN 'Future Showroom'
    END)                                                                                                                         AS future_showroom

              , psd.days_of_selling
              , psd.first_sale_date
              , psd.last_sale_date

              , oos.count_sizes
              , oos.count_0_inventory_size
              , oos.percent_0_inventory_size

              , (CASE
                     WHEN gp.product_sku IS NOT NULL THEN 'Ghosted Product'
                     ELSE 'Non Ghosted Product' END)                                                                             AS ghosted_product_flag

              , (CASE
                     WHEN bp.product_sku IS NOT NULL THEN 'Bundle Option Product'
                     ELSE 'Non Bundle Option Product' END)                                                                       AS bundle_option_product_flag
              , bp.list_of_bundle_product_ids
              , bp.group_code
              , LAG(main_data.qty_available_to_sell)
                    OVER (PARTITION BY mdp.business_unit, mdp.region, mdp.country, mdp.product_sku ORDER BY main_data.date DESC) AS qty_available_to_sell_end_of_day
FROM gfb.merch_dim_product mdp
         LEFT JOIN (
    SELECT COALESCE(si.business_unit, ri.business_unit, ii.main_brand)                                      AS business_unit_for_join
         , COALESCE(si.region, ri.region, ii.region)                                                        AS region_for_join
         , COALESCE(si.country, ri.country, ii.country)                                                     AS country_for_join
         , COALESCE(si.product_sku, ri.product_sku, ii.product_sku)                                         AS product_sku_for_join
         , COALESCE(si.order_date, ri.return_date, ii.inventory_date)                                       AS date
         , COALESCE(si.clearance_flag, ii.clearance_flag)                                                   AS clearance_flag
         , COALESCE(si.clearance_price,
                    ii.clearance_price)                                                                     AS clearance_price
         , COALESCE(si.lead_only_flag, ri.lead_only_flag, ii.lead_only_flag)                                AS lead_only_flag
         , COALESCE(si.post_reg_flag, ri.post_reg_flag, ii.post_reg_flag)                                   AS post_reg_flag
         , si.core_size_flag                                                                                AS core_size_flag
         , COALESCE(si.total_qty_sold, 0)                                                                   AS total_qty_sold
         , COALESCE(si.total_product_revenue, 0)                                                            AS total_product_revenue
         , COALESCE(si.total_cogs, 0)                                                                       AS total_cogs
         , COALESCE(si.activating_product_revenue, 0)                                                       AS activating_product_revenue
         , COALESCE(si.activating_qty_sold, 0)                                                              AS activating_qty_sold
         , COALESCE(si.activating_cogs, 0)                                                                  AS activating_cogs
         , COALESCE(si.repeat_product_revenue, 0)                                                           AS repeat_product_revenue
         , COALESCE(si.repeat_qty_sold, 0)                                                                  AS repeat_qty_sold
         , COALESCE(si.repeat_cogs, 0)                                                                      AS repeat_cogs
         , COALESCE(si.total_discount, 0)                                                                   AS total_discount
         , COALESCE(si.activating_discount, 0)                                                              AS activating_discount
         , COALESCE(si.repeat_discount, 0)                                                                  AS repeat_discount
         , COALESCE(si.lead_qty_sold, 0)                                                                    AS lead_qty_sold
         , COALESCE(si.lead_product_revenue, 0)                                                             AS lead_product_revenue
         , COALESCE(si.lead_cogs, 0)                                                                        AS lead_cogs
         , COALESCE(si.total_product_revenue_with_tariff, 0)                                                AS total_product_revenue_with_tariff
         , COALESCE(si.repeat_product_revenue_with_tariff, 0)                                               AS repeat_product_revenue_with_tariff
         , COALESCE(si.activating_product_revenue_with_tariff, 0)                                           AS activating_product_revenue_with_tariff
         , COALESCE(si.lead_product_revenue_with_tariff, 0)                                                 AS lead_product_revenue_with_tariff
         , COALESCE(si.tokens_applied, 0)                                                                   AS tokens_applied
         , COALESCE(si.token_qty_sold, 0)                                                                   AS token_qty_sold
         , COALESCE(si.token_local_amount, 0)                                                               AS token_local_amount
         , COALESCE(si.token_product_revenue, 0)                                                            AS token_product_revenue
         , COALESCE(si.token_product_revenue_with_tariff, 0)                                                AS token_product_revenue_with_tariff
         , COALESCE(si.token_cogs, 0)                                                                       AS token_cogs
         , COALESCE(si.token_discount, 0)                                                                   AS token_discount
         , COALESCE(si.two_for_one_applied, 0)                                                              AS two_for_one_applied
         , COALESCE(si.two_for_one_qty_sold, 0)                                                             AS two_for_one_qty_sold
         , COALESCE(si.two_for_one_product_revenue, 0)                                                      AS two_for_one_product_revenue
         , COALESCE(si.two_for_one_product_revenue_with_tariff, 0)                                          AS two_for_one_product_revenue_with_tariff
         , COALESCE(si.two_for_one_cogs, 0)                                                                 AS two_for_one_cogs
         , COALESCE(si.two_for_one_discount, 0)                                                             AS two_for_one_discount
         , COALESCE(si.activating_token_qty_sold, 0)                                                        AS activating_token_qty_sold
         , COALESCE(si.activating_token_product_revenue, 0)                                                 AS activating_token_product_revenue
         , COALESCE(si.activating_token_product_revenue_with_tariff, 0)                                     AS activating_token_product_revenue_with_tariff
         , COALESCE(si.activating_token_cogs, 0)                                                            AS activating_token_cogs
         , COALESCE(si.activating_token_discount, 0)                                                        AS activating_token_discount
         , COALESCE(si.repeat_token_qty_sold, 0)                                                            AS repeat_token_qty_sold
         , COALESCE(si.repeat_token_product_revenue, 0)                                                     AS repeat_token_product_revenue
         , COALESCE(si.repeat_token_product_revenue_with_tariff, 0)                                         AS repeat_token_product_revenue_with_tariff
         , COALESCE(si.repeat_token_cogs, 0)                                                                AS repeat_token_cogs
         , COALESCE(si.repeat_token_discount, 0)                                                            AS repeat_token_discount
         , COALESCE(si.activating_two_for_one_qty_sold, 0)                                                  AS activating_two_for_one_qty_sold
         , COALESCE(si.activating_two_for_one_product_revenue, 0)                                           AS activating_two_for_one_product_revenue
         , COALESCE(si.activating_two_for_one_product_revenue_with_tariff, 0)                               AS activating_two_for_one_product_revenue_with_tariff
         , COALESCE(si.activating_two_for_one_cogs, 0)                                                      AS activating_two_for_one_cogs
         , COALESCE(si.activating_two_for_one_discount, 0)                                                  AS activating_two_for_one_discount
         , COALESCE(si.repeat_two_for_one_qty_sold, 0)                                                      AS repeat_two_for_one_qty_sold
         , COALESCE(si.repeat_two_for_one_product_revenue, 0)                                               AS repeat_two_for_one_product_revenue
         , COALESCE(si.repeat_two_for_one_product_revenue_with_tariff, 0)                                   AS repeat_two_for_one_product_revenue_with_tariff
         , COALESCE(si.repeat_two_for_one_cogs, 0)                                                          AS repeat_two_for_one_cogs
         , COALESCE(si.repeat_two_for_one_discount, 0)                                                      AS repeat_two_for_one_discount
         , COALESCE(si.one_item_applied, 0)                                                                 AS one_item_applied
         , COALESCE(si.one_item_qty_sold, 0)                                                                AS one_item_qty_sold
         , COALESCE(si.one_item_cogs, 0)                                                                    AS one_item_cogs
         , COALESCE(si.one_item_product_revenue, 0)                                                         AS one_item_product_revenue
         , COALESCE(si.one_item_product_revenue_with_tariff, 0)                                             AS one_item_product_revenue_with_tariff
         , COALESCE(si.one_item_discount, 0)                                                                AS one_item_discount
         , COALESCE(si.activating_one_item_qty_sold, 0)                                                     AS activating_one_item_qty_sold
         , COALESCE(si.activating_one_item_product_revenue, 0)                                              AS activating_one_item_product_revenue
         , COALESCE(si.activating_one_item_product_revenue_with_tariff, 0)                                  AS activating_one_item_product_revenue_with_tariff
         , COALESCE(si.activating_one_item_cogs, 0)                                                         AS activating_one_item_cogs
         , COALESCE(si.activating_one_item_discount, 0)                                                     AS activating_one_item_discount
         , COALESCE(si.repeat_one_item_qty_sold, 0)                                                         AS repeat_one_item_qty_sold
         , COALESCE(si.repeat_one_item_product_revenue, 0)                                                  AS repeat_one_item_product_revenue
         , COALESCE(si.repeat_one_item_product_revenue_with_tariff, 0)                                      AS repeat_one_item_product_revenue_with_tariff
         , COALESCE(si.repeat_one_item_cogs, 0)                                                             AS repeat_one_item_cogs
         , COALESCE(si.repeat_one_item_discount, 0)                                                         AS repeat_one_item_discount
         , COALESCE(si.total_shipping_revenue, 0)                                                           AS total_shipping_revenue
         , COALESCE(si.total_shipping_cost, 0)                                                              AS total_shipping_cost
         , COALESCE(si.activating_shipping_revenue, 0)                                                      AS activating_shipping_revenue
         , COALESCE(si.activating_shipping_cost, 0)                                                         AS activating_shipping_cost
         , COALESCE(si.repeat_shipping_revenue, 0)                                                          AS repeat_shipping_revenue
         , COALESCE(si.repeat_shipping_cost, 0)                                                             AS repeat_shipping_cost
         , COALESCE(si.token_shipping_revenue, 0)                                                           AS token_shipping_revenue
         , COALESCE(si.token_shipping_cost, 0)                                                              AS token_shipping_cost
         , COALESCE(si.activating_token_shipping_revenue, 0)                                                AS activating_token_shipping_revenue
         , COALESCE(si.activating_token_shipping_cost, 0)                                                   AS activating_token_shipping_cost
         , COALESCE(si.repeat_token_shipping_revenue, 0)                                                    AS repeat_token_shipping_revenue
         , COALESCE(si.repeat_token_shipping_cost, 0)                                                       AS repeat_token_shipping_cost
         , COALESCE(si.two_for_one_shipping_revenue, 0)                                                     AS two_for_one_shipping_revenue
         , COALESCE(si.two_for_one_shipping_cost, 0)                                                        AS two_for_one_shipping_cost
         , COALESCE(si.activating_two_for_one_shipping_revenue, 0)                                          AS activating_two_for_one_shipping_revenue
         , COALESCE(si.activating_two_for_one_shipping_cost, 0)                                             AS activating_two_for_one_shipping_cost
         , COALESCE(si.repeat_two_for_one_shipping_revenue, 0)                                              AS repeat_two_for_one_shipping_revenue
         , COALESCE(si.repeat_two_for_one_shipping_cost, 0)                                                 AS repeat_two_for_one_shipping_cost
         , COALESCE(si.one_item_shipping_revenue, 0)                                                        AS one_item_shipping_revenue
         , COALESCE(si.one_item_shipping_cost, 0)                                                           AS one_item_shipping_cost
         , COALESCE(si.activating_one_item_shipping_revenue, 0)                                             AS activating_one_item_shipping_revenue
         , COALESCE(si.activating_one_item_shipping_cost, 0)                                                AS activating_one_item_shipping_cost
         , COALESCE(si.repeat_one_item_shipping_revenue, 0)                                                 AS repeat_one_item_shipping_revenue
         , COALESCE(si.repeat_one_item_shipping_cost, 0)                                                    AS repeat_one_item_shipping_cost
         , COALESCE(three_for_one_applied, 0)                                                               AS three_for_one_applied
         , COALESCE(three_for_one_qty_sold, 0)                                                              AS three_for_one_qty_sold
         , COALESCE(three_for_one_local_amount, 0)                                                          AS three_for_one_local_amount
         , COALESCE(three_for_one_product_revenue, 0)                                                       AS three_for_one_product_revenue
         , COALESCE(three_for_one_cogs, 0)                                                                  AS three_for_one_cogs
         , COALESCE(three_for_one_product_revenue_with_tariff, 0)                                           AS three_for_one_product_revenue_with_tariff
         , COALESCE(three_for_one_discount, 0)                                                              AS three_for_one_discount
         , COALESCE(activating_three_for_one_product_revenue, 0)                                            AS activating_three_for_one_product_revenue
         , COALESCE(activating_three_for_one_qty_sold, 0)                                                   AS activating_three_for_one_qty_sold
         , COALESCE(activating_three_for_one_cogs, 0)                                                       AS activating_three_for_one_cogs
         , COALESCE(activating_three_for_one_product_revenue_with_tariff, 0)                                AS activating_three_for_one_product_revenue_with_tariff
         , COALESCE(repeat_three_for_one_product_revenue, 0)                                                AS repeat_three_for_one_product_revenue
         , COALESCE(repeat_three_for_one_qty_sold, 0)                                                       AS repeat_three_for_one_qty_sold
         , COALESCE(repeat_three_for_one_cogs, 0)                                                           AS repeat_three_for_one_cogs
         , COALESCE(repeat_three_for_one_product_revenue_with_tariff, 0)                                    AS repeat_three_for_one_product_revenue_with_tariff
         , COALESCE(activating_three_for_one_discount, 0)                                                   AS activating_three_for_one_discount
         , COALESCE(repeat_three_for_one_discount, 0)                                                       AS repeat_three_for_one_discount
         , COALESCE(three_for_one_shipping_revenue, 0)                                                      AS three_for_one_shipping_revenue
         , COALESCE(three_for_one_shipping_cost, 0)                                                         AS three_for_one_shipping_cost
         , COALESCE(activating_three_for_one_shipping_revenue, 0)                                           AS activating_three_for_one_shipping_revenue
         , COALESCE(activating_three_for_one_shipping_cost, 0)                                              AS activating_three_for_one_shipping_cost
         , COALESCE(repeat_three_for_one_shipping_revenue, 0)                                               AS repeat_three_for_one_shipping_revenue
         , COALESCE(repeat_three_for_one_shipping_cost, 0)                                                  AS repeat_three_for_one_shipping_cost
         , COALESCE(item_only_total_qty_sold, 0)                                                            AS item_only_total_qty_sold
         , COALESCE(item_only_activating_qty_sold, 0)                                                       AS item_only_activating_qty_sold
         , COALESCE(item_only_repeat_qty_sold, 0)                                                           AS item_only_repeat_qty_sold

         , COUNT(si.product_sku)
                 OVER (PARTITION BY si.business_unit, si.region, si.country, si.order_date, si.product_sku) AS coef

         , COALESCE(ri.total_return_unit, 0) / IFF(coef = 0, 1, coef)                                       AS total_return_unit
         , COALESCE(ri.total_return_dollars, 0) / IFF(coef = 0, 1, coef)                                    AS total_return_dollars
         , COALESCE(ri.total_return_cost, 0) / IFF(coef = 0, 1, coef)                                       AS total_return_cost

         , COALESCE(ri.activating_return_units, 0) / IFF(coef = 0, 1, coef)                                 AS activating_return_units
         , COALESCE(ri.activating_return_dollars, 0) / IFF(coef = 0, 1, coef)                               AS activating_return_dollars
         , COALESCE(ri.repeat_return_units, 0) / IFF(coef = 0, 1, coef)                                     AS repeat_return_units
         , COALESCE(ri.repeat_return_dollars, 0) / IFF(coef = 0, 1, coef)                                   AS repeat_return_dollars

         , COALESCE(ii.qty_onhand, 0) / IFF(coef = 0, 1, coef)                                              AS qty_onhand
         , COALESCE(ii.qty_available_to_sell, 0) / IFF(coef = 0, 1, coef)                                   AS qty_available_to_sell
         , COALESCE(ii.qty_open_to_buy, 0) / IFF(coef = 0, 1, coef)                                         AS open_to_buy_qty
         , COALESCE(ii.kentucky_qty_available_to_sell, 0) /
           IFF(coef = 0, 1, coef)                                                                           AS kentucky_qty_available_to_sell
         , COALESCE(ii.perris_qty_available_to_sell, 0) / IFF(coef = 0, 1, coef)                            AS perris_qty_available_to_sell
         , COALESCE(ii.canada_qty_available_to_sell, 0) / IFF(coef = 0, 1, coef)                            AS canada_qty_available_to_sell
         , COALESCE(ii.eu_qty_available_to_sell, 0) / IFF(coef = 0, 1, coef)                                AS eu_qty_available_to_sell
         , COALESCE(ii.uk_qty_available_to_sell, 0) / IFF(coef = 0, 1, coef)                                AS uk_qty_available_to_sell
         , COALESCE(ii.tj_qty_available_to_sell, 0) / IFF(coef = 0, 1, coef)                                AS tj_qty_available_to_sell
         , COALESCE(ii.qty_intransit, 0) / IFF(coef = 0, 1, coef)                                           AS qty_intransit
         , COALESCE(ii.sizes_available_in_stock, 0) / IFF(coef = 0, 1, coef)                                AS sizes_available_in_stock
    FROM _sales_info_place_date si
             FULL JOIN _return_info ri
                       ON ri.business_unit = si.business_unit
                           AND ri.region = si.region
                           AND ri.country = si.country
                           AND ri.product_sku = si.product_sku
                           AND ri.return_date = si.order_date
             FULL JOIN _inventory_info ii
                       ON ii.main_brand = COALESCE(si.business_unit, ri.business_unit)
                           AND ii.region = COALESCE(si.region, ri.region)
                           AND ii.country = COALESCE(si.country, ri.country)
                           AND ii.product_sku = COALESCE(si.product_sku, ri.product_sku)
                           AND ii.inventory_date = COALESCE(si.order_date, ri.return_date)
) main_data
                   ON mdp.business_unit = main_data.business_unit_for_join
                       AND mdp.region = main_data.region_for_join
                       AND mdp.country = main_data.country_for_join
                       AND mdp.product_sku = main_data.product_sku_for_join
         LEFT JOIN _product_sales_days psd
                   ON psd.region = mdp.region
                       AND psd.product_sku = mdp.product_sku
         LEFT JOIN _current_out_of_stock oos
                   ON oos.region = mdp.region
                       AND oos.product_sku = mdp.product_sku
         LEFT JOIN _ghost_product gp
                   ON gp.business_unit = mdp.business_unit
                       AND gp.region = mdp.region
                       AND gp.product_sku = mdp.product_sku
         LEFT JOIN _bundle_product bp
                   ON bp.business_unit = mdp.business_unit
                       AND bp.region = mdp.region
                       AND bp.product_sku = mdp.product_sku
         LEFT JOIN _main_brand mb ON mb.product_sku = mdp.product_sku
         LEFT JOIN _prepack p ON p.product_sku = mdp.product_sku
WHERE (CASE
           WHEN main_data.date IS NULL AND mdp.latest_launch_date >= DATE_TRUNC(MONTH, CURRENT_DATE())
               THEN mdp.latest_launch_date::DATE
           ELSE main_data.date END) IS NOT NULL;


CREATE OR REPLACE TRANSIENT TABLE gfb.dos_107_merch_data_set_by_ship_date AS
SELECT DISTINCT mdp.*
              , mb.main_brand
              , CASE
                    WHEN main_data.date IS NULL AND mdp.latest_launch_date >= DATE_TRUNC(MONTH, CURRENT_DATE())
                        THEN mdp.latest_launch_date::DATE
                    ELSE main_data.date END                                                                                      AS date
              , main_data.clearance_flag
              , main_data.clearance_price
              , main_data.core_size_flag
              , main_data.total_qty_sold
              , main_data.total_product_revenue
              , main_data.total_cogs
              , main_data.activating_product_revenue
              , main_data.activating_qty_sold
              , main_data.activating_cogs
              , main_data.repeat_product_revenue
              , main_data.repeat_qty_sold
              , main_data.repeat_cogs
              , main_data.total_discount
              , main_data.activating_discount
              , main_data.repeat_discount
              , main_data.lead_qty_sold
              , main_data.lead_product_revenue
              , main_data.lead_cogs
              , main_data.total_product_revenue_with_tariff
              , main_data.repeat_product_revenue_with_tariff
              , main_data.activating_product_revenue_with_tariff
              , main_data.lead_product_revenue_with_tariff
              , main_data.tokens_applied
              , main_data.token_qty_sold
              , main_data.token_local_amount
              , main_data.token_product_revenue
              , main_data.token_product_revenue_with_tariff
              , main_data.token_cogs
              , main_data.token_discount
              , main_data.two_for_one_applied
              , main_data.two_for_one_qty_sold
              , main_data.two_for_one_product_revenue
              , main_data.two_for_one_product_revenue_with_tariff
              , main_data.two_for_one_cogs
              , main_data.two_for_one_discount
              , main_data.activating_token_qty_sold
              , main_data.activating_token_product_revenue
              , main_data.activating_token_product_revenue_with_tariff
              , main_data.activating_token_cogs
              , main_data.activating_token_discount
              , main_data.repeat_token_qty_sold
              , main_data.repeat_token_product_revenue
              , main_data.repeat_token_product_revenue_with_tariff
              , main_data.repeat_token_cogs
              , main_data.repeat_token_discount
              , main_data.activating_two_for_one_qty_sold
              , main_data.activating_two_for_one_product_revenue
              , main_data.activating_two_for_one_product_revenue_with_tariff
              , main_data.activating_two_for_one_cogs
              , main_data.activating_two_for_one_discount
              , main_data.repeat_two_for_one_qty_sold
              , main_data.repeat_two_for_one_product_revenue
              , main_data.repeat_two_for_one_product_revenue_with_tariff
              , main_data.repeat_two_for_one_cogs
              , main_data.repeat_two_for_one_discount

              , main_data.total_return_unit
              , main_data.total_return_dollars
              , main_data.activating_return_units
              , main_data.activating_return_dollars
              , main_data.repeat_return_units
              , main_data.repeat_return_dollars
              , main_data.total_return_cost
              , main_data.one_item_applied
              , main_data.one_item_qty_sold
              , main_data.one_item_cogs
              , main_data.one_item_product_revenue
              , main_data.one_item_product_revenue_with_tariff
              , main_data.one_item_discount
              , main_data.activating_one_item_qty_sold
              , main_data.activating_one_item_product_revenue
              , main_data.activating_one_item_product_revenue_with_tariff
              , main_data.activating_one_item_cogs
              , main_data.activating_one_item_discount
              , main_data.repeat_one_item_qty_sold
              , main_data.repeat_one_item_product_revenue
              , main_data.repeat_one_item_product_revenue_with_tariff
              , main_data.repeat_one_item_cogs
              , main_data.repeat_one_item_discount
              , main_data.total_shipping_revenue
              , main_data.total_shipping_cost
              , main_data.activating_shipping_revenue
              , main_data.activating_shipping_cost
              , main_data.repeat_shipping_revenue
              , main_data.repeat_shipping_cost
              , main_data.token_shipping_revenue
              , main_data.token_shipping_cost
              , main_data.activating_token_shipping_revenue
              , main_data.activating_token_shipping_cost
              , main_data.repeat_token_shipping_revenue
              , main_data.repeat_token_shipping_cost
              , main_data.two_for_one_shipping_revenue
              , main_data.two_for_one_shipping_cost
              , main_data.activating_two_for_one_shipping_revenue
              , main_data.activating_two_for_one_shipping_cost
              , main_data.repeat_two_for_one_shipping_revenue
              , main_data.repeat_two_for_one_shipping_cost
              , main_data.one_item_shipping_revenue
              , main_data.one_item_shipping_cost
              , main_data.activating_one_item_shipping_revenue
              , main_data.activating_one_item_shipping_cost
              , main_data.repeat_one_item_shipping_revenue
              , main_data.repeat_one_item_shipping_cost
              , main_data.three_for_one_applied
              , main_data.three_for_one_qty_sold
              , main_data.three_for_one_local_amount
              , main_data.three_for_one_product_revenue
              , main_data.three_for_one_cogs
              , main_data.three_for_one_product_revenue_with_tariff
              , main_data.three_for_one_discount
              , main_data.activating_three_for_one_product_revenue
              , main_data.activating_three_for_one_qty_sold
              , main_data.activating_three_for_one_cogs
              , main_data.activating_three_for_one_product_revenue_with_tariff
              , main_data.repeat_three_for_one_product_revenue
              , main_data.repeat_three_for_one_qty_sold
              , main_data.repeat_three_for_one_cogs
              , main_data.repeat_three_for_one_product_revenue_with_tariff
              , main_data.activating_three_for_one_discount
              , main_data.repeat_three_for_one_discount
              , main_data.three_for_one_shipping_revenue
              , main_data.three_for_one_shipping_cost
              , main_data.activating_three_for_one_shipping_revenue
              , main_data.activating_three_for_one_shipping_cost
              , main_data.repeat_three_for_one_shipping_revenue
              , main_data.repeat_three_for_one_shipping_cost
              , main_data.item_only_total_qty_sold
              , main_data.item_only_activating_qty_sold
              , main_data.item_only_repeat_qty_sold

              , (CASE
                     WHEN (mb.main_brand IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS') AND mb.main_brand = mdp.business_unit)
                         OR (mdp.region = 'EU' AND mb.main_brand IN ('SHOEDAZZLE', 'FABKIDS'))
                         OR (mdp.sub_brand != 'JFB')
                         THEN main_data.qty_onhand
                     ELSE 0 END)                                                                                                 AS qty_onhand
              , (CASE
                     WHEN (mb.main_brand IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS') AND mb.main_brand = mdp.business_unit)
                         OR (mdp.region = 'EU' AND mb.main_brand IN ('SHOEDAZZLE', 'FABKIDS'))
                         OR (mdp.sub_brand != 'JFB')
                         THEN main_data.qty_available_to_sell
                     ELSE 0 END)                                                                                                 AS qty_available_to_sell
              , (CASE
                     WHEN (mb.main_brand IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS') AND mb.main_brand = mdp.business_unit)
                         OR (mdp.region = 'EU' AND mb.main_brand IN ('SHOEDAZZLE', 'FABKIDS'))
                         OR (mdp.sub_brand != 'JFB')
                         THEN main_data.open_to_buy_qty
                     ELSE 0 END)                                                                                                 AS open_to_buy_qty
              , main_data.kentucky_qty_available_to_sell
              , main_data.perris_qty_available_to_sell
              , main_data.canada_qty_available_to_sell
              , main_data.eu_qty_available_to_sell
              , main_data.uk_qty_available_to_sell
              , main_data.tj_qty_available_to_sell
              , main_data.qty_intransit
              , main_data.sizes_available_in_stock

              , (CASE
                     WHEN mdp.latest_launch_date > DATE_TRUNC(MONTH, main_data.date) THEN 'Future Showroom'
    END)                                                                                                                         AS future_showroom

              , psd.days_of_selling
              , psd.first_sale_date
              , psd.last_sale_date

              , oos.count_sizes
              , oos.count_0_inventory_size
              , oos.percent_0_inventory_size

              , (CASE
                     WHEN gp.product_sku IS NOT NULL THEN 'Ghosted Product'
                     ELSE 'Non Ghosted Product' END)                                                                             AS ghosted_product_flag

              , (CASE
                     WHEN bp.product_sku IS NOT NULL THEN 'Bundle Option Product'
                     ELSE 'Non Bundle Option Product' END)                                                                       AS bundle_option_product_flag
              , bp.list_of_bundle_product_ids

              , bp.group_code

              , LAG(main_data.qty_available_to_sell)
                    OVER (PARTITION BY mdp.business_unit, mdp.region, mdp.country, mdp.product_sku ORDER BY main_data.date DESC) AS qty_available_to_sell_end_of_day
FROM gfb.merch_dim_product mdp
         LEFT JOIN (
    SELECT COALESCE(si.business_unit, ri.business_unit, ii.main_brand)                                     AS business_unit_for_join
         , COALESCE(si.region, ri.region, ii.region)                                                       AS region_for_join
         , COALESCE(si.country, ri.country, ii.country)                                                    AS country_for_join
         , COALESCE(si.product_sku, ri.product_sku, ii.product_sku)                                        AS product_sku_for_join
         , COALESCE(si.ship_date, ri.return_date, ii.inventory_date)                                       AS date
         , COALESCE(si.clearance_flag, ii.clearance_flag)                                                  AS clearance_flag
         , COALESCE(si.clearance_price, ii.clearance_price)                                                AS clearance_price
         , si.core_size_flag                                                                               AS core_size_flag
         , COALESCE(si.total_qty_sold, 0)                                                                  AS total_qty_sold
         , COALESCE(si.total_product_revenue, 0)                                                           AS total_product_revenue
         , COALESCE(si.total_cogs, 0)                                                                      AS total_cogs
         , COALESCE(si.activating_product_revenue, 0)                                                      AS activating_product_revenue
         , COALESCE(si.activating_qty_sold, 0)                                                             AS activating_qty_sold
         , COALESCE(si.activating_cogs, 0)                                                                 AS activating_cogs
         , COALESCE(si.repeat_product_revenue, 0)                                                          AS repeat_product_revenue
         , COALESCE(si.repeat_qty_sold, 0)                                                                 AS repeat_qty_sold
         , COALESCE(si.repeat_cogs, 0)                                                                     AS repeat_cogs
         , COALESCE(si.total_discount, 0)                                                                  AS total_discount
         , COALESCE(si.activating_discount, 0)                                                             AS activating_discount
         , COALESCE(si.repeat_discount, 0)                                                                 AS repeat_discount
         , COALESCE(si.lead_qty_sold, 0)                                                                   AS lead_qty_sold
         , COALESCE(si.lead_product_revenue, 0)                                                            AS lead_product_revenue
         , COALESCE(si.lead_cogs, 0)                                                                       AS lead_cogs
         , COALESCE(si.total_product_revenue_with_tariff, 0)                                               AS total_product_revenue_with_tariff
         , COALESCE(si.repeat_product_revenue_with_tariff, 0)                                              AS repeat_product_revenue_with_tariff
         , COALESCE(si.activating_product_revenue_with_tariff, 0)                                          AS activating_product_revenue_with_tariff
         , COALESCE(si.lead_product_revenue_with_tariff, 0)                                                AS lead_product_revenue_with_tariff
         , COALESCE(si.tokens_applied, 0)                                                                  AS tokens_applied
         , COALESCE(si.token_qty_sold, 0)                                                                  AS token_qty_sold
         , COALESCE(si.token_local_amount, 0)                                                              AS token_local_amount
         , COALESCE(si.token_product_revenue, 0)                                                           AS token_product_revenue
         , COALESCE(si.token_product_revenue_with_tariff, 0)                                               AS token_product_revenue_with_tariff
         , COALESCE(si.token_cogs, 0)                                                                      AS token_cogs
         , COALESCE(si.token_discount, 0)                                                                  AS token_discount
         , COALESCE(si.two_for_one_applied, 0)                                                             AS two_for_one_applied
         , COALESCE(si.two_for_one_qty_sold, 0)                                                            AS two_for_one_qty_sold
         , COALESCE(si.two_for_one_product_revenue, 0)                                                     AS two_for_one_product_revenue
         , COALESCE(si.two_for_one_product_revenue_with_tariff, 0)                                         AS two_for_one_product_revenue_with_tariff
         , COALESCE(si.two_for_one_cogs, 0)                                                                AS two_for_one_cogs
         , COALESCE(si.two_for_one_discount, 0)                                                            AS two_for_one_discount
         , COALESCE(si.activating_token_qty_sold, 0)                                                       AS activating_token_qty_sold
         , COALESCE(si.activating_token_product_revenue, 0)                                                AS activating_token_product_revenue
         , COALESCE(si.activating_token_product_revenue_with_tariff, 0)                                    AS activating_token_product_revenue_with_tariff
         , COALESCE(si.activating_token_cogs, 0)                                                           AS activating_token_cogs
         , COALESCE(si.activating_token_discount, 0)                                                       AS activating_token_discount
         , COALESCE(si.repeat_token_qty_sold, 0)                                                           AS repeat_token_qty_sold
         , COALESCE(si.repeat_token_product_revenue, 0)                                                    AS repeat_token_product_revenue
         , COALESCE(si.repeat_token_product_revenue_with_tariff, 0)                                        AS repeat_token_product_revenue_with_tariff
         , COALESCE(si.repeat_token_cogs, 0)                                                               AS repeat_token_cogs
         , COALESCE(si.repeat_token_discount, 0)                                                           AS repeat_token_discount
         , COALESCE(si.activating_two_for_one_qty_sold, 0)                                                 AS activating_two_for_one_qty_sold
         , COALESCE(si.activating_two_for_one_product_revenue, 0)                                          AS activating_two_for_one_product_revenue
         , COALESCE(si.activating_two_for_one_product_revenue_with_tariff, 0)                              AS activating_two_for_one_product_revenue_with_tariff
         , COALESCE(si.activating_two_for_one_cogs, 0)                                                     AS activating_two_for_one_cogs
         , COALESCE(si.activating_two_for_one_discount, 0)                                                 AS activating_two_for_one_discount
         , COALESCE(si.repeat_two_for_one_qty_sold, 0)                                                     AS repeat_two_for_one_qty_sold
         , COALESCE(si.repeat_two_for_one_product_revenue, 0)                                              AS repeat_two_for_one_product_revenue
         , COALESCE(si.repeat_two_for_one_product_revenue_with_tariff, 0)                                  AS repeat_two_for_one_product_revenue_with_tariff
         , COALESCE(si.repeat_two_for_one_cogs, 0)                                                         AS repeat_two_for_one_cogs
         , COALESCE(si.repeat_two_for_one_discount, 0)                                                     AS repeat_two_for_one_discount
         , COALESCE(si.one_item_applied, 0)                                                                AS one_item_applied
         , COALESCE(si.one_item_qty_sold, 0)                                                               AS one_item_qty_sold
         , COALESCE(si.one_item_cogs, 0)                                                                   AS one_item_cogs
         , COALESCE(si.one_item_product_revenue, 0)                                                        AS one_item_product_revenue
         , COALESCE(si.one_item_product_revenue_with_tariff, 0)                                            AS one_item_product_revenue_with_tariff
         , COALESCE(si.one_item_discount, 0)                                                               AS one_item_discount
         , COALESCE(si.activating_one_item_qty_sold, 0)                                                    AS activating_one_item_qty_sold
         , COALESCE(si.activating_one_item_product_revenue, 0)                                             AS activating_one_item_product_revenue
         , COALESCE(si.activating_one_item_product_revenue_with_tariff, 0)                                 AS activating_one_item_product_revenue_with_tariff
         , COALESCE(si.activating_one_item_cogs, 0)                                                        AS activating_one_item_cogs
         , COALESCE(si.activating_one_item_discount, 0)                                                    AS activating_one_item_discount
         , COALESCE(si.repeat_one_item_qty_sold, 0)                                                        AS repeat_one_item_qty_sold
         , COALESCE(si.repeat_one_item_product_revenue, 0)                                                 AS repeat_one_item_product_revenue
         , COALESCE(si.repeat_one_item_product_revenue_with_tariff, 0)                                     AS repeat_one_item_product_revenue_with_tariff
         , COALESCE(si.repeat_one_item_cogs, 0)                                                            AS repeat_one_item_cogs
         , COALESCE(si.repeat_one_item_discount, 0)                                                        AS repeat_one_item_discount
         , COALESCE(si.total_shipping_revenue, 0)                                                          AS total_shipping_revenue
         , COALESCE(si.total_shipping_cost, 0)                                                             AS total_shipping_cost
         , COALESCE(si.activating_shipping_revenue, 0)                                                     AS activating_shipping_revenue
         , COALESCE(si.activating_shipping_cost, 0)                                                        AS activating_shipping_cost
         , COALESCE(si.repeat_shipping_revenue, 0)                                                         AS repeat_shipping_revenue
         , COALESCE(si.repeat_shipping_cost, 0)                                                            AS repeat_shipping_cost
         , COALESCE(si.token_shipping_revenue, 0)                                                          AS token_shipping_revenue
         , COALESCE(si.token_shipping_cost, 0)                                                             AS token_shipping_cost
         , COALESCE(si.activating_token_shipping_revenue, 0)                                               AS activating_token_shipping_revenue
         , COALESCE(si.activating_token_shipping_cost, 0)                                                  AS activating_token_shipping_cost
         , COALESCE(si.repeat_token_shipping_revenue, 0)                                                   AS repeat_token_shipping_revenue
         , COALESCE(si.repeat_token_shipping_cost, 0)                                                      AS repeat_token_shipping_cost
         , COALESCE(si.two_for_one_shipping_revenue, 0)                                                    AS two_for_one_shipping_revenue
         , COALESCE(si.two_for_one_shipping_cost, 0)                                                       AS two_for_one_shipping_cost
         , COALESCE(si.activating_two_for_one_shipping_revenue, 0)                                         AS activating_two_for_one_shipping_revenue
         , COALESCE(si.activating_two_for_one_shipping_cost, 0)                                            AS activating_two_for_one_shipping_cost
         , COALESCE(si.repeat_two_for_one_shipping_revenue, 0)                                             AS repeat_two_for_one_shipping_revenue
         , COALESCE(si.repeat_two_for_one_shipping_cost, 0)                                                AS repeat_two_for_one_shipping_cost
         , COALESCE(si.one_item_shipping_revenue, 0)                                                       AS one_item_shipping_revenue
         , COALESCE(si.one_item_shipping_cost, 0)                                                          AS one_item_shipping_cost
         , COALESCE(si.activating_one_item_shipping_revenue, 0)                                            AS activating_one_item_shipping_revenue
         , COALESCE(si.activating_one_item_shipping_cost, 0)                                               AS activating_one_item_shipping_cost
         , COALESCE(si.repeat_one_item_shipping_revenue, 0)                                                AS repeat_one_item_shipping_revenue
         , COALESCE(si.repeat_one_item_shipping_cost, 0)                                                   AS repeat_one_item_shipping_cost
         , COALESCE(three_for_one_applied, 0)                                                              AS three_for_one_applied
         , COALESCE(three_for_one_qty_sold, 0)                                                             AS three_for_one_qty_sold
         , COALESCE(three_for_one_local_amount, 0)                                                         AS three_for_one_local_amount
         , COALESCE(three_for_one_product_revenue, 0)                                                      AS three_for_one_product_revenue
         , COALESCE(three_for_one_cogs, 0)                                                                 AS three_for_one_cogs
         , COALESCE(three_for_one_product_revenue_with_tariff, 0)                                          AS three_for_one_product_revenue_with_tariff
         , COALESCE(three_for_one_discount, 0)                                                             AS three_for_one_discount
         , COALESCE(activating_three_for_one_product_revenue, 0)                                           AS activating_three_for_one_product_revenue
         , COALESCE(activating_three_for_one_qty_sold, 0)                                                  AS activating_three_for_one_qty_sold
         , COALESCE(activating_three_for_one_cogs, 0)                                                      AS activating_three_for_one_cogs
         , COALESCE(activating_three_for_one_product_revenue_with_tariff, 0)                               AS activating_three_for_one_product_revenue_with_tariff
         , COALESCE(repeat_three_for_one_product_revenue, 0)                                               AS repeat_three_for_one_product_revenue
         , COALESCE(repeat_three_for_one_qty_sold, 0)                                                      AS repeat_three_for_one_qty_sold
         , COALESCE(repeat_three_for_one_cogs, 0)                                                          AS repeat_three_for_one_cogs
         , COALESCE(repeat_three_for_one_product_revenue_with_tariff, 0)                                   AS repeat_three_for_one_product_revenue_with_tariff
         , COALESCE(activating_three_for_one_discount, 0)                                                  AS activating_three_for_one_discount
         , COALESCE(repeat_three_for_one_discount, 0)                                                      AS repeat_three_for_one_discount
         , COALESCE(three_for_one_shipping_revenue, 0)                                                     AS three_for_one_shipping_revenue
         , COALESCE(three_for_one_shipping_cost, 0)                                                        AS three_for_one_shipping_cost
         , COALESCE(activating_three_for_one_shipping_revenue, 0)                                          AS activating_three_for_one_shipping_revenue
         , COALESCE(activating_three_for_one_shipping_cost, 0)                                             AS activating_three_for_one_shipping_cost
         , COALESCE(repeat_three_for_one_shipping_revenue, 0)                                              AS repeat_three_for_one_shipping_revenue
         , COALESCE(repeat_three_for_one_shipping_cost, 0)                                                 AS repeat_three_for_one_shipping_cost
         , COALESCE(item_only_total_qty_sold, 0)                                                           AS item_only_total_qty_sold
         , COALESCE(item_only_activating_qty_sold, 0)                                                      AS item_only_activating_qty_sold
         , COALESCE(item_only_repeat_qty_sold, 0)                                                          AS item_only_repeat_qty_sold

         , COUNT(si.product_sku)
                 OVER (PARTITION BY si.business_unit, si.region, si.country, si.ship_date, si.product_sku) AS coef

         , COALESCE(ri.total_return_unit, 0) / IFF(coef = 0, 1, coef)                                      AS total_return_unit
         , COALESCE(ri.total_return_dollars, 0) / IFF(coef = 0, 1, coef)                                   AS total_return_dollars
         , COALESCE(ri.activating_return_units, 0) / IFF(coef = 0, 1, coef)                                AS activating_return_units
         , COALESCE(ri.activating_return_dollars, 0) / IFF(coef = 0, 1, coef)                              AS activating_return_dollars
         , COALESCE(ri.repeat_return_units, 0) / IFF(coef = 0, 1, coef)                                    AS repeat_return_units
         , COALESCE(ri.repeat_return_dollars, 0) / IFF(coef = 0, 1, coef)                                  AS repeat_return_dollars
         , COALESCE(ri.total_return_cost, 0) / IFF(coef = 0, 1, coef)                                      AS total_return_cost

         , COALESCE(ii.qty_onhand, 0) / IFF(coef = 0, 1, coef)                                             AS qty_onhand
         , COALESCE(ii.qty_available_to_sell, 0) / IFF(coef = 0, 1, coef)                                  AS qty_available_to_sell
         , COALESCE(ii.qty_open_to_buy, 0) / IFF(coef = 0, 1, coef)                                        AS open_to_buy_qty

         , COALESCE(ii.kentucky_qty_available_to_sell, 0) /
           IFF(coef = 0, 1, coef)                                                                          AS kentucky_qty_available_to_sell
         , COALESCE(ii.perris_qty_available_to_sell, 0) / IFF(coef = 0, 1, coef)                           AS perris_qty_available_to_sell
         , COALESCE(ii.canada_qty_available_to_sell, 0) / IFF(coef = 0, 1, coef)                           AS canada_qty_available_to_sell
         , COALESCE(ii.eu_qty_available_to_sell, 0) / IFF(coef = 0, 1, coef)                               AS eu_qty_available_to_sell
         , COALESCE(ii.uk_qty_available_to_sell, 0) / IFF(coef = 0, 1, coef)                               AS uk_qty_available_to_sell
         , COALESCE(ii.tj_qty_available_to_sell, 0) / IFF(coef = 0, 1, coef)                               AS tj_qty_available_to_sell
         , COALESCE(ii.qty_intransit, 0) / IFF(coef = 0, 1, coef)                                          AS qty_intransit
         , COALESCE(ii.sizes_available_in_stock, 0) / IFF(coef = 0, 1, coef)                               AS sizes_available_in_stock
    FROM _sales_info_ship_date si
             FULL JOIN _return_info ri
                       ON ri.business_unit = si.business_unit
                           AND ri.region = si.region
                           AND ri.country = si.country
                           AND ri.product_sku = si.product_sku
                           AND ri.return_date = si.ship_date
             FULL JOIN _inventory_info ii
                       ON ii.main_brand = COALESCE(si.business_unit, ri.business_unit)
                           AND ii.region = COALESCE(si.region, ri.region)
                           AND ii.country = COALESCE(si.country, ri.country)
                           AND ii.product_sku = COALESCE(si.product_sku, ri.product_sku)
                           AND ii.inventory_date = COALESCE(si.ship_date, ri.return_date)
) main_data
                   ON mdp.business_unit = main_data.business_unit_for_join
                       AND mdp.region = main_data.region_for_join
                       AND mdp.country = main_data.country_for_join
                       AND mdp.product_sku = main_data.product_sku_for_join
         LEFT JOIN _product_sales_days psd
                   ON psd.region = mdp.region
                       AND psd.product_sku = mdp.product_sku
         LEFT JOIN _current_out_of_stock oos
                   ON oos.region = mdp.region
                       AND oos.product_sku = mdp.product_sku
         LEFT JOIN _ghost_product gp
                   ON gp.business_unit = mdp.business_unit
                       AND gp.region = mdp.region
                       AND gp.product_sku = mdp.product_sku
         LEFT JOIN _bundle_product bp
                   ON bp.business_unit = mdp.business_unit
                       AND bp.region = mdp.region
                       AND bp.product_sku = mdp.product_sku
         LEFT JOIN _main_brand mb ON mb.product_sku = mdp.product_sku
WHERE (CASE
           WHEN main_data.date IS NULL AND mdp.latest_launch_date >= DATE_TRUNC(MONTH, CURRENT_DATE())
               THEN mdp.latest_launch_date::DATE
           ELSE main_data.date END) IS NOT NULL;
