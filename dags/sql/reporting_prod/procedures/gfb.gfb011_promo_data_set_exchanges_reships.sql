SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, CURRENT_DATE()));
SET end_date = CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _order_detail AS
SELECT fol.business_unit                                                 AS business_unit,
       mfd.sub_brand,
       fol.region                                                        AS region,
       fol.country                                                       AS country,
       fol.order_classification,
       fol.order_date                                                    AS order_date,
       fol.order_id,
       fol.order_line_id,
       fol.product_sku,
       mfd.style_name,
       mfd.department_detail,
       mfd.subcategory,
       fol.promo_id_1                                                    AS promo_id_1,
       IFF(fol.product_type = 'Membership GWP', 'GWP', fol.promo_code_1) AS promo_code_1,
       fol.promo_id_2                                                    AS promo_id_2,
       fol.promo_code_2                                                  AS promo_code_2,
       fol.customer_id,
       fol.session_id,
       fol.order_type,
       fol.dp_color                                                      AS color,
       DATE_TRUNC(DAY, dc.registration_local_datetime)                   AS registration_date,
       --Sales
       fol.total_qty_sold                                                AS total_qty_sold,
       (CASE
            WHEN fol.bundle_order_line_id = -1
                THEN fol.total_qty_sold
            ELSE 0 END)                                                  AS item_only_total_qty_sold,
       (CASE
            WHEN fol.order_type = 'vip activating' AND fol.bundle_order_line_id = -1
                THEN fol.total_qty_sold
            ELSE 0 END)                                                  AS item_only_activating_qty_sold,
       (CASE
            WHEN fol.order_type != 'vip activating' AND fol.bundle_order_line_id = -1
                THEN fol.total_qty_sold
            ELSE 0 END)                                                  AS item_only_repeat_qty_sold,
       fol.total_product_revenue                                         AS total_product_revenue,
       fol.total_cogs                                                    AS total_cogs,
       fol.cash_collected_amount                                         AS total_cash_collected,
       fol.total_cash_credit_amount + fol.total_non_cash_credit_amount   AS total_credit_redeemed_amount,
       fol.tokens_applied,
       fol.order_line_subtotal                                           AS total_subtotal_amount,
       CASE
           WHEN fol.order_type = 'vip activating'
               THEN fol.total_product_revenue
           ELSE 0 END                                                    AS activating_product_revenue,
       CASE
           WHEN fol.order_type = 'vip activating'
               THEN fol.total_qty_sold
           ELSE 0 END                                                    AS activating_qty_sold,
       CASE
           WHEN fol.order_type = 'vip activating'
               THEN fol.total_cogs
           ELSE 0 END                                                    AS activating_cogs,
       CASE
           WHEN fol.order_type = 'vip repeat'
               THEN fol.total_product_revenue
           ELSE 0 END                                                    AS repeat_product_revenue,
       CASE
           WHEN fol.order_type = 'vip repeat'
               THEN fol.total_qty_sold
           ELSE 0 END                                                    AS repeat_qty_sold,
       CASE
           WHEN fol.order_type = 'vip repeat'
               THEN fol.total_cogs
           ELSE 0 END                                                    AS repeat_cogs,
       CASE
           WHEN fol.order_type = 'ecom'
               THEN fol.total_product_revenue
           ELSE 0 END                                                    AS ecom_product_revenue,
       CASE
           WHEN fol.order_type = 'ecom'
               THEN fol.total_qty_sold
           ELSE 0 END                                                    AS ecom_qty_sold,
       CASE
           WHEN fol.order_type = 'ecom'
               THEN fol.total_cogs
           ELSE 0 END                                                    AS ecom_cogs,
       fol.total_discount                                                AS total_discount,
       CASE
           WHEN fol.order_type = 'vip activating'
               THEN fol.total_discount
           ELSE 0 END                                                    AS activating_discount,
       CASE
           WHEN fol.order_type = 'vip repeat'
               THEN fol.total_discount
           ELSE 0 END                                                    AS repeat_discount,
       CASE
           WHEN fol.order_type = 'ecom'
               THEN fol.total_discount
           ELSE 0 END                                                    AS ecom_discount,
       fol.total_shipping_revenue                                        AS total_shipping_revenue,
       CASE
           WHEN fol.order_type = 'vip activating'
               THEN fol.total_shipping_revenue
           ELSE 0 END                                                    AS activating_shipping_revenue,
       CASE
           WHEN fol.order_type = 'vip repeat'
               THEN fol.total_shipping_revenue
           ELSE 0 END                                                    AS repeat_shipping_revenue,
       CASE
           WHEN fol.order_type = 'ecom'
               THEN fol.total_shipping_revenue
           ELSE 0 END                                                    AS ecom_shipping_revenue,
       fol.total_shipping_cost                                           AS total_shipping_cost,
       CASE
           WHEN fol.order_type = 'vip activating'
               THEN fol.total_shipping_cost
           ELSE 0 END                                                    AS activating_shipping_cost,
       CASE
           WHEN fol.order_type = 'vip repeat'
               THEN fol.total_shipping_cost
           ELSE 0 END                                                    AS repeat_shipping_cost,
       CASE
           WHEN fol.order_type = 'ecom'
               THEN fol.total_shipping_cost
           ELSE 0 END                                                    AS ecom_shipping_cost,
       fol.clearance_flag,
       fol.clearance_price,
       fol.order_shipping_revenue
FROM gfb.gfb_order_line_data_set_place_date fol
         JOIN gfb.merch_dim_product mfd
              ON mfd.business_unit = fol.business_unit
                  AND mfd.region = fol.region
                  AND mfd.country = fol.country
                  AND mfd.product_sku = fol.product_sku
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = fol.customer_id
WHERE fol.order_date >= $start_date
  AND fol.order_date < $end_date
  AND fol.order_type IN ('vip activating', 'vip repeat', 'ecom')
  AND fol.order_classification IN ('exchange', 'product order', 'reship');

CREATE OR REPLACE TEMPORARY TABLE _order_detail_promo AS
SELECT od.*,
       smd.utm_source                          AS utm_source,
       smd.utm_medium                          AS utm_medium,
       smd.utm_campaign                        AS utm_campaign,
       scm.channel,
       scm.subchannel,
       --promo 1
       dp_1.promotion_name                     AS promo_1_promotion_name,
       dp_1.parent_promo_classification        AS promo_1_parent_promo_classification,
       dp_1.child_promo_classification         AS promo_1_child_promo_classification,
       dp_1.promotion_code                     AS promo_1_promotion_code,
       dp_1.status                             AS promo_1_status,
       dp_1.promotion_type                     AS promo_1_promotion_type,
       dp_1.promotion_discount_method          AS promo_1_promotion_discount_method,
       dp_1.subtotal_discount_percentage       AS promo_1_subtotal_discount_percentage,
       dp_1.subtotal_discount_amount           AS promo_1_subtotal_discount_amount,
       dp_1.shipping_discount_percentage       AS promo_1_shipping_discount_percentage,
       dp_1.shipping_discount_amount           AS promo_1_shipping_discount_amount,
       dp_1.product_category_include           AS promo_1_product_category_include,
       dp_1.featured_product_list_include      AS promo_1_featured_product_list_include,
       dp_1.product_category_exclude           AS promo_1_product_category_exclude,
       dp_1.featured_product_list_exclude      AS promo_1_featured_product_list_exclude,
       dp_1.allow_membership_credits           AS promo_1_allow_membership_credits,
       dp_1.allow_prepaid_creditcard           AS promo_1_allow_prepaid_creditcard,
       dp_1.date_start                         AS promo_1_date_start,
       dp_1.date_end                           AS promo_1_date_end,
       dp_1.refunds_allowed                    AS promo_1_refunds_allowed,
       dp_1.exchanges_allowed                  AS promo_1_exchanges_allowed,
       dp_1.display_on_pdp                     AS promo_1_display_on_pdp,
       dp_1.pdp_label                          AS promo_1_pdp_label,
       dp_1.allowed_with_other_promo           AS promo_1_allowed_with_other_promo,
       dp_1.max_uses_per_customer              AS promo_1_max_uses_per_customer,
       dp_1.min_cart_value                     AS promo_1_min_cart_value,
       --segments
       --1. Choose Your Segment
       dp_1.choose_your_segment                AS promo_1_choose_your_segment,
       dp_1.user_list                          AS promo_1_user_list,
       dp_1.sailthru_list                      AS promo_1_sailthru_list,
       dp_1.sign_up_gateway                    AS promo_1_sign_up_gateway,
       dp_1.current_gateway                    AS promo_1_current_gateway,
       --2. Add Filters
       --checkout options
       dp_1.checkout_option_leads_payg_flag    AS promo_1_checkout_option_leads_payg_flag,
       dp_1.checkout_option_post_reg_lead_flag AS promo_1_checkout_option_post_reg_lead_flag,
       dp_1.checkout_option_vip_flag           AS promo_1_checkout_option_vip_flag,
       --registration date
       dp_1.registration_date_option           AS promo_1_registration_date_option,
       dp_1.registration_exact_date            AS promo_1_registration_exact_date,
       dp_1.registration_start_date            AS promo_1_registration_start_date,
       dp_1.registration_end_date              AS promo_1_registration_end_date,
       dp_1.registration_day                   AS promo_1_registration_day,
       dp_1.registration_day_start             AS promo_1_registration_day_start,
       dp_1.registration_day_end               AS promo_1_registration_day_end,
       --3. Qualification
       dp_1.required_purchase_product_num      AS promo_1_required_purchase_product_num,
       dp_1.offer                              AS promo_1_offer,
       dp_1.campaign_tag                       AS promo_1_campaign_tag,
       --promo 2
       dp_2.promotion_name                     AS promo_2_promotion_name,
       dp_2.parent_promo_classification        AS promo_2_parent_promo_classification,
       dp_2.child_promo_classification         AS promo_2_child_promo_classification,
       dp_2.promotion_code                     AS promo_2_promotion_code,
       dp_2.status                             AS promo_2_status,
       dp_2.promotion_type                     AS promo_2_promotion_type,
       dp_2.promotion_discount_method          AS promo_2_promotion_discount_method,
       dp_2.subtotal_discount_percentage       AS promo_2_subtotal_discount_percentage,
       dp_2.subtotal_discount_amount           AS promo_2_subtotal_discount_amount,
       dp_2.shipping_discount_percentage       AS promo_2_shipping_discount_percentage,
       dp_2.shipping_discount_amount           AS promo_2_shipping_discount_amount,
       dp_2.product_category_include           AS promo_2_product_category_include,
       dp_2.featured_product_list_include      AS promo_2_featured_product_list_include,
       dp_2.product_category_exclude           AS promo_2_product_category_exclude,
       dp_2.featured_product_list_exclude      AS promo_2_featured_product_list_exclude,
       dp_2.allow_membership_credits           AS promo_2_allow_membership_credits,
       dp_2.allow_prepaid_creditcard           AS promo_2_allow_prepaid_creditcard,
       dp_2.date_start                         AS promo_2_date_start,
       dp_2.date_end                           AS promo_2_date_end,
       dp_2.refunds_allowed                    AS promo_2_refunds_allowed,
       dp_2.exchanges_allowed                  AS promo_2_exchanges_allowed,
       dp_2.display_on_pdp                     AS promo_2_display_on_pdp,
       dp_2.pdp_label                          AS promo_2_pdp_label,
       dp_2.allowed_with_other_promo           AS promo_2_allowed_with_other_promo,
       dp_2.max_uses_per_customer              AS promo_2_max_uses_per_customer,
       dp_2.min_cart_value                     AS promo_2_min_cart_value,
       --segments
       --1. Choose Your Segment
       dp_2.choose_your_segment                AS promo_2_choose_your_segment,
       dp_2.user_list                          AS promo_2_user_list,
       dp_2.sailthru_list                      AS promo_2_sailthru_list,
       dp_2.sign_up_gateway                    AS promo_2_sign_up_gateway,
       dp_2.current_gateway                    AS promo_2_current_gateway,
       --2. Add Filters
       --checkout options
       dp_2.checkout_option_leads_payg_flag    AS promo_2_checkout_option_leads_payg_flag,
       dp_2.checkout_option_post_reg_lead_flag AS promo_2_checkout_option_post_reg_lead_flag,
       dp_2.checkout_option_vip_flag           AS promo_2_checkout_option_vip_flag,
       --registration date
       dp_2.registration_date_option           AS promo_2_registration_date_option,
       dp_2.registration_exact_date            AS promo_2_registration_exact_date,
       dp_2.registration_start_date            AS promo_2_registration_start_date,
       dp_2.registration_end_date              AS promo_2_registration_end_date,
       dp_2.registration_day                   AS promo_2_registration_day,
       dp_2.registration_day_start             AS promo_2_registration_day_start,
       dp_2.registration_day_end               AS promo_2_registration_day_end,
       --3. Qualification
       dp_2.required_purchase_product_num      AS promo_2_required_purchase_product_num,
       dp_2.offer                              AS promo_2_offer,
       dp_2.campaign_tag                       AS promo_2_campaign_tag,
       CASE
           WHEN dp_1.product_category_include IS NULL AND dp_1.featured_product_list_include IS NULL AND
                dp_2.product_category_include IS NULL AND dp_2.featured_product_list_include IS NULL
               THEN 'All Products'
           WHEN dp_1.product_category_include IS NOT NULL THEN dp_1.product_category_include
           WHEN dp_2.product_category_include IS NOT NULL THEN dp_2.product_category_include
           ELSE 'Featured Product List'
           END                                 AS promo_category_include,
       pm_1.promo_mapping                      AS promo_1_promo_mapping,
       pm_2.promo_mapping                      AS promo_2_promo_mapping
FROM _order_detail od
         LEFT JOIN reporting_base_prod.shared.session smd
                   ON smd.meta_original_session_id = od.session_id
         LEFT JOIN reporting_base_prod.shared.media_source_channel_mapping scm
                   ON scm.media_source_hash = smd.media_source_hash
         LEFT JOIN gfb.dim_promo dp_1
                   ON dp_1.promo_id = od.promo_id_1
         LEFT JOIN gfb.dim_promo dp_2 /* not every order_line has 2nd promo */
                   ON dp_2.promo_id = od.promo_id_2
         LEFT JOIN lake_view.sharepoint.gfb_promo_mapping pm_1
                   ON UPPER(pm_1.business_unit) = od.business_unit
                       AND UPPER(pm_1.country) = od.country
                       AND UPPER(pm_1.promo_code) = od.promo_code_1
                       AND (od.order_date BETWEEN pm_1.start_date AND pm_1.end_date)
         LEFT JOIN lake_view.sharepoint.gfb_promo_mapping pm_2
                   ON UPPER(pm_2.business_unit) = od.business_unit
                       AND UPPER(pm_2.country) = od.country
                       AND UPPER(pm_2.promo_code) = od.promo_code_2
                       AND (od.order_date BETWEEN pm_2.start_date AND pm_2.end_date);

CREATE OR REPLACE TEMPORARY TABLE _order_promo AS
SELECT b.order_id,
       RTRIM(REPLACE(b.promo_code_by_order, ' ,', ''), ', ')           AS promo_code_by_order,
       RTRIM(REPLACE(b.promo_classification_by_order, ' ,', ''), ', ') AS promo_classification_by_order,
       RTRIM(REPLACE(b.promo_name_by_order, ' ,', ''), ', ')           AS promo_name_by_order,
       RTRIM(REPLACE(b.promo_mapping_by_order, ' ,', ''), ', ')        AS promo_mapping_by_order
FROM (SELECT pc_1.order_id,
             pc_1.promo_1_promotion_code || ', ' || COALESCE(pc_2.promo_2_promotion_code, '') AS promo_code_by_order,
             COALESCE(ppc_1.promo_1_parent_promo_classification, '') || ',' ||
             COALESCE(pcc_1.promo_1_child_promo_classification, '')
                 || ', '
                 || COALESCE(ppc_2.promo_2_parent_promo_classification, '') || ',' ||
             COALESCE(pcc_2.promo_2_child_promo_classification, '')                           AS promo_classification_by_order,
             COALESCE(pn_1.promo_1_promotion_name, '') || ',' ||
             COALESCE(pn_2.promo_2_promotion_name, '')                                        AS promo_name_by_order,
             COALESCE(pm_1.promo_1_promo_mapping, '') || ',' ||
             COALESCE(pm_2.promo_2_promo_mapping, '')                                         AS promo_mapping_by_order
      FROM (SELECT a.order_id,
                   LISTAGG(a.promo_1_promotion_code, ', ') AS promo_1_promotion_code
            FROM (SELECT DISTINCT a.order_id,
                                  a.promo_1_promotion_code
                  FROM _order_detail_promo a
                  WHERE a.promo_1_promotion_code IS NOT NULL
                  ORDER BY a.promo_1_promotion_code) a
            GROUP BY a.order_id) pc_1
               LEFT JOIN
           (SELECT a.order_id,
                   LISTAGG(a.promo_2_promotion_code, ', ') AS promo_2_promotion_code
            FROM (SELECT DISTINCT a.order_id,
                                  a.promo_2_promotion_code
                  FROM _order_detail_promo a
                  WHERE a.promo_2_promotion_code IS NOT NULL
                  ORDER BY a.promo_2_promotion_code) a
            GROUP BY a.order_id) pc_2
           ON pc_2.order_id = pc_1.order_id
               LEFT JOIN
           (SELECT a.order_id,
                   LISTAGG(a.promo_1_parent_promo_classification, ', ') AS promo_1_parent_promo_classification
            FROM (SELECT DISTINCT a.order_id,
                                  a.promo_1_parent_promo_classification
                  FROM _order_detail_promo a
                  WHERE a.promo_1_parent_promo_classification IS NOT NULL
                  ORDER BY a.promo_1_parent_promo_classification) a
            GROUP BY a.order_id) ppc_1
           ON ppc_1.order_id = pc_1.order_id
               LEFT JOIN
           (SELECT a.order_id,
                   LISTAGG(a.promo_1_child_promo_classification, ', ') AS promo_1_child_promo_classification
            FROM (SELECT DISTINCT a.order_id,
                                  a.promo_1_child_promo_classification
                  FROM _order_detail_promo a
                  WHERE a.promo_1_child_promo_classification IS NOT NULL
                  ORDER BY a.promo_1_child_promo_classification) a
            GROUP BY a.order_id) pcc_1
           ON pc_1.order_id = pcc_1.order_id
               LEFT JOIN
           (SELECT a.order_id,
                   LISTAGG(a.promo_2_parent_promo_classification, ', ') AS promo_2_parent_promo_classification
            FROM (SELECT DISTINCT a.order_id,
                                  a.promo_2_parent_promo_classification
                  FROM _order_detail_promo a
                  WHERE a.promo_2_parent_promo_classification IS NOT NULL
                  ORDER BY a.promo_2_parent_promo_classification) a
            GROUP BY a.order_id) ppc_2
           ON ppc_2.order_id = pc_1.order_id
               LEFT JOIN
           (SELECT a.order_id,
                   LISTAGG(a.promo_2_child_promo_classification, ', ') AS promo_2_child_promo_classification
            FROM (SELECT DISTINCT a.order_id,
                                  a.promo_2_child_promo_classification
                  FROM _order_detail_promo a
                  WHERE a.promo_2_child_promo_classification IS NOT NULL
                  ORDER BY a.promo_2_child_promo_classification) a
            GROUP BY a.order_id) pcc_2
           ON pcc_2.order_id = pc_1.order_id
               LEFT JOIN
           (SELECT a.order_id,
                   LISTAGG(a.promo_1_promotion_name, ', ') AS promo_1_promotion_name
            FROM (SELECT DISTINCT a.order_id,
                                  a.promo_1_promotion_name
                  FROM _order_detail_promo a
                  WHERE a.promo_1_promotion_name IS NOT NULL
                  ORDER BY a.promo_1_promotion_name) a
            GROUP BY a.order_id) pn_1
           ON pn_1.order_id = pc_1.order_id
               LEFT JOIN
           (SELECT a.order_id,
                   LISTAGG(a.promo_2_promotion_name, ', ') AS promo_2_promotion_name
            FROM (SELECT DISTINCT a.order_id,
                                  a.promo_2_promotion_name
                  FROM _order_detail_promo a
                  WHERE a.promo_2_promotion_name IS NOT NULL
                  ORDER BY a.promo_2_promotion_name) a
            GROUP BY a.order_id) pn_2
           ON pn_2.order_id = pc_1.order_id
               LEFT JOIN
           (SELECT a.order_id,
                   LISTAGG(a.promo_1_promo_mapping, ', ') AS promo_1_promo_mapping
            FROM (SELECT DISTINCT a.order_id,
                                  a.promo_1_promo_mapping
                  FROM _order_detail_promo a
                  WHERE a.promo_1_promo_mapping IS NOT NULL
                  ORDER BY a.promo_1_promo_mapping) a
            GROUP BY a.order_id) pm_1
           ON pm_1.order_id = pc_1.order_id
               LEFT JOIN
           (SELECT a.order_id,
                   LISTAGG(a.promo_2_promo_mapping, ', ') AS promo_2_promo_mapping
            FROM (SELECT DISTINCT a.order_id,
                                  a.promo_2_promo_mapping
                  FROM _order_detail_promo a
                  WHERE a.promo_2_promo_mapping IS NOT NULL
                  ORDER BY a.promo_2_promo_mapping) a
            GROUP BY a.order_id) pm_2
           ON pm_2.order_id = pc_1.order_id) b;

CREATE OR REPLACE TEMPORARY TABLE _order_promo_data_set AS
SELECT odp.*,
       op.promo_code_by_order,
       op.promo_classification_by_order,
       CASE
           WHEN CONTAINS(LOWER(op.promo_classification_by_order), 'upsell') THEN 'Upsell'
           ELSE 'No Upsell' END AS upsell_flag,
       op.promo_name_by_order,
       op.promo_mapping_by_order,
       CASE
           WHEN CONTAINS(UPPER(odp.promo_1_offer), 'BOGO') AND odp.order_type = 'vip activating'
               THEN 'Activating BOGO'
           WHEN CONTAINS(UPPER(odp.promo_1_offer), 'BOGO') THEN 'BOGO'
           WHEN odp.promo_1_child_promo_classification = 'Free Trial' THEN 'Free Trial'
           WHEN CONTAINS(UPPER(odp.promo_1_promotion_code), 'ENDO') AND odp.order_type = 'vip activating'
               THEN 'Endowment - Lead'
           WHEN CONTAINS(UPPER(odp.promo_1_promotion_code), 'ENDO') AND odp.order_type = 'vip repeat'
               THEN 'Endowment - VIP'
           WHEN CONTAINS(UPPER(odp.promo_1_promotion_code), 'UPSELL') AND odp.order_type = 'vip activating'
               THEN 'Activating Upsell'
           WHEN CONTAINS(odp.promo_1_offer, 'Off ') AND odp.promo_1_offer NOT LIKE '%Free Shipping%'
               THEN 'AOV Stretch'
           WHEN CONTAINS(odp.promo_1_offer, '3 for') THEN 'Bundle Offer'
           WHEN CONTAINS(odp.promo_1_offer, 'Price') THEN 'Price Bucket'
           WHEN CONTAINS(odp.promo_category_include, 'All Products') AND odp.promo_1_offer NOT LIKE '%Credit%'
               THEN 'Site Wide'
           WHEN CONTAINS(odp.promo_category_include, 'Featured Product List') AND
                NOT CONTAINS(odp.promo_1_offer, 'Credit') AND NOT CONTAINS(odp.promo_1_offer, '% Off')
               THEN 'Price Bucket'
           WHEN NOT CONTAINS(odp.promo_category_include, 'All Products') AND
                NOT CONTAINS(odp.promo_1_offer, 'Credit')
               THEN 'Category Specific - ' || odp.promo_category_include
           WHEN CONTAINS(odp.promo_category_include, 'All Products') AND
                CONTAINS(odp.promo_1_offer, 'Credit') AND odp.order_type = 'vip repeat' THEN 'Endowment - VIP'
           WHEN CONTAINS(odp.promo_category_include, 'All Products') AND
                CONTAINS(odp.promo_1_offer, 'Credit') AND odp.order_type = 'vip activating'
               THEN 'Endowment - Lead'
           WHEN CONTAINS(odp.promo_1_offer, 'Credit') AND odp.order_type = 'vip repeat' THEN 'Endowment - VIP'
           WHEN CONTAINS(odp.promo_1_offer, 'Credit') AND odp.order_type = 'vip activating'
               THEN 'Endowment - Lead'
           WHEN UPPER(odp.promo_1_promotion_code) IN (
                                                      'MASTERFLAT6', 'MASTERFLAT995B', 'MASTERFLAT15E',
                                                      'MASTERFLAT1995B', 'MASTERFLAT595', 'MASTERFLAT1295',
                                                      'GENPRICEBUCKET1695', 'GENPRICEBUCKET1795',
                                                      'GENPRICEBUCKET1895',
                                                      'GENPRICEBUCKET2095', 'GENPRICEBUCKET2295',
                                                      'GENPRICEBUCKET1795',
                                                      'GENPRICEBUCKET1895', 'GENPRICEBUCKET2085'
               ) THEN 'Clearance'
           END                  AS promo_1_group,
       CASE
           WHEN CONTAINS(odp.promo_2_offer, 'BOGO') AND odp.order_type = 'vip activating' THEN 'Activating BOGO'
           WHEN CONTAINS(odp.promo_2_offer, 'BOGO') THEN 'BOGO'
           WHEN odp.promo_2_child_promo_classification = 'Free Trial' THEN 'Free Trial'
           WHEN CONTAINS(odp.promo_2_promotion_code, 'ENDO') AND odp.order_type = 'vip activating'
               THEN 'Endowment - Lead'
           WHEN CONTAINS(odp.promo_2_promotion_code, 'ENDO') AND odp.order_type = 'vip repeat'
               THEN 'Endowment - VIP'
           WHEN CONTAINS(odp.promo_2_promotion_code, 'UPSELL') AND odp.order_type = 'vip activating'
               THEN 'Activating Upsell'
           WHEN CONTAINS(odp.promo_2_offer, 'Off ') AND odp.promo_2_offer NOT LIKE '%Free Shipping%'
               THEN 'AOV Stretch'
           WHEN CONTAINS(odp.promo_2_offer, '3 for') THEN 'Bundle Offer'
           WHEN CONTAINS(odp.promo_2_offer, 'Price') THEN 'Price Bucket'
           WHEN CONTAINS(odp.promo_category_include, 'All Products') AND odp.promo_2_offer NOT LIKE '%Credit%'
               THEN 'Site Wide'
           WHEN CONTAINS(odp.promo_category_include, 'Featured Product List') AND
                NOT CONTAINS(odp.promo_2_offer, 'Credit') AND NOT CONTAINS(odp.promo_2_offer, '% Off')
               THEN 'Price Bucket'
           WHEN NOT CONTAINS(odp.promo_category_include, 'All Products') AND
                NOT CONTAINS(odp.promo_2_offer, 'Credit')
               THEN 'Category Specific - ' || odp.promo_category_include
           WHEN CONTAINS(odp.promo_category_include, 'All Products') AND
                CONTAINS(odp.promo_2_offer, 'Credit') AND odp.order_type = 'vip repeat' THEN 'Endowment - VIP'
           WHEN CONTAINS(odp.promo_category_include, 'All Products') AND
                CONTAINS(odp.promo_2_offer, 'Credit') AND odp.order_type = 'vip activating'
               THEN 'Endowment - Lead'
           WHEN CONTAINS(odp.promo_2_offer, 'Credit') AND odp.order_type = 'vip repeat' THEN 'Endowment - VIP'
           WHEN CONTAINS(odp.promo_2_offer, 'Credit') AND odp.order_type = 'vip activating'
               THEN 'Endowment - Lead'
           WHEN UPPER(odp.promo_2_promotion_code) IN (
                                                      'MASTERFLAT6', 'MASTERFLAT995B', 'MASTERFLAT15E',
                                                      'MASTERFLAT1995B', 'MASTERFLAT595', 'MASTERFLAT1295',
                                                      'GENPRICEBUCKET1695', 'GENPRICEBUCKET1795',
                                                      'GENPRICEBUCKET1895',
                                                      'GENPRICEBUCKET2095', 'GENPRICEBUCKET2295',
                                                      'GENPRICEBUCKET1795',
                                                      'GENPRICEBUCKET1895', 'GENPRICEBUCKET2085'
               ) THEN 'Clearance'
           END                  AS promo_2_group
FROM _order_detail_promo odp
         LEFT JOIN _order_promo op
                   ON op.order_id = odp.order_id;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb011_promo_data_set_exchanges_reships AS
SELECT DISTINCT a.*,
                CASE
                    WHEN a.order_type = 'vip activating' AND (a.promo_id_1 IS NOT NULL OR a.promo_id_2 IS NOT NULL)
                        THEN 'VIP Acquisition'
                    WHEN CONTAINS(a.promo_1_group, 'Activating') THEN 'VIP Acquisition'
                    WHEN CONTAINS(a.promo_1_group, 'Lead') THEN 'VIP Acquisition'
                    WHEN a.promo_1_group = 'AOV Stretch' THEN 'GM$ Capture'
                    WHEN a.promo_1_group = 'Site Wide' THEN 'GM$ Capture'
                    WHEN a.promo_1_group = 'Endowment - VIP' THEN 'VIP Engagement'
                    WHEN (a.promo_id_1 IS NOT NULL OR a.promo_id_2 IS NOT NULL) THEN 'Inventory Mover'
                    END                                                                                                                     AS promo_1_goal,
                CASE
                    WHEN a.order_type = 'vip activating' AND (a.promo_id_1 IS NOT NULL OR a.promo_id_2 IS NOT NULL)
                        THEN 'VIP Acquisition'
                    WHEN CONTAINS(a.promo_2_group, 'Activating') THEN 'VIP Acquisition'
                    WHEN CONTAINS(a.promo_2_group, 'Lead') THEN 'VIP Acquisition'
                    WHEN a.promo_2_group = 'AOV Stretch' THEN 'GM$ Capture'
                    WHEN a.promo_2_group = 'Site Wide' THEN 'GM$ Capture'
                    WHEN a.promo_2_group = 'Endowment - VIP' THEN 'VIP Engagement'
                    WHEN (a.promo_id_1 IS NOT NULL OR a.promo_id_2 IS NOT NULL) THEN 'Inventory Mover'
                    END                                                                                                                     AS promo_2_goal,
                CASE
                    WHEN a.promo_code_1 IS NULL AND a.promo_code_2 IS NULL THEN 'Non-Promo Orders'
                    ELSE 'Promo Orders' END                                                                                                 AS promo_order_flag,
                CASE
                    WHEN promo_order_flag = 'Non-Promo Orders' THEN NULL
                    WHEN a.business_unit != 'FABKIDS' AND a.order_type = 'vip activating' AND
                         DATEDIFF(DAY, a.registration_date, a.order_date) + 1 > 7 AND
                         (LOWER(a.promo_1_campaign_tag) LIKE '%cross%' OR LOWER(a.promo_2_campaign_tag) LIKE '%cross%')
                        THEN 'Aged Lead Cross Promo'
                    WHEN a.business_unit != 'FABKIDS' AND a.order_type = 'vip activating' AND
                         DATEDIFF(DAY, a.registration_date, a.order_date) + 1 > 7 THEN 'Aged Lead'
                    WHEN a.business_unit != 'FABKIDS' AND a.order_type = 'vip activating' AND
                         DATEDIFF(DAY, a.registration_date, a.order_date) + 1 <= 7 AND
                         (LOWER(a.promo_1_campaign_tag) LIKE '%cross%' OR LOWER(a.promo_2_campaign_tag) LIKE '%cross%')
                        THEN 'Paid Media Cross Promo'
                    WHEN a.business_unit != 'FABKIDS' AND a.order_type = 'vip activating' AND
                         DATEDIFF(DAY, a.registration_date, a.order_date) + 1 <= 7 THEN 'Paid Media'
                    --FK Logic
                    WHEN a.business_unit = 'FABKIDS' AND a.order_type = 'vip activating' AND
                         DATEDIFF(DAY, a.registration_date, a.order_date) + 1 > 30 AND
                         (LOWER(a.promo_1_campaign_tag) LIKE '%cross%' OR LOWER(a.promo_2_campaign_tag) LIKE '%cross%')
                        THEN 'Aged Lead Cross Promo'
                    WHEN a.business_unit = 'FABKIDS' AND a.order_type = 'vip activating' AND
                         DATEDIFF(DAY, a.registration_date, a.order_date) + 1 > 30 THEN 'Aged Lead'
                    WHEN a.business_unit = 'FABKIDS' AND a.order_type = 'vip activating' AND
                         DATEDIFF(DAY, a.registration_date, a.order_date) + 1 <= 30 AND
                         (LOWER(a.promo_1_campaign_tag) LIKE '%cross%' OR LOWER(a.promo_2_campaign_tag) LIKE '%cross%')
                        THEN 'Paid Media Cross Promo'
                    WHEN a.business_unit = 'FABKIDS' AND a.order_type = 'vip activating' AND
                         DATEDIFF(DAY, a.registration_date, a.order_date) + 1 <= 30 THEN 'Paid Media'
                    END                                                                                                                     AS activating_offers,
                CASE
                    WHEN a.promo_code_by_order IS NULL THEN 'regular order'
                    ELSE 'sale order' END                                                                                                   AS sale_order_flag,
                CASE
                    WHEN cl.order_id IS NOT NULL THEN 'order with clearance'
                    ELSE 'non-clearance order' END                                                                                          AS order_with_clearance_flag,
                CASE
                    WHEN cl.order_id IS NOT NULL THEN 'order with markdown'
                    ELSE 'non-markdown order' END                                                                                           AS order_with_markdown_flag,
                CASE
                    WHEN a.order_shipping_revenue = 0 THEN 'free shipping'
                    ELSE 'non free shipping' END                                                                                            AS order_free_shipping_flag,
                COALESCE(dv.current_month_credit_billing_status, 'No Credit Billing - ' ||
                                                                 CAST(DATE_TRUNC(MONTH, DATEADD(DAY, -1, CURRENT_DATE())) AS VARCHAR(100))) AS current_month_credit_billing_status
FROM _order_promo_data_set a
         LEFT JOIN
     (SELECT DISTINCT od.order_id,
                      od.clearance_flag
      FROM _order_detail od
      WHERE od.clearance_flag = 'clearance') cl
     ON cl.order_id = a.order_id
         LEFT JOIN gfb.gfb_dim_vip dv
                   ON dv.customer_id = a.customer_id;
