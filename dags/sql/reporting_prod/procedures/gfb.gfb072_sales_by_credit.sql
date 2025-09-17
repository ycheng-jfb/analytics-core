SET start_date = dateadd(year, -2, date_trunc(year, current_date()));

CREATE OR REPLACE TEMP TABLE _order_base AS
SELECT order_id
FROM edw_prod.data_model_jfb.fact_order
WHERE order_local_datetime::DATE >= $start_date
ORDER BY order_id;

CREATE OR REPLACE TEMPORARY TABLE _credits_tokens AS
SELECT
    oc.order_id,
    LISTAGG(DISTINCT ROUND(credit_issued_local_amount,2),', ') AS membership_price,
    COUNT(DISTINCT CASE WHEN dc.credit_type = 'Fixed Credit' THEN oc.store_credit_id END) AS credits_used,
    0 AS tokens_used
FROM lake_jfb_view.ultra_merchant.order_credit oc
JOIN edw_prod.data_model_jfb.dim_credit dc
    ON oc.store_credit_id = dc.credit_id
WHERE dc.source_credit_id_type = 'store_credit_id'
    AND dc.credit_type = 'Fixed Credit'
    AND dc.credit_reason = 'Membership Credit'
    and oc.order_id IN (SELECT order_id FROM _order_base)
GROUP BY oc.order_id
UNION ALL
SELECT
    oc.order_id,
    LISTAGG(DISTINCT (CASE WHEN dc.credit_reason = 'Converted Membership Credit' THEN 49.95 ELSE ROUND(credit_issued_local_amount,2) END),', ') AS membership_price,
    0 AS credits_used,
    COUNT(DISTINCT oc.membership_token_id) AS tokens_used
FROM lake_jfb_view.ultra_merchant.order_credit oc
JOIN edw_prod.data_model_jfb.dim_credit dc
    ON oc.membership_token_id = dc.credit_id
WHERE dc.source_credit_id_type = 'Token'
    AND dc.credit_type = 'Token'
    AND dc.credit_reason IN ('Token Billing', 'Converted Membership Credit')
    and oc.order_id IN (SELECT order_id FROM _order_base)
GROUP BY oc.order_id;

CREATE OR REPLACE TEMPORARY TABLE _credits_used AS
SELECT
    order_id,
    membership_price,
    SUM(credits_used) AS credits_used,
    SUM(tokens_used) AS tokens_used
FROM _credits_tokens
GROUP BY order_id,
         membership_price;

CREATE OR REPLACE TEMPORARY TABLE _refunds AS
SELECT order_id,
    SUM(cash_refund_local_amount) AS cash_refund_local_amount
FROM edw_prod.data_model_jfb.fact_refund
WHERE order_id IN (SELECT order_id FROM _order_base)
GROUP BY order_id;

CREATE OR REPLACE TEMPORARY TABLE _chargebacks AS
SELECT order_id,
    SUM(chargeback_local_amount) AS chargeback_local_amount
FROM edw_prod.data_model_jfb.fact_chargeback
WHERE order_id IN (SELECT order_id FROM _order_base)
GROUP BY order_id;

CREATE OR REPLACE TEMPORARY TABLE _frl AS
SELECT order_id,
    SUM(estimated_return_shipping_cost_local_amount) AS estimated_return_shipping_cost_local_amount,
    SUM(estimated_returned_product_cost_local_amount_resaleable) AS estimated_returned_product_cost_local_amount_resaleable
FROM edw_prod.data_model_jfb.fact_return_line
WHERE order_id IN (SELECT order_id FROM _order_base)
GROUP BY order_id;

CREATE OR REPLACE TEMPORARY TABLE _order_cost_GM AS
SELECT fo.order_id,
    fo.order_local_datetime::DATE AS order_date,
    IFF(osc.order_classification_l1 IN ('Product Order','Reship','Exchange'), fo.cash_gross_revenue_local_amount,0) AS product_order_cash_gross_revenue_amount,
    IFF(osc.order_sales_channel_l1 = 'Billing Order', fo.cash_gross_revenue_local_amount ,0) AS billing_cash_gross_revenue,
    IFF(osc.order_sales_channel_l1 = 'Billing Order', fr.cash_refund_local_amount,0) AS billing_cash_refund_amount,
    IFF(osc.order_sales_channel_l1 = 'Billing Order', fc.chargeback_local_amount,0) AS billing_cash_chargeback_amount,
    IFF(osc.order_classification_l1 = 'Product Order', fo.reporting_landed_cost_local_amount,0) AS product_order_landed_product_cost_amount,
    IFF(osc.order_classification_l1 = 'Product Order', fo.estimated_shipping_supplies_cost_local_amount,0) AS product_order_shipping_supplies_cost_amount,
    IFF(osc.order_classification_l1 = 'Product Order', fo.shipping_cost_local_amount,0) AS product_order_shipping_cost_amount,
    IFF(osc.order_classification_l1 = 'Product Order',fc.chargeback_local_amount,0) AS product_order_cash_chargeback_amount,
    frl.estimated_return_shipping_cost_local_amount AS product_order_return_shipping_cost_amount,
    IFF(osc.order_classification_l1 = 'Product Order',fr.cash_refund_local_amount,0) AS product_order_cash_refund_amount,
    frl.estimated_returned_product_cost_local_amount_resaleable AS product_order_cost_product_returned_resaleable_amount,
    IFF(osc.order_classification_l1 = 'Reship', fo.reporting_landed_cost_local_amount,0) AS product_order_Reship_product_cost_amount,
    IFF(osc.order_classification_l1 = 'Reship', fo.shipping_cost_local_amount,0) AS product_order_Reship_shipping_cost_amount,
    IFF(osc.order_classification_l1 = 'Reship', fo.estimated_shipping_supplies_cost_local_amount,0) AS product_order_Reship_shipping_supplies_cost_amount,
    IFF(osc.order_classification_l1 = 'Exchange', fo.reporting_landed_cost_local_amount,0) AS product_order_Exchange_product_cost_amount,
    IFF(osc.order_classification_l1 = 'Exchange', fo.shipping_cost_local_amount,0) AS product_order_Exchange_shipping_cost_amount,
    IFF(osc.order_classification_l1 = 'Exchange', fo.estimated_shipping_supplies_cost_local_amount,0) AS product_order_Exchange_shipping_supplies_cost_amount,
    IFF(osc.order_classification_l1 IN ('Product Order','Reship','Exchange'), fo.misc_cogs_local_amount,0) AS product_order_misc_cogs_amount
FROM edw_prod.data_model_jfb.fact_order fo
    JOIN edw_prod.data_model_jfb.dim_order_sales_channel AS osc
        ON osc.order_sales_channel_key = fo.order_sales_channel_key
    LEFT JOIN _refunds AS fr
        ON fr.order_id = fo.order_id
    LEFT JOIN _chargebacks fc
        ON fc.order_id = fo.order_id
    LEFT JOIN _frl AS frl
        ON fo.order_id = frl.order_id
WHERE fo.order_id IN (SELECT order_id FROM _order_base)
ORDER BY 1;

CREATE OR REPLACE TEMPORARY TABLE _active_credits AS
SELECT sc.customer_id,
       m.price                                                AS membership_price,
       COUNT(sc.store_credit_id)                              AS active_credits
FROM lake_jfb_view.ultra_merchant.store_credit sc
     JOIN edw_prod.data_model_jfb.dim_credit dc
          ON dc.credit_id = sc.store_credit_id
     LEFT JOIN lake_jfb_view.ultra_merchant.membership m
               ON m.customer_id = sc.customer_id
WHERE sc.statuscode = 3240
GROUP BY sc.customer_id,
       m.price;

CREATE OR REPLACE TEMPORARY TABLE _order_line_data_set_placed AS
SELECT
    old.business_unit,
    mdp.sub_brand,
    old.region,
    old.order_id,
    old.clearance_flag,
    ac.active_credits,
    old.order_date,
    SUM(old.total_product_revenue) AS revenue,
    SUM(old.total_cogs) AS total_cogs,
    SUM(old.total_shipping_revenue) AS shipping_revenue,
    SUM(old.total_qty_sold) AS units,
    SUM(old.cash_collected_amount) AS cash_collected_amount,
    SUM(old.total_cash_credit_amount) AS total_cash_credit_amount,
    SUM(old.total_non_cash_credit_amount) AS total_non_cash_credit_amount,
    SUM(old.total_product_revenue - total_cogs) AS gaap_gross_margin
FROM gfb.gfb_order_line_data_set_place_date old
    JOIN gfb.merch_dim_product mdp
        ON mdp.business_unit = old.business_unit
            AND mdp.region = old.region
            AND mdp.country = old.country
            AND mdp.product_sku = old.product_sku
    LEFT JOIN _active_credits ac
        ON ac.customer_id = old.customer_id
WHERE order_id IN (SELECT order_id FROM _order_base)
    AND order_classification = 'product order'
    AND order_type = 'vip repeat'
GROUP BY
    old.business_unit,
    mdp.sub_brand,
    old.region,
    old.order_id,
    old.clearance_flag,
    ac.active_credits,
    old.order_date;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb072_sales_by_credit AS
SELECT
    CASE
        WHEN cu.credits_used IS NULL THEN 'Cash Only Order'
        WHEN cu.credits_used IS NOT NULL AND cash_collected_amount > 0 THEN 'Cash and Credit Order'
        ELSE 'Credit Only Order'
    END AS order_type,
    CASE
        WHEN cu.credits_used >= 8 THEN '8+'
        ELSE cu.credits_used::STRING
    END AS credits_applied,
    CASE
        WHEN cu.tokens_used >= 8 THEN '8+'
        ELSE cu.tokens_used::STRING
    END AS tokens_applied,
    cu.membership_price,
    ol.business_unit,
    ol.sub_brand,
    ol.region,
    ol.order_date,
    ol.order_id,
    ol.clearance_flag,
    ol.active_credits,
    ol.revenue,
    ol.shipping_revenue,
    ol.units,
    ol.gaap_gross_margin,
    ol.total_cash_credit_amount,
    ol.total_non_cash_credit_amount,
    ol.cash_collected_amount,
     IFNULL(gm.product_order_cash_gross_revenue_amount,0)
                + IFNULL(gm.billing_cash_gross_revenue,0)
                - IFNULL(gm.product_order_cash_refund_amount,0)
                - IFNULL(gm.product_order_cash_chargeback_amount,0)
                - IFNULL(gm.billing_cash_refund_amount,0)
                - IFNULL(gm.billing_cash_chargeback_amount,0)
                - IFNULL(gm.product_order_landed_product_cost_amount,0)
                - IFNULL(gm.product_order_shipping_supplies_cost_amount,0)
                - IFNULL(gm.product_order_shipping_cost_amount,0)
                - IFNULL(gm.product_order_return_shipping_cost_amount,0)
                + IFNULL(gm.product_order_cost_product_returned_resaleable_amount,0)
                - IFNULL(gm.product_order_reship_product_cost_amount,0)
                - IFNULL(gm.product_order_reship_shipping_cost_amount,0)
                - IFNULL(gm.product_order_reship_shipping_supplies_cost_amount,0)
                - IFNULL(gm.product_order_exchange_product_cost_amount,0)
                - IFNULL(gm.product_order_exchange_shipping_cost_amount,0)
                - IFNULL(gm.product_order_exchange_shipping_supplies_cost_amount,0)
                - IFNULL(gm.product_order_misc_cogs_amount,0)
            AS cash_gross_margin
FROM _order_line_data_set_placed ol
LEFT JOIN _order_cost_GM gm
    ON ol.order_id = gm.order_id
LEFT JOIN _credits_used cu
    ON cu.order_id = ol.order_id;
