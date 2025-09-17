
CREATE OR REPLACE TEMPORARY TABLE _orders_and_billings AS
SELECT b.date_object
     , b.date
     , b.currency_type
     , b.currency_object
     , b.store_id                                                                                       AS event_store_id
     , COALESCE(b.vip_store_id, -1)                                                                     AS vip_store_id
     , b.is_retail_vip
     , b.gender                                                                                         AS customer_gender
     , b.is_cross_promo
     , b.finance_specialty_store
     , b.is_scrubs_customer
     , mc.membership_order_type_l1
     , mc.membership_order_type_l2
     , mc.membership_order_type_l3
     /*Product Order Metrics - Order Counts*/
     , SUM(product_order_count)                                                                         AS product_order_count_excluding_reship_exch
     , SUM(product_order_count + product_order_reship_order_count +
           product_order_exchange_order_count)                                                          AS product_order_count_including_reship_exch
     , SUM(product_order_reship_order_count + product_order_exchange_order_count)                          AS product_order_reship_exch
     /* Order Metrics - Unit Counts*/
     , SUM(product_order_unit_count)                                                                    AS unit_count_excluding_reship_exch
     , SUM(product_order_unit_count + product_order_reship_unit_count +
           product_order_exchange_unit_count)                                                           AS unit_count_including_reship_exch
     , SUM(product_order_reship_unit_count + product_order_exchange_unit_count)                            AS unit_count_reship_exch
     /* Product Order Metrics - Shipped Order Discounts*/
     , SUM(product_order_product_discount_amount)                                                       AS product_order_discount
     /* Product Order Metrics - Shipping Costs*/
     , SUM(product_order_shipping_cost_amount)                                                          AS outbound_shipping_costs
     , SUM(product_order_exchange_shipping_cost_amount +
           product_order_reship_shipping_cost_amount)                                                   AS outbound_shipping_costs_reship_exch
     , SUM(product_order_return_shipping_cost_amount)                                                   AS return_shipping_cost
     /* Product Order Metrics - Shipping Revenue*/
     , SUM(product_order_shipping_revenue_amount)                                                       AS shipping_revenue
     /* Product Order Metrics - Product Revenue*/
     , SUM(product_gross_revenue)                                                                       AS product_revenue_including_shipping
     , SUM(product_gross_revenue_excl_shipping)                                                         AS product_revenue_excluding_shipping
     /*Cash Metrics - Cash Gross Revenue*/
     , SUM(cash_gross_revenue)                                                                          AS cash_gross_revenue
     , SUM(cash_gross_revenue - product_order_shipping_revenue_amount)                                  AS cash_gross_revenue_excluding_shipping
     /*Cash Metrics - Cash Refund*/
     , SUM(product_order_cash_refund_amount)                                                            AS cash_refund_product_order
     , SUM(billing_cash_refund_amount)                                                                  AS cash_refund_billing
     , SUM(product_order_cash_refund_amount + billing_cash_refund_amount)                               AS cash_refund
     /*Cash Metrics - Store Credit Refund*/
     , SUM(b.product_order_cash_credit_refund_amount)                                                   AS store_credit_refund
     /*Cash Metrics - Chargebacks*/
     , SUM(product_order_cash_chargeback_amount + billing_cash_chargeback_amount)                       AS chargebacks
     , SUM(billing_cash_chargeback_amount)                                                              AS chargebacks_credit_billings
     /*Product Costs - Shipping Costs*/
     , SUM(product_order_shipping_cost_amount)                                                          AS shipping_cost
     , SUM(product_order_shipping_supplies_cost_amount)                                                 AS shipping_supplies_cost
     , SUM(product_order_reship_shipping_cost_amount +
           product_order_exchange_shipping_cost_amount)                                                 AS shipping_cost_reship_exch
     , SUM(product_order_reship_shipping_supplies_cost_amount +
           product_order_exchange_shipping_supplies_cost_amount)                                        AS shipping_supplies_cost_reship_exch
     /*Product Costs - Returns*/
     , SUM(product_order_cost_product_returned_resaleable_amount)                                       AS product_cost_returned_resaleable
     , SUM(product_order_return_unit_count)                                                             AS return_unit_count
     , SUM(product_order_cost_product_returned_damaged_amount)                                          AS product_order_cost_product_returned_damaged_amount
     /*Product Costs - Product Cost*/
     , SUM(product_order_landed_product_cost_amount + product_order_reship_product_cost_amount +
           product_order_exchange_product_cost_amount)                                                  AS order_product_cost
     , SUM(product_order_landed_product_cost_amount_accounting + product_order_reship_product_cost_amount_accounting +
           product_order_exchange_product_cost_amount_accounting)                                       AS order_product_cost_accounting
     , SUM(product_order_landed_product_cost_amount_accounting)                                         AS order_product_cost_excluding_reship_exch_accounting
     , SUM(product_order_reship_product_cost_amount +
           product_order_exchange_product_cost_amount)                                                  AS order_product_cost_reship_exch
     , SUM(product_order_reship_product_cost_amount)                                                    AS order_product_cost_reship
     , SUM(product_order_landed_product_cost_amount +
           product_order_reship_product_cost_amount +
           product_order_exchange_product_cost_amount -
           product_order_cost_product_returned_resaleable_amount)                                       AS product_cost_net_returns
      /*VARIABLE COSTS*/
     , SUM(product_order_variable_warehouse_cost_amount)                                                AS variable_warehouse_labor_cost
     , SUM(product_order_variable_gms_cost_amount + billing_variable_gms_cost_amount)                      AS variable_gms_labor_cost
     , SUM(product_order_payment_processing_cost_amount +
           billing_payment_processing_cost_amount)                                                      AS variable_payment_processing_cost
    /*BILLING AND CREDITS*/
     , SUM(billed_credit_cash_transaction_amount)                                                       AS billed_credit_cash_transaction_amount
     , SUM(membership_fee_cash_transaction_amount)                                                      AS membership_fee_cash_transaction_amount
     , SUM(gift_card_transaction_amount)                                                                AS gift_card_transaction_amount
     , SUM(legacy_credit_cash_transaction_amount)                                                       AS legacy_credit_cash_transaction_amount
     , SUM(billed_credit_cash_refund_chargeback_amount)                                                 AS billed_credit_cash_refund_chargeback_amount
     , SUM(membership_fee_cash_refund_chargeback_amount)                                                AS membership_fee_cash_refund_chargeback_amount
     , SUM(gift_card_cash_refund_chargeback_amount)                                                     AS gift_card_cash_refund_chargeback_amount
     , SUM(legacy_credit_cash_refund_chargeback_amount)                                                 AS legacy_credit_cash_refund_chargeback_amount
     /*Credit Metrics*/
     , SUM(billed_cash_credit_issued_amount)                                                            AS billed_credit_issued_amount
     , SUM(billed_cash_credit_redeemed_amount)                                                          AS billed_credit_redeemed_amount
     , SUM(billed_cash_credit_cancelled_amount)                                                         AS billed_credit_cancelled_amount
     , SUM(billed_cash_credit_expired_amount)                                                           AS billed_credit_expired_amount
     , SUM(billed_cash_credit_issued_equivalent_count)                                                  AS billed_credit_issued_equivalent_counts
     , SUM(billed_cash_credit_redeemed_equivalent_count)                                                AS billed_credit_redeemed_equivalent_counts
     , SUM(billed_cash_credit_cancelled_equivalent_count)                                               AS billed_credit_cancelled_equivalent_counts
     , SUM(billed_cash_credit_expired_equivalent_count)                                                 AS billed_credit_expired_equivalent_counts
     , SUM(billed_cash_credit_issued_equivalent_count - billed_cash_credit_redeemed_equivalent_count -
           billed_cash_credit_cancelled_equivalent_count -
           billed_cash_credit_expired_equivalent_count)                                                 AS billed_cash_credit_net_equivalent_count
     , SUM(refund_cash_credit_issued_amount)                                                            AS refund_credit_issued_amount
     , SUM(refund_cash_credit_redeemed_amount)                                                          AS refund_credit_redeemed_amount
     , SUM(refund_cash_credit_cancelled_amount)                                                         AS refund_credit_cancelled_amount
     , SUM(refund_cash_credit_expired_amount)                                                           AS refund_credit_expired_amount
     , SUM(other_cash_credit_issued_amount)                                                             AS other_cash_credit_issued_amount
     , SUM(other_cash_credit_redeemed_amount)                                                           AS other_cash_credit_redeemed_amount
     , SUM(other_cash_credit_cancelled_amount)                                                          AS other_cash_credit_cancelled_amount
     , SUM(other_cash_credit_expired_amount)                                                            AS other_cash_credit_expired_amount
     , SUM(noncash_credit_issued_amount)                                                                AS noncash_credit_issued_amount
     , SUM(noncash_credit_cancelled_amount)                                                             AS noncash_credit_cancelled_amount
     , SUM(noncash_credit_expired_amount)                                                               AS noncash_credit_expired_amount
     , SUM(product_order_noncash_credit_redeemed_amount)                                                AS noncash_credit_redeemed_amount
     , SUM(billed_cash_credit_issued_amount - billed_cash_credit_redeemed_amount - billed_cash_credit_cancelled_amount -
           billed_cash_credit_expired_amount)                                                           AS billed_cash_credit_net_amount
     /*Financial*/
     , SUM(product_margin_pre_return)                                                                   AS product_margin_pre_return
     , SUM(product_gross_profit)                                                                        AS product_gross_profit
     , SUM (product_net_revenue)                                                                        AS product_net_revenue
     , SUM(cash_gross_profit)                                                                           AS cash_gross_profit
     , SUM(cash_variable_contribution_profit)                                                           AS cash_contribution_profit
     , SUM(b.product_order_misc_cogs_amount)                                                            AS misc_cogs_amount
     , SUM(product_order_landed_product_cost_amount + product_order_exchange_product_cost_amount +
           product_order_reship_product_cost_amount
               + product_order_shipping_cost_amount + product_order_reship_shipping_cost_amount +
           product_order_exchange_shipping_cost_amount
               + product_order_return_shipping_cost_amount
               + product_order_shipping_supplies_cost_amount + product_order_exchange_shipping_supplies_cost_amount +
           product_order_reship_shipping_supplies_cost_amount
               + product_order_misc_cogs_amount
               -product_order_cost_product_returned_resaleable_amount)                                   AS total_cogs_amount
      , SUM(product_order_landed_product_cost_amount_accounting + product_order_exchange_product_cost_amount_accounting +
           product_order_reship_product_cost_amount_accounting
               + product_order_shipping_cost_amount + product_order_reship_shipping_cost_amount +
           product_order_exchange_shipping_cost_amount
               + product_order_return_shipping_cost_amount
               + product_order_shipping_supplies_cost_amount + product_order_exchange_shipping_supplies_cost_amount +
           product_order_reship_shipping_supplies_cost_amount
               + product_order_misc_cogs_amount
               -product_order_cost_product_returned_resaleable_amount_accounting)                   AS total_cogs_amount_accounting
     , SUM(b.product_order_subtotal_excl_tariff_amount -
           product_order_noncash_credit_redeemed_amount)                                                AS discount_rate_denom
FROM analytics_base.finance_sales_ops b
         LEFT JOIN data_model_jfb.dim_order_membership_classification mc
                   ON mc.order_membership_classification_key = b.order_membership_classification_key
WHERE b.date >= '2014-01-01'
GROUP BY b.date_object
       , b.date
       , b.currency_type
       , b.currency_object
       , b.store_id
       , COALESCE(b.vip_store_id, -1)
       , b.is_retail_vip
       , b.gender
       , b.is_cross_promo
       , b.finance_specialty_store
       , b.is_scrubs_customer
       , mc.membership_order_type_l1
       , mc.membership_order_type_l2
       , mc.membership_order_type_l3
;

CREATE OR REPLACE TEMPORARY TABLE _scaffold_leads AS
SELECT *
FROM (SELECT full_date AS date
      FROM data_model_jfb.dim_date
      WHERE full_date >= '2016-01-01'
        AND full_date <= (SELECT MAX(date) FROM analytics_base.acquisition_media_spend_daily_agg)) d
         CROSS JOIN (SELECT DISTINCT date_object
                         , currency_object
                         , currency_type
                         , event_store_id
                         , vip_store_id
                         , is_retail_vip
                         , customer_gender
                         , is_cross_promo
                         , finance_specialty_store
                         , is_scrubs_customer
                         , is_retail_registration
                         , membership_order_type_l1
                         , membership_order_type_l2
                         , membership_order_type_l3
                         , registration_channel
                         , registration_subchannel
                         , how_did_you_hear
                         , how_did_you_hear_parent
                    FROM analytics_base.acquisition_media_spend_daily_agg) a;

CREATE OR REPLACE TEMPORARY TABLE _leads AS
SELECT s.date,
       s.date_object,
       s.currency_object,
       s.currency_type,
       s.event_store_id,
       s.vip_store_id,
       s.is_retail_vip,
       s.customer_gender,
       s.is_cross_promo,
       s.finance_specialty_store,
       s.is_scrubs_customer,
       s.is_retail_registration,
       s.membership_order_type_l1,
       s.membership_order_type_l2,
       s.membership_order_type_l3,
       s.registration_channel,
       s.registration_subchannel,
       s.how_did_you_hear,
       s.how_did_you_hear_parent,
       SUM(a.leads) AS leads
FROM _scaffold_leads s
LEFT JOIN analytics_base.acquisition_media_spend_daily_agg a ON a.date_object = s.date_object
    AND a.date = s.date
    AND a.currency_object = s.currency_object
    AND a.currency_type = s.currency_type
    AND a.event_store_id = s.event_store_id
    AND a.vip_store_id = s.vip_store_id
    AND a.is_retail_vip = s.is_retail_vip
    AND a.customer_gender = s.customer_gender
    AND a.is_cross_promo = s.is_cross_promo
    AND a.finance_specialty_store = s.finance_specialty_store
    AND a.is_scrubs_customer = s.is_scrubs_customer
    AND a.is_retail_registration = s.is_retail_registration
    AND a.membership_order_type_l1 = s.membership_order_type_l1
    AND a.membership_order_type_l2 = s.membership_order_type_l2
    AND a.membership_order_type_l3 = s.membership_order_type_l3
    AND a.registration_channel = s.registration_channel
    AND a.registration_subchannel = s.registration_subchannel
    AND a.how_did_you_hear = s.how_did_you_hear
    AND a.how_did_you_hear_parent = s.how_did_you_hear_parent
GROUP BY s.date,
       s.date_object,
       s.currency_object,
       s.currency_type,
       s.event_store_id,
       s.vip_store_id,
       s.is_retail_vip,
       s.customer_gender,
       s.is_cross_promo,
       s.finance_specialty_store,
       s.is_scrubs_customer,
       s.is_retail_registration,
       s.membership_order_type_l1,
       s.membership_order_type_l2,
       s.membership_order_type_l3,
       s.registration_channel,
       s.registration_subchannel,
       s.how_did_you_hear,
       s.how_did_you_hear_parent;


CREATE OR REPLACE TEMPORARY TABLE _cumulative_leads AS
SELECT date
     , date_object
     , currency_object
     , currency_type
     , event_store_id
     , vip_store_id
     , is_retail_vip
     , customer_gender
     , is_cross_promo
     , finance_specialty_store
     , is_scrubs_customer
     , is_retail_registration
     , membership_order_type_l1
     , membership_order_type_l2
     , membership_order_type_l3
     , registration_channel
     , registration_subchannel
     , how_did_you_hear
     , how_did_you_hear_parent
     , COALESCE(leads,0) as leads
     , SUM(leads) OVER (PARTITION BY date_object
                                , currency_object
                                , currency_type
                                , event_store_id
                                , vip_store_id
                                , is_retail_vip
                                , customer_gender
                                , is_cross_promo
                                , finance_specialty_store
                                , is_scrubs_customer
                                , is_retail_registration
                                , membership_order_type_l1
                                , membership_order_type_l2
                                , membership_order_type_l3
                                , registration_channel
                                , registration_subchannel
                                , how_did_you_hear
                                , how_did_you_hear_parent
                                ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_leads
FROM _leads
WHERE date >= '2016-01-01'
;

CREATE OR REPLACE TEMPORARY TABLE _acqusition AS
SELECT s.date_object
     , s.date
     , s.currency_object
     , s.currency_type
     , s.event_store_id
     , s.vip_store_id
     , s.is_retail_vip
     , s.customer_gender
     , s.is_cross_promo
     , s.finance_specialty_store
     , s.is_scrubs_customer
     , s.membership_order_type_l1
     , s.membership_order_type_l2
     , s.membership_order_type_l3
     , SUM(a.leads)                        AS leads
     , SUM(IFF(a.is_retail_registration = 0, a.leads, 0)) AS online_leads
     , SUM(IFF(a.is_retail_registration = 1, a.leads, 0)) AS retail_leads
     , SUM(reactivated_leads)              AS reactivated_leads
     , SUM(new_vips)                       AS new_vips
     , SUM(paid_vips)                      AS paid_vips
     , SUM(unpaid_vips)                    AS unpaid_vips
     , SUM(reactivated_vips)               AS reactivated_vips
     , SUM(vips_from_reactivated_leads_m1) AS vips_from_reactivated_leads_m1
     , SUM(new_vips_m1)                    AS new_vips_m1
     , SUM(paid_vips_m1)                   AS paid_vips_m1
     , SUM(cancels)                        AS cancels
     , SUM(m1_cancels)                     AS m1_cancels
     , SUM(bop_vips)                       AS bop_vips
     , SUM(media_spend)                    AS media_spend
     , SUM(cumulative_leads)               AS cumulative_leads
FROM _scaffold_leads s
LEFT JOIN analytics_base.acquisition_media_spend_daily_agg a ON s.date_object = a.date_object
    AND s.date = a.date
    AND s.currency_object = a.currency_object
    AND s.currency_type = a.currency_type
    AND s.event_store_id = a.event_store_id
    AND s.vip_store_id = a.vip_store_id
    AND s.is_retail_vip = a.is_retail_vip
    AND s.customer_gender = a.customer_gender
    AND s.is_cross_promo = a.is_cross_promo
    AND s.finance_specialty_store = a.finance_specialty_store
    AND s.is_retail_registration = a.is_retail_registration
    AND s.is_scrubs_customer = a.is_scrubs_customer
    AND s.membership_order_type_l1 = a.membership_order_type_l1
    AND s.membership_order_type_l2 = a.membership_order_type_l2
    AND s.membership_order_type_l3 = a.membership_order_type_l3
    AND s.registration_channel = a.registration_channel
    AND s.registration_subchannel = a.registration_subchannel
    AND s.how_did_you_hear = a.how_did_you_hear
    AND s.how_did_you_hear_parent = a.how_did_you_hear_parent
LEFT JOIN _cumulative_leads cl ON s.date_object = cl.date_object
    AND s.date = cl.date
    AND s.currency_object = cl.currency_object
    AND s.currency_type = cl.currency_type
    AND s.event_store_id = cl.event_store_id
    AND s.vip_store_id = cl.vip_store_id
    AND s.is_retail_vip = cl.is_retail_vip
    AND s.customer_gender = cl.customer_gender
    AND s.is_cross_promo = cl.is_cross_promo
    AND s.finance_specialty_store = cl.finance_specialty_store
    AND s.is_retail_registration = cl.is_retail_registration
    AND s.is_scrubs_customer = cl.is_scrubs_customer
    AND s.membership_order_type_l1 = cl.membership_order_type_l1
    AND s.membership_order_type_l2 = cl.membership_order_type_l2
    AND s.membership_order_type_l3 = cl.membership_order_type_l3
    AND s.registration_channel = cl.registration_channel
    AND s.registration_subchannel = cl.registration_subchannel
    AND s.how_did_you_hear = cl.how_did_you_hear
    AND s.how_did_you_hear_parent = cl.how_did_you_hear_parent
WHERE s.date >= '2014-01-01'
GROUP BY s.date_object
       , s.date
       , s.currency_object
       , s.currency_type
       , s.event_store_id
       , s.vip_store_id
       , s.is_retail_vip
       , s.customer_gender
       , s.is_cross_promo
       , s.finance_specialty_store
       , s.is_scrubs_customer
       , s.membership_order_type_l1
       , s.membership_order_type_l2
       , s.membership_order_type_l3
;



CREATE OR REPLACE TEMPORARY TABLE _scaffold AS
SELECT DISTINCT CAST('Orders' AS VARCHAR) AS metric_type,
                date_object,
                date,
                currency_type,
                currency_object,
                event_store_id,
                vip_store_id,
                is_retail_vip,
                customer_gender,
                is_cross_promo,
                finance_specialty_store,
                is_scrubs_customer,
                membership_order_type_l1,
                membership_order_type_l2,
                membership_order_type_l3
FROM _orders_and_billings
UNION
SELECT 'Acquisition' AS metric_type,
       date_object,
       date,
       currency_type,
       currency_object,
       event_store_id,
       vip_store_id,
       is_retail_vip,
       customer_gender,
       is_cross_promo,
       finance_specialty_store,
       is_scrubs_customer,
       membership_order_type_l1,
       membership_order_type_l2,
       membership_order_type_l3
FROM _acqusition;

INSERT INTO _scaffold
SELECT metric_type,
       'shipped' AS date_object,
       date,
       currency_type,
       currency_object,
       event_store_id,
       vip_store_id,
       is_retail_vip,
       customer_gender,
       is_cross_promo,
       finance_specialty_store,
       is_scrubs_customer,
       membership_order_type_l1,
       membership_order_type_l2,
       membership_order_type_l3
FROM _scaffold
WHERE metric_type = 'Acquisition';



CREATE OR REPLACE TABLE reporting.finance_kpi_base AS
SELECT s.metric_type
     , st2.store_full_name                                           AS vip_store_full_name
     , s.vip_store_id                                                AS vip_store_id
     , s.is_retail_vip                                               AS is_retail_vip
     , st.store_brand                                                AS event_store_brand
     , st.store_region                                               AS event_store_region
     , st.store_country                                              AS event_store_country
     , st.store_type                                                 AS event_store_location
     , st.store_full_name                                            AS event_store_full_name
     , s.event_store_id                                              AS event_store_id
     , s.customer_gender
     , s.is_cross_promo
     , s.finance_specialty_store
     , s.is_scrubs_customer
     , s.date_object
     , s.date                                                        AS date
     , s.currency_object
     , s.currency_type
     , s.membership_order_type_l1
     , s.membership_order_type_l2
     , s.membership_order_type_l3
     , IFNULL(product_order_count_excluding_reship_exch, 0)          AS product_order_count_excluding_reship_exch
     , IFNULL(product_order_count_including_reship_exch, 0)          AS product_order_count_including_reship_exch
     , IFNULL(product_order_reship_exch, 0)                          AS product_order_reship_exch
     , IFNULL(unit_count_excluding_reship_exch, 0)                   AS unit_count_excluding_reship_exch
     , IFNULL(unit_count_including_reship_exch, 0)                   AS unit_count_including_reship_exch
     , IFNULL(unit_count_reship_exch, 0)                             AS unit_count_reship_exch
     , IFNULL(product_order_discount, 0)                             AS product_order_discount
     , IFNULL(outbound_shipping_costs, 0)                            AS outbound_shipping_costs
     , IFNULL(outbound_shipping_costs_reship_exch, 0)                AS outbound_shipping_costs_reship_exch
     , IFNULL(return_shipping_cost, 0)                               AS return_shipping_cost
     , IFNULL(shipping_revenue, 0)                                   AS shipping_revenue
     , IFNULL(product_revenue_including_shipping, 0)                 AS product_revenue_including_shipping
     , IFNULL(product_revenue_excluding_shipping, 0)                 AS product_revenue_excluding_shipping
     , IFNULL(cash_gross_revenue, 0)                                 AS cash_gross_revenue
     , IFNULL(cash_gross_revenue_excluding_shipping, 0)              AS cash_gross_revenue_excluding_shipping
     , IFNULL(cash_refund_product_order, 0)                          AS cash_refund_product_order
     , IFNULL(cash_refund_billing, 0)                                AS cash_refund_billing
     , IFNULL(cash_refund, 0)                                        AS cash_refund
     , IFNULL(store_credit_refund, 0)                                AS store_credit_refund
     , IFNULL(chargebacks, 0)                                        AS chargebacks
     , IFNULL(chargebacks_credit_billings, 0)                        AS chargebacks_credit_billings
     , IFNULL(shipping_cost, 0)                                      AS shipping_cost
     , IFNULL(shipping_supplies_cost, 0)                             AS shipping_supplies_cost
     , IFNULL(shipping_cost_reship_exch, 0)                          AS shipping_cost_reship_exch
     , IFNULL(shipping_supplies_cost_reship_exch, 0)                 AS shipping_supplies_cost_reship_exch
     , IFNULL(product_cost_returned_resaleable, 0)                   AS product_cost_returned_resaleable
     , IFNULL(product_order_cost_product_returned_damaged_amount, 0) AS product_order_cost_product_returned_damaged_amount
     , IFNULL(return_unit_count, 0)                                  AS return_unit_count
     , IFNULL(order_product_cost, 0)                                 AS order_product_cost
     , IFNULL(order_product_cost_accounting,0)                       AS order_product_cost_accounting
     , IFNULL(order_product_cost_excluding_reship_exch_accounting,0) AS order_product_cost_excluding_reship_exch_accounting
     , IFNULL(order_product_cost_reship_exch, 0)                     AS order_product_cost_reship_exch
     , IFNULL(order_product_cost_reship, 0)                          AS order_product_cost_reship
     , IFNULL(product_cost_net_returns, 0)                           AS product_cost_net_returns
     , IFNULL(variable_warehouse_labor_cost, 0)                      AS variable_warehouse_labor_cost
     , IFNULL(variable_gms_labor_cost, 0)                            AS variable_gms_labor_cost
     , IFNULL(variable_payment_processing_cost, 0)                   AS variable_payment_processing_cost
     , IFNULL(billed_credit_cash_transaction_amount, 0)              AS billed_credit_cash_transaction_amount
     , IFNULL(membership_fee_cash_transaction_amount, 0)             AS membership_fee_cash_transaction_amount
     , IFNULL(gift_card_transaction_amount, 0)                       AS gift_card_transaction_amount
     , IFNULL(legacy_credit_cash_transaction_amount, 0)              AS legacy_credit_cash_transaction_amount
     , IFNULL(billed_credit_cash_refund_chargeback_amount, 0)        AS billed_credit_cash_refund_chargeback_amount
     , IFNULL(membership_fee_cash_refund_chargeback_amount, 0)       AS membership_fee_cash_refund_chargeback_amount
     , IFNULL(gift_card_cash_refund_chargeback_amount, 0)            AS gift_card_cash_refund_chargeback_amount
     , IFNULL(legacy_credit_cash_refund_chargeback_amount, 0)        AS legacy_credit_cash_refund_chargeback_amount
     , IFNULL(billed_credit_issued_amount, 0)                        AS billed_credit_issued_amount
     , IFNULL(billed_credit_redeemed_amount, 0)                      AS billed_credit_redeemed_amount
     , IFNULL(billed_credit_cancelled_amount, 0)                     AS billed_credit_cancelled_amount
     , IFNULL(billed_credit_expired_amount, 0)                       AS billed_credit_expired_amount
     , IFNULL(billed_credit_issued_equivalent_counts, 0)             AS billed_credit_issued_equivalent_counts
     , IFNULL(billed_credit_redeemed_equivalent_counts, 0)           AS billed_credit_redeemed_equivalent_counts
     , IFNULL(billed_credit_cancelled_equivalent_counts, 0)          AS billed_credit_cancelled_equivalent_counts
     , IFNULL(billed_credit_expired_equivalent_counts, 0)            AS billed_credit_expired_equivalent_counts
     , IFNULL(billed_cash_credit_net_equivalent_count, 0)            AS billed_cash_credit_net_equivalent_count
     , IFNULL(refund_credit_issued_amount, 0)                        AS refund_credit_issued_amount
     , IFNULL(refund_credit_redeemed_amount, 0)                      AS refund_credit_redeemed_amount
     , IFNULL(refund_credit_cancelled_amount, 0)                     AS refund_credit_cancelled_amount
     , IFNULL(refund_credit_expired_amount, 0)                       AS refund_credit_expired_amount
     , IFNULL(other_cash_credit_issued_amount, 0)                    AS other_cash_credit_issued_amount
     , IFNULL(other_cash_credit_redeemed_amount, 0)                  AS other_cash_credit_redeemed_amount
     , IFNULL(other_cash_credit_cancelled_amount, 0)                 AS other_cash_credit_cancelled_amount
     , IFNULL(other_cash_credit_expired_amount, 0)                   AS other_cash_credit_expired_amount
     , IFNULL(noncash_credit_issued_amount, 0)                       AS noncash_credit_issued_amount
     , IFNULL(noncash_credit_cancelled_amount, 0)                    AS noncash_credit_cancelled_amount
     , IFNULL(noncash_credit_expired_amount, 0)                      AS noncash_credit_expired_amount
     , IFNULL(noncash_credit_redeemed_amount, 0)                     AS noncash_credit_redeemed_amount
     , IFNULL(billed_cash_credit_net_amount, 0)                      AS billed_net_credit_billings
     , IFNULL(product_margin_pre_return, 0)                          AS product_margin_pre_return
     , IFNULL(product_gross_profit, 0)                               AS product_gross_profit
     , IFNULL(product_net_revenue, 0)                                AS product_net_revenue
     , IFNULL(cash_gross_profit, 0)                                  AS cash_gross_profit
     , IFNULL(cash_contribution_profit, 0)                           AS cash_contribution_profit
     , IFNULL(misc_cogs_amount, 0)                                   AS misc_cogs_amount
     , IFNULL(total_cogs_amount, 0)                                  AS total_cogs_amount
     , IFNULL(total_cogs_amount_accounting,0)                        AS total_cogs_amount_accounting
     , IFNULL(discount_rate_denom, 0)                                AS discount_rate_denom
     , IFNULL(leads, 0)                                              AS leads
     , IFNULL(online_leads, 0)                                       AS online_leads
     , IFNULL(retail_leads, 0)                                       AS retail_leads
     , IFNULL(reactivated_leads, 0)                                  AS reactivated_leads
     , IFNULL(new_vips, 0)                                           AS new_vips
     , IFNULL(paid_vips, 0)                                          AS paid_vips
     , IFNULL(unpaid_vips, 0)                                        AS unpaid_vips
     , IFNULL(reactivated_vips, 0)                                   AS reactivated_vips
     , IFNULL(vips_from_reactivated_leads_m1, 0)                     AS vips_from_reactivated_leads_m1
     , IFNULL(new_vips_m1, 0)                                        AS new_vips_m1
     , IFNULL(paid_vips_m1, 0)                                       AS paid_vips_m1
     , IFNULL(cancels, 0)                                            AS cancels
     , IFNULL(m1_cancels, 0)                                         AS m1_cancels
     , IFNULL(bop_vips, 0)                                           AS bop_vips
     , IFNULL(media_spend, 0)                                        AS media_spend
     , IFNULL(cumulative_leads, 0)                                   AS cumulative_leads
FROM _scaffold s
         JOIN data_model_jfb.dim_store st ON st.store_id = s.event_store_id
         JOIN data_model_jfb.dim_store st2 ON st2.store_id = s.vip_store_id
         LEFT JOIN _orders_and_billings po ON po.event_store_id = s.event_store_id
    AND po.vip_store_id = s.vip_store_id
    AND po.customer_gender = s.customer_gender
    AND po.is_retail_vip = s.is_retail_vip
    AND po.finance_specialty_store = s.finance_specialty_store
    AND po.is_scrubs_customer = s.is_scrubs_customer
    AND po.is_cross_promo = s.is_cross_promo
    AND po.date_object = s.date_object
    AND po.date = s.date
    AND po.membership_order_type_l1 = s.membership_order_type_l1
    AND po.membership_order_type_l2 = s.membership_order_type_l2
    AND po.membership_order_type_l3 = s.membership_order_type_l3
    AND po.currency_type = s.currency_type
    AND po.currency_object = s.currency_object
    AND s.metric_type = 'Orders'
         LEFT JOIN _acqusition a ON a.event_store_id = s.event_store_id
    AND a.vip_store_id = s.vip_store_id
    AND a.customer_gender = s.customer_gender
    AND a.is_cross_promo = s.is_cross_promo
    AND a.is_retail_vip = s.is_retail_vip
    AND a.finance_specialty_store = s.finance_specialty_store
    AND a.is_scrubs_customer = s.is_scrubs_customer
    AND a.date = s.date
    AND a.membership_order_type_l1 = s.membership_order_type_l1
    AND a.membership_order_type_l2 = s.membership_order_type_l2
    AND a.membership_order_type_l3 = s.membership_order_type_l3
    AND a.currency_type = s.currency_type
    AND a.currency_object = s.currency_object
    AND s.metric_type = 'Acquisition';
