CREATE OR REPLACE TABLE edw_prod.reporting.finance_kpi_final_output AS
SELECT 'DDD Individual'  AS report_version
     , ddd.currency_type AS currency
     , fsm.report_mapping
     , fsm.business_unit
     , ddd.metric_type
     , ddd.vip_store_full_name
     , ddd.vip_store_id
     , ddd.is_retail_vip
     , ddd.event_store_brand
     , ddd.event_store_region
     , ddd.event_store_country
     , ddd.event_store_location
     , ddd.event_store_full_name
     , ddd.event_store_id
     , ddd.customer_gender
     , ddd.is_cross_promo
     , ddd.finance_specialty_store
     , ddd.is_scrubs_customer
     , ddd.date_object
     , ddd.date
     , ddd.currency_object
     , ddd.currency_type
     , ddd.membership_order_type_l1
     , ddd.membership_order_type_l2
     , ddd.membership_order_type_l3
     , ddd.product_order_count_excluding_reship_exch
     , ddd.product_order_count_including_reship_exch
     , ddd.product_order_reship_exch
     , ddd.unit_count_excluding_reship_exch
     , ddd.unit_count_including_reship_exch
     , ddd.unit_count_reship_exch
     , ddd.product_order_discount
     , ddd.outbound_shipping_costs
     , ddd.outbound_shipping_costs_reship_exch
     , ddd.return_shipping_cost
     , ddd.shipping_revenue
     , ddd.product_revenue_including_shipping
     , ddd.product_revenue_excluding_shipping
     , ddd.cash_gross_revenue
     , ddd.cash_gross_revenue_excluding_shipping
     , ddd.cash_refund_product_order
     , ddd.cash_refund_billing
     , ddd.cash_refund
     , ddd.store_credit_refund
     , ddd.chargebacks
     , ddd.chargebacks_credit_billings
     , ddd.shipping_cost
     , ddd.shipping_supplies_cost
     , ddd.shipping_cost_reship_exch
     , ddd.shipping_supplies_cost_reship_exch
     , ddd.product_cost_returned_resaleable
     , ddd.product_order_cost_product_returned_damaged_amount
     , ddd.return_unit_count
     , ddd.order_product_cost
     , ddd.order_product_cost_accounting
     , ddd.order_product_cost_excluding_reship_exch_accounting
     , ddd.order_product_cost_reship_exch
     , ddd.order_product_cost_reship
     , ddd.product_cost_net_returns
     , ddd.variable_warehouse_labor_cost
     , ddd.variable_gms_labor_cost
     , ddd.variable_payment_processing_cost
     , ddd.billed_credit_cash_transaction_amount
     , ddd.membership_fee_cash_transaction_amount
     , ddd.gift_card_transaction_amount
     , ddd.legacy_credit_cash_transaction_amount
     , ddd.billed_credit_cash_refund_chargeback_amount
     , ddd.membership_fee_cash_refund_chargeback_amount
     , ddd.gift_card_cash_refund_chargeback_amount
     , ddd.legacy_credit_cash_refund_chargeback_amount
     , ddd.billed_credit_issued_amount
     , ddd.billed_credit_redeemed_amount
     , ddd.billed_credit_cancelled_amount
     , ddd.billed_credit_expired_amount
     , ddd.billed_credit_issued_equivalent_counts
     , ddd.billed_credit_redeemed_equivalent_counts
     , ddd.billed_credit_cancelled_equivalent_counts
     , ddd.billed_credit_expired_equivalent_counts
     , ddd.billed_cash_credit_net_equivalent_count
     , ddd.refund_credit_issued_amount
     , ddd.refund_credit_redeemed_amount
     , ddd.refund_credit_cancelled_amount
     , ddd.refund_credit_expired_amount
     , ddd.other_cash_credit_issued_amount
     , ddd.other_cash_credit_redeemed_amount
     , ddd.other_cash_credit_cancelled_amount
     , ddd.other_cash_credit_expired_amount
     , ddd.noncash_credit_issued_amount
     , ddd.noncash_credit_cancelled_amount
     , ddd.noncash_credit_expired_amount
     , ddd.noncash_credit_redeemed_amount
     , ddd.billed_net_credit_billings
     , ddd.product_margin_pre_return
     , ddd.product_gross_profit
     , ddd.cash_gross_profit
     , ddd.cash_contribution_profit
     , ddd.misc_cogs_amount
     , ddd.total_cogs_amount
     , ddd.total_cogs_amount_accounting
     , ddd.discount_rate_denom
     , ddd.product_net_revenue
     , ddd.leads
     , ddd.online_leads
     , ddd.retail_leads
     , ddd.reactivated_leads
     , ddd.new_vips
     , ddd.paid_vips
     , ddd.unpaid_vips
     , ddd.reactivated_vips
     , ddd.vips_from_reactivated_leads_m1
     , ddd.new_vips_m1
     , ddd.paid_vips_m1
     , ddd.cancels AS cancels_positive
     , ddd.m1_cancels
     , ddd.bop_vips
     , ddd.media_spend
     , ddd.cumulative_leads
     , 0 as retail_attribution_retail_billed_credit_redeemed_amount
FROM edw_prod.reference.finance_segment_mapping fsm
         JOIN edw_prod.reporting.finance_kpi_base ddd ON ddd.vip_store_id = fsm.vip_store_id
    AND ddd.event_store_id = fsm.event_store_id
    AND ddd.metric_type = fsm.metric_type
    AND ddd.is_retail_vip = fsm.is_retail_vip
    AND ddd.is_cross_promo = fsm.is_cross_promo
    AND ddd.customer_gender = fsm.customer_gender
    AND fsm.is_ddd_individual = 'TRUE'
    AND ddd.date_object = 'shipped'
    AND ddd.finance_specialty_store = fsm.finance_specialty_store
    AND ddd.is_scrubs_customer = fsm.is_scrubs_customer
    AND ddd_individual_currency_type = ddd.currency_type
UNION ALL
SELECT 'DDD Consolidated' AS report_version
     , ddd.currency_type  AS currency
     , fsm.report_mapping
     , fsm.business_unit
     , ddd.metric_type
     , ddd.vip_store_full_name
     , ddd.vip_store_id
     , ddd.is_retail_vip
     , ddd.event_store_brand
     , ddd.event_store_region
     , ddd.event_store_country
     , ddd.event_store_location
     , ddd.event_store_full_name
     , ddd.event_store_id
     , ddd.customer_gender
     , ddd.is_cross_promo
     , ddd.finance_specialty_store
     , ddd.is_scrubs_customer
     , ddd.date_object
     , ddd.date
     , ddd.currency_object
     , ddd.currency_type
     , ddd.membership_order_type_l1
     , ddd.membership_order_type_l2
     , ddd.membership_order_type_l3
     , ddd.product_order_count_excluding_reship_exch
     , ddd.product_order_count_including_reship_exch
     , ddd.product_order_reship_exch
     , ddd.unit_count_excluding_reship_exch
     , ddd.unit_count_including_reship_exch
     , ddd.unit_count_reship_exch
     , ddd.product_order_discount
     , ddd.outbound_shipping_costs
     , ddd.outbound_shipping_costs_reship_exch
     , ddd.return_shipping_cost
     , ddd.shipping_revenue
     , ddd.product_revenue_including_shipping
     , ddd.product_revenue_excluding_shipping
     , ddd.cash_gross_revenue
     , ddd.cash_gross_revenue_excluding_shipping
     , ddd.cash_refund_product_order
     , ddd.cash_refund_billing
     , ddd.cash_refund
     , ddd.store_credit_refund
     , ddd.chargebacks
     , ddd.chargebacks_credit_billings
     , ddd.shipping_cost
     , ddd.shipping_supplies_cost
     , ddd.shipping_cost_reship_exch
     , ddd.shipping_supplies_cost_reship_exch
     , ddd.product_cost_returned_resaleable
     , ddd.product_order_cost_product_returned_damaged_amount
     , ddd.return_unit_count
     , ddd.order_product_cost
     , ddd.order_product_cost_accounting
     , ddd.order_product_cost_excluding_reship_exch_accounting
     , ddd.order_product_cost_reship_exch
     , ddd.order_product_cost_reship
     , ddd.product_cost_net_returns
     , ddd.variable_warehouse_labor_cost
     , ddd.variable_gms_labor_cost
     , ddd.variable_payment_processing_cost
     , ddd.billed_credit_cash_transaction_amount
     , ddd.membership_fee_cash_transaction_amount
     , ddd.gift_card_transaction_amount
     , ddd.legacy_credit_cash_transaction_amount
     , ddd.billed_credit_cash_refund_chargeback_amount
     , ddd.membership_fee_cash_refund_chargeback_amount
     , ddd.gift_card_cash_refund_chargeback_amount
     , ddd.legacy_credit_cash_refund_chargeback_amount
     , ddd.billed_credit_issued_amount
     , ddd.billed_credit_redeemed_amount
     , ddd.billed_credit_cancelled_amount
     , ddd.billed_credit_expired_amount
     , ddd.billed_credit_issued_equivalent_counts
     , ddd.billed_credit_redeemed_equivalent_counts
     , ddd.billed_credit_cancelled_equivalent_counts
     , ddd.billed_credit_expired_equivalent_counts
     , ddd.billed_cash_credit_net_equivalent_count
     , ddd.refund_credit_issued_amount
     , ddd.refund_credit_redeemed_amount
     , ddd.refund_credit_cancelled_amount
     , ddd.refund_credit_expired_amount
     , ddd.other_cash_credit_issued_amount
     , ddd.other_cash_credit_redeemed_amount
     , ddd.other_cash_credit_cancelled_amount
     , ddd.other_cash_credit_expired_amount
     , ddd.noncash_credit_issued_amount
     , ddd.noncash_credit_cancelled_amount
     , ddd.noncash_credit_expired_amount
     , ddd.noncash_credit_redeemed_amount
     , ddd.billed_net_credit_billings
     , ddd.product_margin_pre_return
     , ddd.product_gross_profit
     , ddd.cash_gross_profit
     , ddd.cash_contribution_profit
     , ddd.misc_cogs_amount
     , ddd.total_cogs_amount
     , ddd.total_cogs_amount_accounting
     , ddd.discount_rate_denom
     , ddd.product_net_revenue
     , ddd.leads
     , ddd.online_leads
     , ddd.retail_leads
     , ddd.reactivated_leads
     , ddd.new_vips
     , ddd.paid_vips
     , ddd.unpaid_vips
     , ddd.reactivated_vips
     , ddd.vips_from_reactivated_leads_m1
     , ddd.new_vips_m1
     , ddd.paid_vips_m1
     , ddd.cancels AS cancels_positive
     , ddd.m1_cancels
     , ddd.bop_vips
     , ddd.media_spend
     , ddd.cumulative_leads
     , 0 as retail_attribution_retail_billed_credit_redeemed_amount
FROM edw_prod.reference.finance_segment_mapping fsm
         JOIN edw_prod.reporting.finance_kpi_base ddd ON ddd.vip_store_id = fsm.vip_store_id
    AND ddd.event_store_id = fsm.event_store_id
    AND ddd.metric_type = fsm.metric_type
    AND ddd.is_retail_vip = fsm.is_retail_vip
    AND ddd.is_cross_promo = fsm.is_cross_promo
    AND ddd.customer_gender = fsm.customer_gender
    AND fsm.is_ddd_consolidated = 'TRUE' -- DDD Consolidated
    AND ddd.date_object = 'shipped'
    AND ddd.finance_specialty_store = fsm.finance_specialty_store
    AND ddd.is_scrubs_customer = fsm.is_scrubs_customer
    AND ddd_consolidated_currency_type = ddd.currency_type
UNION ALL
SELECT 'DDD Hyperion'                                 AS report_version
     , ddd.currency_type                              AS currency
     , fsm.report_mapping || ' ' || ddd.currency_type AS report_mapping
     , fsm.business_unit
     , ddd.metric_type
     , ddd.vip_store_full_name
     , ddd.vip_store_id
     , ddd.is_retail_vip
     , ddd.event_store_brand
     , ddd.event_store_region
     , ddd.event_store_country
     , ddd.event_store_location
     , ddd.event_store_full_name
     , ddd.event_store_id
     , ddd.customer_gender
     , ddd.is_cross_promo
     , ddd.finance_specialty_store
     , ddd.is_scrubs_customer
     , ddd.date_object
     , ddd.date
     , ddd.currency_object
     , ddd.currency_type
     , ddd.membership_order_type_l1
     , ddd.membership_order_type_l2
     , ddd.membership_order_type_l3
     , ddd.product_order_count_excluding_reship_exch
     , ddd.product_order_count_including_reship_exch
     , ddd.product_order_reship_exch
     , ddd.unit_count_excluding_reship_exch
     , ddd.unit_count_including_reship_exch
     , ddd.unit_count_reship_exch
     , ddd.product_order_discount
     , ddd.outbound_shipping_costs
     , ddd.outbound_shipping_costs_reship_exch
     , ddd.return_shipping_cost
     , ddd.shipping_revenue
     , ddd.product_revenue_including_shipping
     , ddd.product_revenue_excluding_shipping
     , ddd.cash_gross_revenue
     , ddd.cash_gross_revenue_excluding_shipping
     , ddd.cash_refund_product_order
     , ddd.cash_refund_billing
     , ddd.cash_refund
     , ddd.store_credit_refund
     , ddd.chargebacks
     , ddd.chargebacks_credit_billings
     , ddd.shipping_cost
     , ddd.shipping_supplies_cost
     , ddd.shipping_cost_reship_exch
     , ddd.shipping_supplies_cost_reship_exch
     , ddd.product_cost_returned_resaleable
     , ddd.product_order_cost_product_returned_damaged_amount
     , ddd.return_unit_count
     , ddd.order_product_cost
     , ddd.order_product_cost_accounting
     , ddd.order_product_cost_excluding_reship_exch_accounting
     , ddd.order_product_cost_reship_exch
     , ddd.order_product_cost_reship
     , ddd.product_cost_net_returns
     , ddd.variable_warehouse_labor_cost
     , ddd.variable_gms_labor_cost
     , ddd.variable_payment_processing_cost
     , ddd.billed_credit_cash_transaction_amount
     , ddd.membership_fee_cash_transaction_amount
     , ddd.gift_card_transaction_amount
     , ddd.legacy_credit_cash_transaction_amount
     , ddd.billed_credit_cash_refund_chargeback_amount
     , ddd.membership_fee_cash_refund_chargeback_amount
     , ddd.gift_card_cash_refund_chargeback_amount
     , ddd.legacy_credit_cash_refund_chargeback_amount
     , ddd.billed_credit_issued_amount
     , ddd.billed_credit_redeemed_amount
     , ddd.billed_credit_cancelled_amount
     , ddd.billed_credit_expired_amount
     , ddd.billed_credit_issued_equivalent_counts
     , ddd.billed_credit_redeemed_equivalent_counts
     , ddd.billed_credit_cancelled_equivalent_counts
     , ddd.billed_credit_expired_equivalent_counts
     , ddd.billed_cash_credit_net_equivalent_count
     , ddd.refund_credit_issued_amount
     , ddd.refund_credit_redeemed_amount
     , ddd.refund_credit_cancelled_amount
     , ddd.refund_credit_expired_amount
     , ddd.other_cash_credit_issued_amount
     , ddd.other_cash_credit_redeemed_amount
     , ddd.other_cash_credit_cancelled_amount
     , ddd.other_cash_credit_expired_amount
     , ddd.noncash_credit_issued_amount
     , ddd.noncash_credit_cancelled_amount
     , ddd.noncash_credit_expired_amount
     , ddd.noncash_credit_redeemed_amount
     , ddd.billed_net_credit_billings
     , ddd.product_margin_pre_return
     , ddd.product_gross_profit
     , ddd.cash_gross_profit
     , ddd.cash_contribution_profit
     , ddd.misc_cogs_amount
     , ddd.total_cogs_amount
     , ddd.total_cogs_amount_accounting
     , ddd.discount_rate_denom
     , ddd.product_net_revenue
     , ddd.leads
     , ddd.online_leads
     , ddd.retail_leads
     , ddd.reactivated_leads
     , ddd.new_vips
     , ddd.paid_vips
     , ddd.unpaid_vips
     , ddd.reactivated_vips
     , ddd.vips_from_reactivated_leads_m1
     , ddd.new_vips_m1
     , ddd.paid_vips_m1
     , ddd.cancels AS cancels_positive
     , ddd.m1_cancels
     , ddd.bop_vips
     , ddd.media_spend
     , ddd.cumulative_leads
     , 0 as retail_attribution_retail_billed_credit_redeemed_amount
FROM edw_prod.reference.finance_segment_mapping fsm
         JOIN edw_prod.reporting.finance_kpi_base ddd ON ddd.vip_store_id = fsm.vip_store_id
    AND ddd.event_store_id = fsm.event_store_id
    AND ddd.metric_type = fsm.metric_type
    AND ddd.is_retail_vip = fsm.is_retail_vip
    AND ddd.is_cross_promo = fsm.is_cross_promo
    AND ddd.customer_gender = fsm.customer_gender
    AND fsm.is_ddd_hyperion = 'TRUE'
    AND ddd.date_object = 'shipped'
    AND ddd.finance_specialty_store = fsm.finance_specialty_store
    AND ddd.is_scrubs_customer = fsm.is_scrubs_customer
    AND ddd_hyperion_currency_type = ddd.currency_type
    AND ((ddd.date >= mapping_start_date AND ddd.date < mapping_end_date)
        OR mapping_start_date IS NULL)
UNION ALL

SELECT 'DDD Retail Attribution'                                       AS report_version
     , ddd.currency_type                                              AS currency
     , IFF(fsm.report_mapping = 'FL+SC-RREV-US', CONCAT(fsm.report_mapping, '-', finstore_order.oracle_store_id),
           CONCAT(fsm.report_mapping, '-', finstore_vip.oracle_store_id)) AS report_mapping
     , fsm.business_unit
     , ddd.metric_type
     , ddd.vip_store_full_name
     , ddd.vip_store_id
     , ddd.is_retail_vip
     , ddd.event_store_brand
     , ddd.event_store_region
     , ddd.event_store_country
     , ddd.event_store_location
     , ddd.event_store_full_name
     , ddd.event_store_id
     , ddd.customer_gender
     , ddd.is_cross_promo
     , ddd.finance_specialty_store
     , ddd.is_scrubs_customer
     , ddd.date_object
     , ddd.date
     , ddd.currency_object
     , ddd.currency_type
     , ddd.membership_order_type_l1
     , ddd.membership_order_type_l2
     , ddd.membership_order_type_l3
     , ddd.product_order_count_excluding_reship_exch
     , ddd.product_order_count_including_reship_exch
     , ddd.product_order_reship_exch
     , ddd.unit_count_excluding_reship_exch
     , ddd.unit_count_including_reship_exch
     , ddd.unit_count_reship_exch
     , ddd.product_order_discount
     , ddd.outbound_shipping_costs
     , ddd.outbound_shipping_costs_reship_exch
     , ddd.return_shipping_cost
     , ddd.shipping_revenue
     , ddd.product_revenue_including_shipping
     , ddd.product_revenue_excluding_shipping
     , ddd.cash_gross_revenue
     , ddd.cash_gross_revenue_excluding_shipping
     , ddd.cash_refund_product_order
     , ddd.cash_refund_billing
     , ddd.cash_refund
     , ddd.store_credit_refund
     , ddd.chargebacks
     , ddd.chargebacks_credit_billings
     , ddd.shipping_cost
     , ddd.shipping_supplies_cost
     , ddd.shipping_cost_reship_exch
     , ddd.shipping_supplies_cost_reship_exch
     , ddd.product_cost_returned_resaleable
     , ddd.product_order_cost_product_returned_damaged_amount
     , ddd.return_unit_count
     , ddd.order_product_cost
     , ddd.order_product_cost_accounting
     , ddd.order_product_cost_excluding_reship_exch_accounting
     , ddd.order_product_cost_reship_exch
     , ddd.order_product_cost_reship
     , ddd.product_cost_net_returns
     , ddd.variable_warehouse_labor_cost
     , ddd.variable_gms_labor_cost
     , ddd.variable_payment_processing_cost
     , ddd.billed_credit_cash_transaction_amount
     , ddd.membership_fee_cash_transaction_amount
     , ddd.gift_card_transaction_amount
     , ddd.legacy_credit_cash_transaction_amount
     , ddd.billed_credit_cash_refund_chargeback_amount
     , ddd.membership_fee_cash_refund_chargeback_amount
     , ddd.gift_card_cash_refund_chargeback_amount
     , ddd.legacy_credit_cash_refund_chargeback_amount
     , ddd.billed_credit_issued_amount
     , ddd.billed_credit_redeemed_amount
     , ddd.billed_credit_cancelled_amount
     , ddd.billed_credit_expired_amount
     , ddd.billed_credit_issued_equivalent_counts
     , ddd.billed_credit_redeemed_equivalent_counts
     , ddd.billed_credit_cancelled_equivalent_counts
     , ddd.billed_credit_expired_equivalent_counts
     , ddd.billed_cash_credit_net_equivalent_count
     , ddd.refund_credit_issued_amount
     , ddd.refund_credit_redeemed_amount
     , ddd.refund_credit_cancelled_amount
     , ddd.refund_credit_expired_amount
     , ddd.other_cash_credit_issued_amount
     , ddd.other_cash_credit_redeemed_amount
     , ddd.other_cash_credit_cancelled_amount
     , ddd.other_cash_credit_expired_amount
     , ddd.noncash_credit_issued_amount
     , ddd.noncash_credit_cancelled_amount
     , ddd.noncash_credit_expired_amount
     , ddd.noncash_credit_redeemed_amount
     , ddd.billed_net_credit_billings
     , ddd.product_margin_pre_return
     , ddd.product_gross_profit
     , ddd.cash_gross_profit
     , ddd.cash_contribution_profit
     , ddd.misc_cogs_amount
     , ddd.total_cogs_amount
     , ddd.total_cogs_amount_accounting
     , ddd.discount_rate_denom
     , ddd.product_net_revenue
     , ddd.leads
     , ddd.online_leads
     , ddd.retail_leads
     , ddd.reactivated_leads
     , ddd.new_vips
     , ddd.paid_vips
     , ddd.unpaid_vips
     , ddd.reactivated_vips
     , ddd.vips_from_reactivated_leads_m1
     , ddd.new_vips_m1
     , ddd.paid_vips_m1
     , ddd.cancels AS cancels_positive
     , ddd.m1_cancels
     , ddd.bop_vips
     , ddd.media_spend
     , ddd.cumulative_leads
     , 0 as retail_attribution_retail_billed_credit_redeemed_amount
FROM edw_prod.reference.finance_segment_mapping fsm
         JOIN edw_prod.reporting.finance_kpi_base ddd ON ddd.vip_store_id = fsm.vip_store_id
    AND ddd.event_store_id = fsm.event_store_id
    AND ddd.metric_type = fsm.metric_type
    AND ddd.is_retail_vip = fsm.is_retail_vip
    AND ddd.is_cross_promo = fsm.is_cross_promo
    AND ddd.customer_gender = fsm.customer_gender
    AND ddd.finance_specialty_store = fsm.finance_specialty_store
    AND ddd.is_scrubs_customer = fsm.is_scrubs_customer
    AND fsm.is_retail_attribution_ddd = 'TRUE'
    AND ddd.date_object = 'shipped'
    AND retail_attribution_ddd_currency_type = ddd.currency_type
         LEFT JOIN edw_prod.reference.finance_store_mapping finstore_order ON ddd.event_store_id = finstore_order.store_id
         LEFT JOIN edw_prod.reference.finance_store_mapping finstore_vip ON ddd.vip_store_id = finstore_vip.store_id
WHERE fsm.report_mapping not like '%RREV%'
  AND IFF(fsm.report_mapping = 'FL+SC-RREV-US', CONCAT(fsm.report_mapping, '-', finstore_order.oracle_store_id),
          CONCAT(fsm.report_mapping, '-', finstore_vip.oracle_store_id)) IS NOT NULL

UNION ALL

SELECT 'DDD Retail Attribution'                                       AS report_version
     , ddd.currency_type                                              AS currency
     , fsm.report_mapping
     , fsm.business_unit
     , ddd.metric_type
     , ddd.vip_store_full_name
     , ddd.vip_store_id
     , ddd.is_retail_vip
     , ddd.event_store_brand
     , ddd.event_store_region
     , ddd.event_store_country
     , ddd.event_store_location
     , ddd.event_store_full_name
     , ddd.event_store_id
     , ddd.customer_gender
     , ddd.is_cross_promo
     , ddd.finance_specialty_store
     , ddd.is_scrubs_customer
     , ddd.date_object
     , ddd.date
     , ddd.currency_object
     , ddd.currency_type
     , ddd.membership_order_type_l1
     , ddd.membership_order_type_l2
     , ddd.membership_order_type_l3
     , ddd.product_order_count_excluding_reship_exch
     , ddd.product_order_count_including_reship_exch
     , ddd.product_order_reship_exch
     , ddd.unit_count_excluding_reship_exch
     , ddd.unit_count_including_reship_exch
     , ddd.unit_count_reship_exch
     , ddd.product_order_discount
     , ddd.outbound_shipping_costs
     , ddd.outbound_shipping_costs_reship_exch
     , ddd.return_shipping_cost
     , ddd.shipping_revenue
     , ddd.product_revenue_including_shipping
     , ddd.product_revenue_excluding_shipping
     , ddd.cash_gross_revenue
     , ddd.cash_gross_revenue_excluding_shipping
     , ddd.cash_refund_product_order
     , ddd.cash_refund_billing
     , ddd.cash_refund
     , ddd.store_credit_refund
     , ddd.chargebacks
     , ddd.chargebacks_credit_billings
     , ddd.shipping_cost
     , ddd.shipping_supplies_cost
     , ddd.shipping_cost_reship_exch
     , ddd.shipping_supplies_cost_reship_exch
     , ddd.product_cost_returned_resaleable
     , ddd.product_order_cost_product_returned_damaged_amount
     , ddd.return_unit_count
     , ddd.order_product_cost
     , ddd.order_product_cost_accounting
     , ddd.order_product_cost_excluding_reship_exch_accounting
     , ddd.order_product_cost_reship_exch
     , ddd.order_product_cost_reship
     , ddd.product_cost_net_returns
     , ddd.variable_warehouse_labor_cost
     , ddd.variable_gms_labor_cost
     , ddd.variable_payment_processing_cost
     , ddd.billed_credit_cash_transaction_amount
     , ddd.membership_fee_cash_transaction_amount
     , ddd.gift_card_transaction_amount
     , ddd.legacy_credit_cash_transaction_amount
     , ddd.billed_credit_cash_refund_chargeback_amount
     , ddd.membership_fee_cash_refund_chargeback_amount
     , ddd.gift_card_cash_refund_chargeback_amount
     , ddd.legacy_credit_cash_refund_chargeback_amount
     , ddd.billed_credit_issued_amount
     , ddd.billed_credit_redeemed_amount
     , ddd.billed_credit_cancelled_amount
     , ddd.billed_credit_expired_amount
     , ddd.billed_credit_issued_equivalent_counts
     , ddd.billed_credit_redeemed_equivalent_counts
     , ddd.billed_credit_cancelled_equivalent_counts
     , ddd.billed_credit_expired_equivalent_counts
     , ddd.billed_cash_credit_net_equivalent_count
     , ddd.refund_credit_issued_amount
     , ddd.refund_credit_redeemed_amount
     , ddd.refund_credit_cancelled_amount
     , ddd.refund_credit_expired_amount
     , ddd.other_cash_credit_issued_amount
     , ddd.other_cash_credit_redeemed_amount
     , ddd.other_cash_credit_cancelled_amount
     , ddd.other_cash_credit_expired_amount
     , ddd.noncash_credit_issued_amount
     , ddd.noncash_credit_cancelled_amount
     , ddd.noncash_credit_expired_amount
     , ddd.noncash_credit_redeemed_amount
     , ddd.billed_net_credit_billings
     , ddd.product_margin_pre_return
     , ddd.product_gross_profit
     , ddd.cash_gross_profit
     , ddd.cash_contribution_profit
     , ddd.misc_cogs_amount
     , ddd.total_cogs_amount
     , ddd.total_cogs_amount_accounting
     , ddd.discount_rate_denom
     , ddd.product_net_revenue
     , ddd.leads
     , ddd.online_leads
     , ddd.retail_leads
     , ddd.reactivated_leads
     , ddd.new_vips
     , ddd.paid_vips
     , ddd.unpaid_vips
     , ddd.reactivated_vips
     , ddd.vips_from_reactivated_leads_m1
     , ddd.new_vips_m1
     , ddd.paid_vips_m1
     , ddd.cancels AS cancels_positive
     , ddd.m1_cancels
     , ddd.bop_vips
     , ddd.media_spend
     , ddd.cumulative_leads
     , 0 as retail_attribution_retail_billed_credit_redeemed_amount
FROM edw_prod.reference.finance_segment_mapping fsm
         JOIN edw_prod.reporting.finance_kpi_base ddd ON ddd.vip_store_id = fsm.vip_store_id
    AND ddd.event_store_id = fsm.event_store_id
    AND ddd.metric_type = fsm.metric_type
    AND ddd.is_retail_vip = fsm.is_retail_vip
    AND ddd.is_cross_promo = fsm.is_cross_promo
    AND ddd.customer_gender = fsm.customer_gender
    AND ddd.finance_specialty_store = fsm.finance_specialty_store
    AND ddd.is_scrubs_customer = fsm.is_scrubs_customer
    AND fsm.is_retail_attribution_ddd = 'TRUE'
    AND ddd.date_object = 'shipped'
    AND retail_attribution_ddd_currency_type = ddd.currency_type
WHERE fsm.report_mapping not like '%RREV%'

UNION ALL


SELECT 'DDD Retail Attribution'                                       AS report_version
     , ddd.currency_type                                              AS currency
     , iff(fsm.report_mapping = 'FL+SC-W-R-RREV-US',
                                'FL+SC-W-R-OREV-US'||'-'||finstore_vip.oracle_store_id,
                                'FL+SC-M-R-OREV-US'||'-'||finstore_vip.oracle_store_id) as report_mapping
     , fsm.business_unit
     , ddd.metric_type
     , ddd.vip_store_full_name
     , ddd.vip_store_id
     , ddd.is_retail_vip
     , ddd.event_store_brand
     , ddd.event_store_region
     , ddd.event_store_country
     , ddd.event_store_location
     , ddd.event_store_full_name
     , ddd.event_store_id
     , ddd.customer_gender
     , ddd.is_cross_promo
     , ddd.finance_specialty_store
     , ddd.is_scrubs_customer
     , ddd.date_object
     , ddd.date
     , ddd.currency_object
     , ddd.currency_type
     , ddd.membership_order_type_l1
     , ddd.membership_order_type_l2
     , ddd.membership_order_type_l3
     , 0 AS product_order_count_excluding_reship_exch
     , 0 AS product_order_count_including_reship_exch
     , 0 AS product_order_reship_exch
     , 0 AS unit_count_excluding_reship_exch
     , 0 AS unit_count_including_reship_exch
     , 0 AS unit_count_reship_exch
     , 0 AS product_order_discount
     , 0 AS outbound_shipping_costs
     , 0 AS outbound_shipping_costs_reship_exch
     , 0 AS return_shipping_cost
     , 0 AS shipping_revenue
     , 0 AS product_revenue_including_shipping
     , 0 AS product_revenue_excluding_shipping
     , 0 AS cash_gross_revenue
     , 0 AS cash_gross_revenue_excluding_shipping
     , 0 AS cash_refund_product_order
     , 0 AS cash_refund_billing
     , 0 AS cash_refund
     , 0 AS store_credit_refund
     , 0 AS chargebacks
     , 0 AS chargebacks_credit_billings
     , 0 AS shipping_cost
     , 0 AS shipping_supplies_cost
     , 0 AS shipping_cost_reship_exch
     , 0 AS shipping_supplies_cost_reship_exch
     , 0 AS product_cost_returned_resaleable
     , 0 AS product_order_cost_product_returned_damaged_amount
     , 0 AS return_unit_count
     , 0 AS order_product_cost
     , 0 AS order_product_cost_accounting
     , 0 AS order_product_cost_excluding_reship_exch_accounting
     , 0 AS order_product_cost_reship_exch
     , 0 AS order_product_cost_reship
     , 0 AS product_cost_net_returns
     , 0 AS variable_warehouse_labor_cost
     , 0 AS variable_gms_labor_cost
     , 0 AS variable_payment_processing_cost
     , 0 AS billed_credit_cash_transaction_amount
     , 0 AS membership_fee_cash_transaction_amount
     , 0 AS gift_card_transaction_amount
     , 0 AS legacy_credit_cash_transaction_amount
     , 0 AS billed_credit_cash_refund_chargeback_amount
     , 0 AS membership_fee_cash_refund_chargeback_amount
     , 0 AS gift_card_cash_refund_chargeback_amount
     , 0 AS legacy_credit_cash_refund_chargeback_amount
     , 0 AS billed_credit_issued_amount
     , 0 AS billed_credit_redeemed_amount
     , 0 AS billed_credit_cancelled_amount
     , 0 AS billed_credit_expired_amount
     , 0 AS billed_credit_issued_equivalent_counts
     , 0 AS billed_credit_redeemed_equivalent_counts
     , 0 AS billed_credit_cancelled_equivalent_counts
     , 0 AS billed_credit_expired_equivalent_counts
     , 0 AS billed_cash_credit_net_equivalent_count
     , 0 AS refund_credit_issued_amount
     , 0 AS refund_credit_redeemed_amount
     , 0 AS refund_credit_cancelled_amount
     , 0 AS refund_credit_expired_amount
     , 0 AS other_cash_credit_issued_amount
     , 0 AS other_cash_credit_redeemed_amount
     , 0 AS other_cash_credit_cancelled_amount
     , 0 AS other_cash_credit_expired_amount
     , 0 AS noncash_credit_issued_amount
     , 0 AS noncash_credit_cancelled_amount
     , 0 AS noncash_credit_expired_amount
     , 0 AS noncash_credit_redeemed_amount
     , 0 AS billed_net_credit_billings
     , 0 AS product_margin_pre_return
     , 0 AS product_gross_profit
     , 0 AS cash_gross_profit
     , 0 AS cash_contribution_profit
     , 0 AS misc_cogs_amount
     , 0 AS total_cogs_amount
     , 0 AS total_cogs_amount_accounting
     , 0 AS discount_rate_denom
     , 0 AS product_net_revenue
     , 0 AS leads
     , 0 AS online_leads
     , 0 AS retail_leads
     , 0 AS reactivated_leads
     , 0 AS new_vips
     , 0 AS paid_vips
     , 0 AS unpaid_vips
     , 0 AS reactivated_vips
     , 0 AS vips_from_reactivated_leads_m1
     , 0 AS new_vips_m1
     , 0 AS paid_vips_m1
     , 0 AS cancels_positive
     , 0 AS m1_cancels
     , 0 AS bop_vips
     , 0 AS media_spend
     , 0 AS cumulative_leads
     , ddd.billed_credit_redeemed_amount as retail_attribution_retail_billed_credit_redeemed_amount
FROM edw_prod.reference.finance_segment_mapping fsm
         JOIN edw_prod.reporting.finance_kpi_base ddd ON ddd.vip_store_id = fsm.vip_store_id
    AND ddd.event_store_id = fsm.event_store_id
    AND ddd.metric_type = fsm.metric_type
    AND ddd.is_retail_vip = fsm.is_retail_vip
    AND ddd.is_cross_promo = fsm.is_cross_promo
    AND ddd.customer_gender = fsm.customer_gender
    AND ddd.finance_specialty_store = fsm.finance_specialty_store
    AND ddd.is_scrubs_customer = fsm.is_scrubs_customer
    AND fsm.is_retail_attribution_ddd = 'TRUE'
    AND ddd.date_object = 'shipped'
    AND retail_attribution_ddd_currency_type = ddd.currency_type
         LEFT JOIN edw_prod.reference.finance_store_mapping finstore_order ON ddd.event_store_id = finstore_order.store_id
         LEFT JOIN edw_prod.reference.finance_store_mapping finstore_vip ON ddd.vip_store_id = finstore_vip.store_id
         JOIN edw_prod.data_model_jfb.dim_store ds on ddd.vip_store_id  = ds.store_id and ds.store_region = 'NA'
WHERE fsm.report_mapping in ('FL+SC-W-R-RREV-US','FL+SC-M-R-RREV-US')
  AND IFF(fsm.report_mapping = 'FL+SC-W-R-RREV-US',
          'FL+SC-W-R-OREV-US' || '-' || finstore_vip.oracle_store_id,
          'FL+SC-M-R-OREV-US' || '-' || finstore_vip.oracle_store_id) IS NOT NULL

UNION ALL

SELECT 'DDD Retail Attribution'                                       AS report_version
     , ddd.currency_type                                              AS currency
     , iff(fsm.report_mapping = 'FL+SC-W-R-RREV-US', 'FL+SC-W-R-OREV-US', 'FL+SC-M-R-OREV-US') as report_mapping
     , fsm.business_unit
     , ddd.metric_type
     , ddd.vip_store_full_name
     , ddd.vip_store_id
     , ddd.is_retail_vip
     , ddd.event_store_brand
     , ddd.event_store_region
     , ddd.event_store_country
     , ddd.event_store_location
     , ddd.event_store_full_name
     , ddd.event_store_id
     , ddd.customer_gender
     , ddd.is_cross_promo
     , ddd.finance_specialty_store
     , ddd.is_scrubs_customer
     , ddd.date_object
     , ddd.date
     , ddd.currency_object
     , ddd.currency_type
     , ddd.membership_order_type_l1
     , ddd.membership_order_type_l2
     , ddd.membership_order_type_l3
     , 0 AS product_order_count_excluding_reship_exch
     , 0 AS product_order_count_including_reship_exch
     , 0 AS product_order_reship_exch
     , 0 AS unit_count_excluding_reship_exch
     , 0 AS unit_count_including_reship_exch
     , 0 AS unit_count_reship_exch
     , 0 AS product_order_discount
     , 0 AS outbound_shipping_costs
     , 0 AS outbound_shipping_costs_reship_exch
     , 0 AS return_shipping_cost
     , 0 AS shipping_revenue
     , 0 AS product_revenue_including_shipping
     , 0 AS product_revenue_excluding_shipping
     , 0 AS cash_gross_revenue
     , 0 AS cash_gross_revenue_excluding_shipping
     , 0 AS cash_refund_product_order
     , 0 AS cash_refund_billing
     , 0 AS cash_refund
     , 0 AS store_credit_refund
     , 0 AS chargebacks
     , 0 AS chargebacks_credit_billings
     , 0 AS shipping_cost
     , 0 AS shipping_supplies_cost
     , 0 AS shipping_cost_reship_exch
     , 0 AS shipping_supplies_cost_reship_exch
     , 0 AS product_cost_returned_resaleable
     , 0 AS product_order_cost_product_returned_damaged_amount
     , 0 AS return_unit_count
     , 0 AS order_product_cost
     , 0 AS order_product_cost_accounting
     , 0 AS order_product_cost_excluding_reship_exch_accounting
     , 0 AS order_product_cost_reship_exch
     , 0 AS order_product_cost_reship
     , 0 AS product_cost_net_returns
     , 0 AS variable_warehouse_labor_cost
     , 0 AS variable_gms_labor_cost
     , 0 AS variable_payment_processing_cost
     , 0 AS billed_credit_cash_transaction_amount
     , 0 AS membership_fee_cash_transaction_amount
     , 0 AS gift_card_transaction_amount
     , 0 AS legacy_credit_cash_transaction_amount
     , 0 AS billed_credit_cash_refund_chargeback_amount
     , 0 AS membership_fee_cash_refund_chargeback_amount
     , 0 AS gift_card_cash_refund_chargeback_amount
     , 0 AS legacy_credit_cash_refund_chargeback_amount
     , 0 AS billed_credit_issued_amount
     , 0 AS billed_credit_redeemed_amount
     , 0 AS billed_credit_cancelled_amount
     , 0 AS billed_credit_expired_amount
     , 0 AS billed_credit_issued_equivalent_counts
     , 0 AS billed_credit_redeemed_equivalent_counts
     , 0 AS billed_credit_cancelled_equivalent_counts
     , 0 AS billed_credit_expired_equivalent_counts
     , 0 AS billed_cash_credit_net_equivalent_count
     , 0 AS refund_credit_issued_amount
     , 0 AS refund_credit_redeemed_amount
     , 0 AS refund_credit_cancelled_amount
     , 0 AS refund_credit_expired_amount
     , 0 AS other_cash_credit_issued_amount
     , 0 AS other_cash_credit_redeemed_amount
     , 0 AS other_cash_credit_cancelled_amount
     , 0 AS other_cash_credit_expired_amount
     , 0 AS noncash_credit_issued_amount
     , 0 AS noncash_credit_cancelled_amount
     , 0 AS noncash_credit_expired_amount
     , 0 AS noncash_credit_redeemed_amount
     , 0 AS billed_net_credit_billings
     , 0 AS product_margin_pre_return
     , 0 AS product_gross_profit
     , 0 AS cash_gross_profit
     , 0 AS cash_contribution_profit
     , 0 AS misc_cogs_amount
     , 0 AS total_cogs_amount
     , 0 AS total_cogs_amount_accounting
     , 0 AS discount_rate_denom
     , 0 AS product_net_revenue
     , 0 AS leads
     , 0 AS online_leads
     , 0 AS retail_leads
     , 0 AS reactivated_leads
     , 0 AS new_vips
     , 0 AS paid_vips
     , 0 AS unpaid_vips
     , 0 AS reactivated_vips
     , 0 AS vips_from_reactivated_leads_m1
     , 0 AS new_vips_m1
     , 0 AS paid_vips_m1
     , 0 AS cancels_positive
     , 0 AS m1_cancels
     , 0 AS bop_vips
     , 0 AS media_spend
     , 0 AS cumulative_leads
     , ddd.billed_credit_redeemed_amount as retail_attribution_retail_billed_credit_redeemed_amount
FROM edw_prod.reference.finance_segment_mapping fsm
         JOIN edw_prod.reporting.finance_kpi_base ddd ON ddd.vip_store_id = fsm.vip_store_id
    AND ddd.event_store_id = fsm.event_store_id
    AND ddd.metric_type = fsm.metric_type
    AND ddd.is_retail_vip = fsm.is_retail_vip
    AND ddd.is_cross_promo = fsm.is_cross_promo
    AND ddd.customer_gender = fsm.customer_gender
    AND ddd.finance_specialty_store = fsm.finance_specialty_store
    AND ddd.is_scrubs_customer = fsm.is_scrubs_customer
    AND fsm.is_retail_attribution_ddd = 'TRUE'
    AND ddd.date_object = 'shipped'
    AND retail_attribution_ddd_currency_type = ddd.currency_type
        JOIN edw_prod.data_model_jfb.dim_store ds on ddd.vip_store_id  = ds.store_id and ds.store_region = 'NA'
WHERE fsm.report_mapping in ('FL+SC-W-R-RREV-US','FL+SC-M-R-RREV-US');
