SET latest_daily_cash_update = (SELECT max(meta_update_datetime) FROM reporting.daily_cash_base_calc);

SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);

-- 1. Get acquisition and media spend updates from source table
CREATE OR REPLACE TEMPORARY TABLE _amsda_updates AS
    SELECT amsda.date_object,
    amsda.date,
    amsda.currency_object,
    amsda.currency_type,
    amsda.event_store_id,
    amsda.vip_store_id,
    amsda.is_retail_vip,
    amsda.customer_gender,
    amsda.is_cross_promo,
    amsda.finance_specialty_store,
    amsda.is_scrubs_customer,
    amsda.membership_order_type_l1,
    amsda.membership_order_type_l2,
    amsda.membership_order_type_l3,
    amsda.leads,
    amsda.primary_leads,
    amsda.reactivated_leads,
    amsda.new_vips,
    amsda.reactivated_vips,
    amsda.vips_from_reactivated_leads_m1,
    amsda.paid_vips,
    amsda.unpaid_vips,
    amsda.new_vips_m1,
    amsda.paid_vips_m1,
    amsda.cancels,
    amsda.m1_cancels,
    amsda.bop_vips,
    amsda.media_spend
    FROM analytics_base.acquisition_media_spend_daily_agg AS amsda
    WHERE $latest_daily_cash_update < amsda.meta_update_datetime
;

-- 2.1 Replace domc levels with key
CREATE OR REPLACE TEMPORARY TABLE _amsdau_with_domc_key AS
    SELECT amsdau.date_object,
    amsdau.date,
    amsdau.currency_object,
    amsdau.currency_type,
    amsdau.event_store_id,
    amsdau.vip_store_id,
    amsdau.is_retail_vip,
    amsdau.customer_gender,
    amsdau.is_cross_promo,
    amsdau.finance_specialty_store,
    amsdau.is_scrubs_customer,
    domc.order_membership_classification_key,
    amsdau.leads,
    amsdau.primary_leads,
    amsdau.reactivated_leads,
    amsdau.new_vips,
    amsdau.reactivated_vips,
    amsdau.vips_from_reactivated_leads_m1,
    amsdau.paid_vips,
    amsdau.unpaid_vips,
    amsdau.new_vips_m1,
    amsdau.paid_vips_m1,
    amsdau.cancels,
    amsdau.m1_cancels,
    amsdau.bop_vips,
    amsdau.media_spend
    FROM _amsda_updates AS amsdau
    JOIN data_model.dim_order_membership_classification AS domc
        ON amsdau.membership_order_type_l1 = domc.membership_order_type_l1
        AND amsdau.membership_order_type_l2 = domc.membership_order_type_l2
        AND amsdau.membership_order_type_l3 = domc.membership_order_type_l3
        AND domc.order_membership_classification_key in (1,2,3,4,7) -- Use original keys until membership_order_type_l4 mapping is defined
;

-- 2.2 Get report_mapping and aggregate
CREATE OR REPLACE TEMPORARY TABLE _ams_updates_with_domc_key_and_seg AS
    SELECT uwdk.date_object,
    uwdk.date,
    uwdk.currency_object,
    uwdk.currency_type,
    fsm.report_mapping,
    uwdk.order_membership_classification_key,
    SUM(uwdk.leads) AS leads,
    SUM(uwdk.primary_leads) as primary_leads,
    SUM(uwdk.reactivated_leads) AS reactivated_leads,
    SUM(uwdk.new_vips) AS new_vips,
    SUM(uwdk.reactivated_vips) AS reactivated_vips,
    SUM(uwdk.vips_from_reactivated_leads_m1) AS vips_from_reactivated_leads_m1,
    SUM(uwdk.paid_vips) AS paid_vips,
    SUM(uwdk.unpaid_vips) AS unpaid_vips,
    SUM(uwdk.new_vips_m1) AS new_vips_m1,
    SUM(uwdk.paid_vips_m1) AS paid_vips_m1,
    SUM(uwdk.cancels) AS cancels,
    SUM(uwdk.m1_cancels) AS m1_cancels,
    SUM(uwdk.bop_vips) AS bop_vips,
    SUM(uwdk.media_spend) AS media_spend
    FROM _amsdau_with_domc_key AS uwdk
    JOIN reference.finance_segment_mapping AS fsm
        ON uwdk.event_store_id = fsm.event_store_id
        AND uwdk.vip_store_id = fsm.vip_store_id
        AND uwdk.is_retail_vip = fsm.is_retail_vip
        AND uwdk.customer_gender = fsm.customer_gender
        AND uwdk.is_cross_promo = fsm.is_cross_promo
        AND uwdk.finance_specialty_store = fsm.finance_specialty_store
        AND uwdk.is_scrubs_customer = fsm.is_scrubs_customer
        AND fsm.metric_type = 'Acquisition'
        AND (
            fsm.is_daily_cash_usd = TRUE
            OR fsm.is_daily_cash_eur = TRUE
            OR fsm.report_mapping IN ('FL+SC-OREV-NA')
        )
    WHERE
        fsm.report_mapping NOT ILIKE '%RREV%'
    GROUP BY uwdk.date_object, uwdk.date, uwdk.currency_object, uwdk.currency_type, fsm.report_mapping, uwdk.order_membership_classification_key
;

-- 3.
UPDATE reporting.daily_cash_base_calc AS target
    SET target.leads = source.leads,
        target.primary_leads = source.primary_leads,
        target.reactivated_leads = source.reactivated_leads,
        target.new_vips = source.new_vips,
        target.reactivated_vips = source.reactivated_vips,
        target.vips_from_reactivated_leads_m1 = source.vips_from_reactivated_leads_m1,
        target.paid_vips = source.paid_vips,
        target.unpaid_vips = source.unpaid_vips,
        target.new_vips_m1 = source.new_vips_m1,
        target.paid_vips_m1 = source.paid_vips_m1,
        target.cancels = source.cancels,
        target.m1_cancels = source.m1_cancels,
        target.bop_vips = source.bop_vips,
        target.media_spend = source.media_spend,
        target.meta_update_datetime = $execution_start_time
    FROM _ams_updates_with_domc_key_and_seg AS source
    WHERE target.date = source.date
        AND target.date_object = source.date_object
        AND target.currency_object = source.currency_object
        AND target.currency_type = source.currency_type
        AND target.report_mapping = source.report_mapping
        AND target.order_membership_classification_key = source.order_membership_classification_key
;
