SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

-- column type conversion and renaming for user convenience
-- select only the latest version of a forecast/budget/actual
CREATE OR REPLACE TEMP TABLE _finance_monthly_budget_forecast_actual__ranked AS
SELECT IFF(DATE_TRUNC('MONTH', version_date) > month and version = 'Forecast', 'Actuals', version) as version,
       TO_DATE(DATE_TRUNC('MONTH', version_date))           AS version_date,
       TO_DATE(month)                  AS financial_date,
       RTRIM(LTRIM(hyperion_store_rg)) AS hyperion_store_rg,
       RTRIM(LTRIM(bu))                AS bu,
       membership_credits_charged_count,
       membership_credits_redeemed_count,
       membership_credits_cancelled_count,
       first_msrp_revenue,
       first_discounts,
       repeat_msrp_revenue,
       repeat_discounts,
       leads,
       gross_cash_revenue,
       cash_refunds,
       chargebacks,
       net_cash_revenue_total,
       refunds_and_chargebacks_as_percent_of_gross_cash,
       cash_gross_margin,
       cash_gross_margin_percent_total,
       media_spend,
       cash_contribution_after_media,
       cpl,
       vip_cpa,
       m1_lead_to_vip,
       m1_vips,
       aged_lead_conversions,
       IFF(bu ILIKE '%orev%' AND IFNULL(paid_vips, 0) <> 0, paid_vips, total_new_vips) AS total_new_vips,
       cancels,
       net_change_in_vips,
       ending_vips,
       vip_growth_percent,
       activating_gaap_gross_revenue,
       activating_gaap_net_revenue,
       activating_order_count,
       activating_aov_incl_shipping_rev,
       activating_units_per_transaction,
       activating_discount_percent,
       activating_gross_margin_$,
       activating_gross_margin_percent,
       repeat_gaap_gross_revenue,
       repeat_gaap_net_revenue,
       repeat_order_count,
       repeat_aov_incl_shipping_rev,
       repeat_units_per_transaction,
       repeat_discount_percent,
       repeat_gaap_gross_margin_$,
       repeat_gaap_gross_margin_percent,
       repeat_net_unredeemed_credit_billings,
       repeat_refund_credits,
       repeat_other_ad_rev_div_adaptive_adj,
       repeat_net_cash_revenue,
       repeat_cash_gross_margin,
       repeat_cash_gross_margin_percent,
       membership_credits_charged,
       membership_credits_redeemed,
       membership_credits_refunded_plus_chargebacks,
       net_unredeemed_credit_billings,
       net_unredeemed_credit_billings_as_percent_of_net_cash_revenue,
       refunded_as_credit,
       refund_credit_redeemed,
       net_unredeemed_refund_credit,
       deferred_revenue,
       bom_vips,
       credit_billed_percent,
       credit_redeemed_percent,
       credit_canceled_percent,
       total_units_shipped,
       repeat_cash_purchasers,
       cash_purchase_rate,
       o_div_vip,
       ebitda_minus_cash,
       repeatplusbizdev,
       activating_units,
       repeat_units,
       repeat_product_revenue_incl_shipping,
       product_cost_calc,
       inventory_writedown,
       total_cogs_minus_cash,
       merchant_fees,
       gms_variable,
       fc_variable,
       freight_revenue,
       freight_out_cost,
       outbound_shipping_supplies,
       returns_shipping_cost,
       product_cost_markdown,
       repeat_shipped_order_cash_collected,
       acquisition_margin_$,
       acquisition_margin_percent,
       acquisition_margin_$__div__order,
       product_cost_cust_returns_calc,
       reship_exch_orders_shipped,
       reship_exch_units_shipped,
       cash_net_revenue,
       activating_product_gross_revenue,
       repeat_product_gross_revenue,
       cash_gross_margin_percent,
       repeat_units_shipped,
       repeat_orders_shipped,
       advertising_expenses,
       merchant_fee,
       selling_expenses,
       marketing_expenses,
       direct_general_and_administrative,
       total_opex,
       product_revenue,
       product_gross_margin,
       cash_gross_profit,
       retail_redemption_revenue,
       total_cogs,
       paid_vips,
       cross_promo_ft_vips,
       IFNULL(global_finance_organization,0)                AS global_finance_organization,
       IFNULL(global_ops_office_admin_and_facilities,0)     AS global_ops_office_admin_and_facilities,
       IFNULL(tfg_administration,0)                         AS tfg_administration,
       IFNULL(global_people_team,0)                         AS global_people_team,
       IFNULL(legal,0)                                      AS legal,
       IFNULL(executive,0)                                  AS executive,
       IFNULL(non_allocable_corporate,0)                    AS non_allocable_corporate,
       IFNULL(eu_media_buying,0)                            AS eu_media_buying,
       IFNULL(eu_creative_and_marketing,0)                  AS eu_creative_and_marketing,
       IFNULL(total_backoffice_step1,0)                     AS total_backoffice_step1,
       IFNULL(net_revenue_cash,0)                           AS net_revenue_cash,
       IFNULL(selling_expense,0)                            AS selling_expense,
       IFNULL(global_member_services_fixed,0)               AS global_member_services_fixed,
       IFNULL(fulfillment_centers_fixed,0)                  AS fulfillment_centers_fixed
FROM lake.fpa.monthly_budget_forecast_actual
QUALIFY RANK() OVER (PARTITION BY IFF(DATE_TRUNC('MONTH', version_date) > month and version = 'Forecast', 'Actuals', version), bu, TO_DATE(financial_date) ORDER BY TO_DATE(version_date) DESC) = 1
ORDER BY version, version_date, bu;

CREATE OR REPLACE TEMP TABLE _retail_vip AS
SELECT REPLACE(bu, 'OREV', 'RREV') AS bu_r, bu, version, version_date, financial_date, cross_promo_ft_vips
FROM _finance_monthly_budget_forecast_actual__ranked
WHERE bu ILIKE '%orev%'
  AND IFNULL(cross_promo_ft_vips,0) <> 0;

UPDATE _finance_monthly_budget_forecast_actual__ranked t
SET t.total_new_vips = s.cross_promo_ft_vips
FROM _retail_vip s
WHERE t.bu = s.bu_r
  AND t.version = s.version
  AND t.version_date = s.version_date
  AND t.financial_date = s.financial_date;

-- _store_groupings table for aggregating two segments into one segment
CREATE OR REPLACE TEMP TABLE _store_groupings
(
    store_grouping VARCHAR(50),
    store_abbrev VARCHAR(100)
);

INSERT INTO _store_groupings
    (store_grouping, store_abbrev)
VALUES ('FLWW-FVIP-TREV', 'FLNA-WVIP'),
       ('FLWW-FVIP-TREV', 'FLEU-WVIP'),
       ('FLWW-MVIP-TREV', 'FLNA-MVIP'),
       ('FLWW-MVIP-TREV', 'FLEU-MVIP'),
       ('FLCWW-FVIP-TREV', 'FLNA-WVIP'),
       ('FLCWW-FVIP-TREV', 'FLEU-WVIP'),
       ('FLCWW-FVIP-TREV', 'YTNA-OREV'),
       ('FLCWW-MVIP-TREV', 'FLNA-MVIP'),
       ('FLCWW-MVIP-TREV', 'FLEU-MVIP'),
       ('FLWW-OREV', 'FLEU-FVIP-OREV'),
       ('FLWW-OREV', 'FLEU-MVIP-OREV'),
       ('FLWW-OREV', 'FLNA-OREV'),
       ('FLCWW-OREV', 'FLEU-FVIP-OREV'),
       ('FLCWW-OREV', 'FLEU-MVIP-OREV'),
       ('FLCWW-OREV', 'FLNA-OREV'),
       ('FLCWW-OREV', 'YTNA-OREV'),
       ('FLCNA-TREV', 'FLNA-TREV'),
       ('FLCNA-TREV', 'YTNA-OREV'),
       ('FLWW-RREV', 'FLNA-RREV'),
       ('FLWW-RREV', 'FLEU-RREV'),
       ('FLDE-OREV', 'FLDE-FVIP-OREV'),
       ('FLDE-OREV', 'FLDE-MVIP-OREV'),
       ('GFBWW-TREV', 'JFEU'),
       ('GFBWW-TREV', 'JFNA'),
       ('GFBWW-TREV', 'SDNA'),
       ('GFBWW-TREV', 'FKNA'),
       ('SCWW-TREV', 'SCMNA-TREV'),
       ('SCWW-TREV', 'SCWNA-TREV'),
       ('JFBNA-TREV', 'SDNA'),
       ('JFBNA-TREV', 'FKNA'),
       ('JFBNA-TREV', 'JFNA'),
       ('JFBWW-TREV', 'SDNA'),
       ('JFBWW-TREV', 'FKNA'),
       ('JFBWW-TREV', 'JFNA'),
       ('JFBWW-TREV', 'JFEU'),
       ('JFSDNA-TREV', 'JFNA'),
       ('JFSDNA-TREV', 'SDNA'),
       ('FLC-O-NA', 'FLNA-OREV'),
       ('FLC-O-NA', 'YTNA-OREV'),
       ('FLUK-OREV', 'FLUK-FVIP-OREV'),
       ('FLUK-OREV', 'FLUK-MVIP-OREV');

-- _store_groupings_sub table for subtracting two segments into one segment
CREATE OR REPLACE TEMP TABLE _store_groupings_sub
(
    store_grouping VARCHAR(50),
    store_abbrev VARCHAR(100),
    store_order NUMERIC
);

INSERT INTO _store_groupings_sub
    (store_grouping, store_abbrev, store_order)
VALUES ('FLNA-FVIP-FL-OREV', 'FLNA-FVIP-OREV', 1),
       ('FLNA-FVIP-FL-OREV', 'SCWNA-TREV', 2),
       ('FLNA-MVIP-FL-OREV', 'FLNA-MVIP-OREV', 1),
       ('FLNA-MVIP-FL-OREV', 'SCMNA-TREV', 2),
       ('FLNA-FL-OREV', 'FLNA-OREV', 1),
       ('FLNA-FL-OREV', 'SCWW-TREV', 2),
       ('FLNA-FL-TREV','FLNA-TREV',1),
       ('FLNA-FL-TREV','SCWW-TREV',2),
       ('FLNA-MVIP-FL-TREV','FLNA-MVIP',1),
       ('FLNA-MVIP-FL-TREV','SCMNA-TREV',2),
       ('FLNA-FVIP-FL-TREV','FLNA-WVIP',1),
       ('FLNA-FVIP-FL-TREV','SCWNA-TREV',2);


CREATE OR REPLACE TEMP TABLE _finance_monthly_budget_forecast_actual__existing_agg AS
   SELECT DISTINCT bu, version_date, version FROM _finance_monthly_budget_forecast_actual__ranked
   WHERE bu IN (SELECT store_grouping FROM _store_groupings);

-- start aggregation:
CREATE OR REPLACE TEMP TABLE _finance_monthly_budget_forecast_actual__aggregated AS
SELECT fmb.version,
       fmb.version_date,
       financial_date,
       sg.store_grouping                                                                    AS hyperion_store_rg,
       sg.store_grouping                                                                    AS bu,
       SUM(membership_credits_charged_count)                                                AS membership_credits_charged_count,
       SUM(membership_credits_redeemed_count)                                               AS membership_credits_redeemed_count,
       SUM(membership_credits_cancelled_count)                                              AS membership_credits_cancelled_count,
       SUM(first_msrp_revenue)                                                              AS first_msrp_revenue,
       SUM(first_discounts)                                                                 AS first_discounts,
       SUM(repeat_msrp_revenue)                                                             AS repeat_msrp_revenue,
       SUM(repeat_discounts)                                                                AS repeat_discounts,
       SUM(leads)                                                                           AS leads,
       SUM(gross_cash_revenue)                                                              AS gross_cash_revenue,
       SUM(cash_refunds)                                                                    AS cash_refunds,
       SUM(chargebacks)                                                                     AS chargebacks,
       SUM(net_cash_revenue_total)                                                          AS net_cash_revenue_total,
       SUM(cash_gross_margin)                                                               AS cash_gross_margin,
       SUM(media_spend)                                                                     AS media_spend,
       SUM(cash_contribution_after_media)                                                   AS cash_contribution_after_media,
       SUM(m1_lead_to_vip)                                                                  AS m1_lead_to_vip,
       SUM(m1_vips)                                                                         AS m1_vips,
       SUM(aged_lead_conversions)                                                           AS aged_lead_conversions,
       SUM(total_new_vips)                                                                  AS total_new_vips,
       SUM(cancels)                                                                         AS cancels,
       SUM(net_change_in_vips)                                                              AS net_change_in_vips,
       SUM(ending_vips)                                                                     AS ending_vips,
       SUM(activating_gaap_gross_revenue)                                                   AS activating_gaap_gross_revenue,
       SUM(activating_gaap_net_revenue)                                                     AS activating_gaap_net_revenue,
       SUM(activating_order_count)                                                          AS activating_order_count,
       SUM(activating_aov_incl_shipping_rev)                                                AS activating_aov_incl_shipping_rev,
       SUM(activating_units_per_transaction)                                                AS activating_units_per_transaction,
       SUM(activating_gross_margin_$)                                                       AS activating_gross_margin_$,
       SUM(repeat_gaap_gross_revenue)                                                       AS repeat_gaap_gross_revenue,
       SUM(repeat_gaap_net_revenue)                                                         AS repeat_gaap_net_revenue,
       SUM(repeat_order_count)                                                              AS repeat_order_count,
       SUM(repeat_aov_incl_shipping_rev)                                                    AS repeat_aov_incl_shipping_rev,
       SUM(repeat_units_per_transaction)                                                    AS repeat_units_per_transaction,
       SUM(repeat_gaap_gross_margin_$)                                                      AS repeat_gaap_gross_margin_$,
       SUM(repeat_net_unredeemed_credit_billings)                                           AS repeat_net_unredeemed_credit_billings,
       SUM(repeat_refund_credits)                                                           AS repeat_refund_credits,
       SUM(repeat_other_ad_rev_div_adaptive_adj)                                            AS repeat_other_ad_rev_div_adaptive_adj,
       SUM(repeat_net_cash_revenue)                                                         AS repeat_net_cash_revenue,
       SUM(repeat_cash_gross_margin)                                                        AS repeat_cash_gross_margin,
       SUM(membership_credits_charged)                                                      AS membership_credits_charged,
       SUM(membership_credits_redeemed)                                                     AS membership_credits_redeemed,
       SUM(membership_credits_refunded_plus_chargebacks)                                    AS membership_credits_refunded_plus_chargebacks,
       SUM(net_unredeemed_credit_billings)                                                  AS net_unredeemed_credit_billings,
       SUM(refunded_as_credit)                                                              AS refunded_as_credit,
       SUM(refund_credit_redeemed)                                                          AS refund_credit_redeemed,
       SUM(net_unredeemed_refund_credit)                                                    AS net_unredeemed_refund_credit,
       SUM(deferred_revenue)                                                                AS deferred_revenue,
       SUM(bom_vips)                                                                        AS bom_vips,
       SUM(total_units_shipped)                                                             AS total_units_shipped,
       SUM(repeat_cash_purchasers)                                                          AS repeat_cash_purchasers,
       SUM(ebitda_minus_cash)                                                               AS ebitda_minus_cash,
       SUM(repeatplusbizdev)                                                                AS repeatplusbizdev,                                              -- needs work
       SUM(activating_units)                                                                AS activating_units,
       SUM(repeat_units)                                                                    AS repeat_units,
       SUM(repeat_product_revenue_incl_shipping)                                            AS repeat_product_revenue_incl_shipping,
       SUM(product_cost_calc)                                                               AS product_cost_calc,
       SUM(inventory_writedown)                                                             AS inventory_writedown,
       SUM(total_cogs_minus_cash)                                                           AS total_cogs_minus_cash,
       SUM(merchant_fees)                                                                   AS merchant_fees,
       SUM(gms_variable)                                                                    AS gms_variable,
       SUM(fc_variable)                                                                     AS fc_variable,
       SUM(freight_revenue)                                                                 AS freight_revenue,
       SUM(freight_out_cost)                                                                AS freight_out_cost,
       SUM(outbound_shipping_supplies)                                                      AS outbound_shipping_supplies,
       SUM(returns_shipping_cost)                                                           AS returns_shipping_cost,
       SUM(product_cost_markdown)                                                           AS product_cost_markdown,
       SUM(repeat_shipped_order_cash_collected)                                             AS repeat_shipped_order_cash_collected,
       SUM(acquisition_margin_$)                                                            AS acquisition_margin_$,
       SUM(product_cost_cust_returns_calc)                                                  AS product_cost_cust_returns_calc,
       SUM(reship_exch_orders_shipped)                                                      AS reship_exch_orders_shipped,
       SUM(reship_exch_units_shipped)                                                       AS reship_exch_units_shipped,
       SUM(cash_net_revenue)                                                                AS cash_net_revenue,
       SUM(activating_product_gross_revenue)                                                AS activating_product_gross_revenue,
       SUM(repeat_product_gross_revenue)                                                    AS repeat_product_gross_revenue,
       SUM(repeat_units_shipped)                                                            AS repeat_units_shipped,
       SUM(repeat_orders_shipped)                                                           AS repeat_orders_shipped,
       SUM(advertising_expenses)                                                            AS advertising_expenses,
       SUM(merchant_fee)                                                                    AS merchant_fee,
       SUM(selling_expenses)                                                                AS selling_expenses,
       SUM(marketing_expenses)                                                              AS marketing_expenses,
       SUM(direct_general_and_administrative)                                               AS direct_general_and_administrative,
       SUM(total_opex)                                                                      AS total_opex,
       SUM(product_revenue)                                                                 AS product_revenue,
       SUM(product_gross_margin)                                                            AS product_gross_margin,
       SUM(cash_gross_profit)                                                               AS cash_gross_profit,
       SUM(retail_redemption_revenue)                                                       AS retail_redemption_revenue,
       SUM(total_cogs)                                                                      AS total_cogs,
       SUM(paid_vips)                                                                       AS paid_vips,
       SUM(cross_promo_ft_vips)                                                             AS cross_promo_ft_vips,
       SUM(global_finance_organization)                                                     AS global_finance_organization,
       SUM(global_ops_office_admin_and_facilities)                                          AS global_ops_office_admin_and_facilities,
       SUM(tfg_administration)                                                              AS tfg_administration,
       SUM(global_people_team)                                                              AS global_people_team,
       SUM(legal)                                                                           AS legal,
       SUM(executive)                                                                       AS executive,
       SUM(non_allocable_corporate)                                                         AS non_allocable_corporate,
       SUM(eu_media_buying)                                                                 AS eu_media_buying,
       SUM(eu_creative_and_marketing)                                                       AS eu_creative_and_marketing,
       SUM(total_backoffice_step1)                                                          AS total_backoffice_step1,
       SUM(net_revenue_cash)                                                                AS net_revenue_cash,
       SUM(selling_expense)                                                                 AS selling_expense,
       SUM(global_member_services_fixed)                                                    AS global_member_services_fixed,
       SUM(fulfillment_centers_fixed)                                                       AS fulfillment_centers_fixed,
-- Composites
       (SUM(ABS(cash_refunds)) + SUM(ABS(chargebacks))) /
       NULLIFZERO(SUM(gross_cash_revenue))                                                  AS refunds_and_chargebacks_as_percent_of_gross_cash,              -- needs work
       SUM(cash_gross_margin) / NULLIFZERO(SUM(net_cash_revenue_total))                     AS cash_gross_margin_percent_total,                               -- needs work
       SUM(media_spend) / NULLIFZERO(SUM(leads))                                            AS cpl,                                                           -- needs work
       SUM(media_spend) / NULLIFZERO(SUM(total_new_vips))                                   AS vip_cpa,                                                       -- needs work
       SUM(net_change_in_vips) / NULLIFZERO(SUM(bom_vips))                                  AS vip_growth_percent,                                            -- needs work
       SUM(ABS(first_discounts)) / NULLIFZERO((SUM(ABS(first_discounts)) +
                                               SUM(ABS(activating_gaap_gross_revenue))))    AS activating_discount_percent,                                   -- needs work
       SUM(activating_gross_margin_$) /
       NULLIFZERO(SUM(activating_gaap_net_revenue))                                         AS activating_gross_margin_percent,                               -- needs work
       ABS(SUM(repeat_discounts)) / NULLIFZERO((SUM(ABS(repeat_discounts)) +
                                                SUM(repeat_product_revenue_incl_shipping))) AS repeat_discount_percent,                                       -- needs work, SHOULD WE USE        repeat_gaap_gross_revenue, repeat_product_revenue_incl_shipping,
       SUM(repeat_gaap_gross_margin_$) /
       NULLIFZERO(SUM(repeat_gaap_net_revenue))                                             AS repeat_gaap_gross_margin_percent,                              -- needs work
       SUM(repeat_cash_gross_margin) /
       NULLIFZERO(SUM(repeat_net_cash_revenue))                                             AS repeat_cash_gross_margin_percent,                              -- needs work

       SUM(net_unredeemed_credit_billings) /
       NULLIFZERO(SUM(cash_net_revenue))                                                    AS net_unredeemed_credit_billings_as_percent_of_net_cash_revenue, -- needs work
       SUM(membership_credits_charged_count) / NULLIFZERO(SUM(bom_vips))                    AS credit_billed_percent,                                         -- needs work
       SUM(membership_credits_redeemed_count) /
       NULLIFZERO(SUM(membership_credits_charged_count))                                    AS credit_redeemed_percent,                                       -- needs work
       SUM(membership_credits_cancelled_count) /
       NULLIFZERO(SUM(membership_credits_charged_count))                                    AS credit_canceled_percent,                                       -- needs work
       SUM(repeat_cash_purchasers) / NULLIFZERO(SUM(bom_vips))                              AS cash_purchase_rate,                                            -- needs work
       SUM(repeat_order_count) / NULLIFZERO(SUM(bom_vips))                                  AS o_div_vip,                                                     -- needs work
       SUM(cash_gross_margin) / NULLIFZERO(SUM(cash_net_revenue))                           AS cash_gross_margin_percent,
       SUM(acquisition_margin_$)/NULLIFZERO(SUM(activating_gaap_gross_revenue))             AS acquisition_margin_percent,
       0.0000                                                                               AS acquisition_margin_$__div__order                               -- needs work
FROM _finance_monthly_budget_forecast_actual__ranked fmb
JOIN _store_groupings sg
ON fmb.bu = sg.store_abbrev
GROUP BY fmb.version,
         fmb.version_date,
         financial_date,
         sg.store_grouping;

DELETE
FROM _finance_monthly_budget_forecast_actual__aggregated a
    USING _finance_monthly_budget_forecast_actual__existing_agg b
WHERE a.bu = b.bu
  AND a.version = b.version
  AND a.version_date = b.version_date;

INSERT INTO _finance_monthly_budget_forecast_actual__ranked (
        version,
        version_date,
        financial_date,
        hyperion_store_rg,
        bu,
        membership_credits_charged_count,
        membership_credits_redeemed_count,
        membership_credits_cancelled_count,
        first_msrp_revenue,
        first_discounts,
        repeat_msrp_revenue,
        repeat_discounts,
        leads,
        gross_cash_revenue,
        cash_refunds,
        chargebacks,
        net_cash_revenue_total,
        refunds_and_chargebacks_as_percent_of_gross_cash,
        cash_gross_margin,
        cash_gross_margin_percent_total,
        media_spend,
        cash_contribution_after_media,
        cpl,
        vip_cpa,
        m1_lead_to_vip,
        m1_vips,
        aged_lead_conversions,
        total_new_vips,
        cancels,
        net_change_in_vips,
        ending_vips,
        vip_growth_percent,
        activating_gaap_gross_revenue,
        activating_gaap_net_revenue,
        activating_order_count,
        activating_aov_incl_shipping_rev,
        activating_units_per_transaction,
        activating_discount_percent,
        activating_gross_margin_$,
        activating_gross_margin_percent,
        repeat_gaap_gross_revenue,
        repeat_gaap_net_revenue,
        repeat_order_count,
        repeat_aov_incl_shipping_rev,
        repeat_units_per_transaction,
        repeat_discount_percent,
        repeat_gaap_gross_margin_$,
        repeat_gaap_gross_margin_percent,
        repeat_net_unredeemed_credit_billings,
        repeat_refund_credits,
        repeat_other_ad_rev_div_adaptive_adj,
        repeat_net_cash_revenue,
        repeat_cash_gross_margin,
        repeat_cash_gross_margin_percent,
        membership_credits_charged,
        membership_credits_redeemed,
        membership_credits_refunded_plus_chargebacks,
        net_unredeemed_credit_billings,
        net_unredeemed_credit_billings_as_percent_of_net_cash_revenue,
        refunded_as_credit,
        refund_credit_redeemed,
        net_unredeemed_refund_credit,
        deferred_revenue,
        bom_vips,
        credit_billed_percent,
        credit_redeemed_percent,
        credit_canceled_percent,
        total_units_shipped,
        repeat_cash_purchasers,
        cash_purchase_rate,
        o_div_vip,
        ebitda_minus_cash,
        repeatplusbizdev,
        activating_units,
        repeat_units,
        repeat_product_revenue_incl_shipping,
        product_cost_calc,
        inventory_writedown,
        total_cogs_minus_cash,
        merchant_fees,
        gms_variable,
        fc_variable,
        freight_revenue,
        freight_out_cost,
        outbound_shipping_supplies,
        returns_shipping_cost,
        product_cost_markdown,
        repeat_shipped_order_cash_collected,
        acquisition_margin_$,
        acquisition_margin_percent,
        acquisition_margin_$__div__order,
        product_cost_cust_returns_calc,
        reship_exch_orders_shipped,
        reship_exch_units_shipped,
        cash_net_revenue,
        activating_product_gross_revenue,
        repeat_product_gross_revenue,
        cash_gross_margin_percent,
        repeat_units_shipped,
        repeat_orders_shipped,
        advertising_expenses,
        merchant_fee,
        selling_expenses,
        marketing_expenses,
        direct_general_and_administrative,
        total_opex,
        product_revenue,
        product_gross_margin,
        cash_gross_profit,
        retail_redemption_revenue,
        total_cogs,
        paid_vips,
        cross_promo_ft_vips,
        global_finance_organization,
        global_ops_office_admin_and_facilities,
        tfg_administration,
        global_people_team,
        legal,
        executive,
        non_allocable_corporate,
        eu_media_buying,
        eu_creative_and_marketing,
        total_backoffice_step1,
        net_revenue_cash,
        selling_expense,
        global_member_services_fixed,
        fulfillment_centers_fixed
)
SELECT version,
       version_date,
       financial_date,
       hyperion_store_rg,
       bu,
       membership_credits_charged_count,
       membership_credits_redeemed_count,
       membership_credits_cancelled_count,
       first_msrp_revenue,
       first_discounts,
       repeat_msrp_revenue,
       repeat_discounts,
       leads,
       gross_cash_revenue,
       cash_refunds,
       chargebacks,
       net_cash_revenue_total,
       refunds_and_chargebacks_as_percent_of_gross_cash,
       cash_gross_margin,
       cash_gross_margin_percent_total,
       media_spend,
       cash_contribution_after_media,
       cpl,
       vip_cpa,
       m1_lead_to_vip,
       m1_vips,
       aged_lead_conversions,
       total_new_vips,
       cancels,
       net_change_in_vips,
       ending_vips,
       vip_growth_percent,
       activating_gaap_gross_revenue,
       activating_gaap_net_revenue,
       activating_order_count,
       activating_aov_incl_shipping_rev,
       activating_units_per_transaction,
       activating_discount_percent,
       activating_gross_margin_$,
       activating_gross_margin_percent,
       repeat_gaap_gross_revenue,
       repeat_gaap_net_revenue,
       repeat_order_count,
       repeat_aov_incl_shipping_rev,
       repeat_units_per_transaction,
       repeat_discount_percent,
       repeat_gaap_gross_margin_$,
       repeat_gaap_gross_margin_percent,
       repeat_net_unredeemed_credit_billings,
       repeat_refund_credits,
       repeat_other_ad_rev_div_adaptive_adj,
       repeat_net_cash_revenue,
       repeat_cash_gross_margin,
       repeat_cash_gross_margin_percent,
       membership_credits_charged,
       membership_credits_redeemed,
       membership_credits_refunded_plus_chargebacks,
       net_unredeemed_credit_billings,
       net_unredeemed_credit_billings_as_percent_of_net_cash_revenue,
       refunded_as_credit,
       refund_credit_redeemed,
       net_unredeemed_refund_credit,
       deferred_revenue,
       bom_vips,
       credit_billed_percent,
       credit_redeemed_percent,
       credit_canceled_percent,
       total_units_shipped,
       repeat_cash_purchasers,
       cash_purchase_rate,
       o_div_vip,
       ebitda_minus_cash,
       repeatplusbizdev,
       activating_units,
       repeat_units,
       repeat_product_revenue_incl_shipping,
       product_cost_calc,
       inventory_writedown,
       total_cogs_minus_cash,
       merchant_fees,
       gms_variable,
       fc_variable,
       freight_revenue,
       freight_out_cost,
       outbound_shipping_supplies,
       returns_shipping_cost,
       product_cost_markdown,
       repeat_shipped_order_cash_collected,
       acquisition_margin_$,
       acquisition_margin_percent,
       acquisition_margin_$__div__order,
       product_cost_cust_returns_calc,
       reship_exch_orders_shipped,
       reship_exch_units_shipped,
       cash_net_revenue,
       activating_product_gross_revenue,
       repeat_product_gross_revenue,
       cash_gross_margin_percent,
       repeat_units_shipped,
       repeat_orders_shipped,
       advertising_expenses,
       merchant_fee,
       selling_expenses,
       marketing_expenses,
       direct_general_and_administrative,
       total_opex,
       product_revenue,
       product_gross_margin,
       cash_gross_profit,
       retail_redemption_revenue,
       total_cogs,
       paid_vips,
       cross_promo_ft_vips,
       global_finance_organization,
       global_ops_office_admin_and_facilities,
       tfg_administration,
       global_people_team,
       legal,
       executive,
       non_allocable_corporate,
       eu_media_buying,
       eu_creative_and_marketing,
       total_backoffice_step1,
       net_revenue_cash,
       selling_expense,
       global_member_services_fixed,
       fulfillment_centers_fixed
FROM _finance_monthly_budget_forecast_actual__aggregated;

-- Subtracting two segments for a different segment
CREATE OR REPLACE TEMP TABLE _finance_monthly_budget_forecast_actual__subtracted AS
SELECT fmb1.version
     , fmb1.version_date
     , fmb1.financial_date
     , fmb1.store_grouping                                                     AS hyperion_store_rg
     , fmb1.store_grouping                                                     AS bu
     , COALESCE(fmb1.membership_credits_charged_count, 0) -
       COALESCE(fmb2.membership_credits_charged_count, 0)                      AS membership_credits_charged_count
     , COALESCE(fmb1.membership_credits_redeemed_count, 0) -
       COALESCE(fmb2.membership_credits_redeemed_count, 0)                     AS membership_credits_redeemed_count
     , COALESCE(fmb1.membership_credits_cancelled_count, 0) -
       COALESCE(fmb2.membership_credits_cancelled_count, 0)                    AS membership_credits_cancelled_count
     , COALESCE(fmb1.first_msrp_revenue, 0) -
       COALESCE(fmb2.first_msrp_revenue, 0)                                    AS first_msrp_revenue
     , COALESCE(fmb1.first_discounts, 0) - COALESCE(fmb2.first_discounts, 0)   AS first_discounts
     , COALESCE(fmb1.repeat_msrp_revenue, 0) -
       COALESCE(fmb2.repeat_msrp_revenue, 0)                                   AS repeat_msrp_revenue
     , COALESCE(fmb1.repeat_discounts, 0) - COALESCE(fmb2.repeat_discounts, 0) AS repeat_discounts
     , COALESCE(fmb1.leads, 0) - COALESCE(fmb2.leads, 0)                       AS leads
     , COALESCE(fmb1.gross_cash_revenue, 0) -
       COALESCE(fmb2.gross_cash_revenue, 0)                                    AS gross_cash_revenue
     , COALESCE(fmb1.cash_refunds, 0) - COALESCE(fmb2.cash_refunds, 0)         AS cash_refunds
     , COALESCE(fmb1.chargebacks, 0) - COALESCE(fmb2.chargebacks, 0)           AS chargebacks
     , COALESCE(fmb1.net_cash_revenue_total, 0) -
       COALESCE(fmb2.net_cash_revenue_total, 0)                                AS net_cash_revenue_total
     , COALESCE(fmb1.cash_gross_margin, 0) -
       COALESCE(fmb2.cash_gross_margin, 0)                                     AS cash_gross_margin
     , COALESCE(fmb1.media_spend, 0) - COALESCE(fmb2.media_spend, 0)           AS media_spend
     , COALESCE(fmb1.m1_lead_to_vip, 0) - COALESCE(fmb2.m1_lead_to_vip, 0)     AS m1_lead_to_vip
     , COALESCE(fmb1.m1_vips, 0) - COALESCE(fmb2.m1_vips, 0)                   AS m1_vips
     , COALESCE(fmb1.aged_lead_conversions, 0) -
       COALESCE(fmb2.aged_lead_conversions, 0)                                 AS aged_lead_conversions
     , COALESCE(fmb1.total_new_vips, 0) - COALESCE(fmb2.total_new_vips, 0)     AS total_new_vips
     , COALESCE(fmb1.cancels, 0) - COALESCE(fmb2.cancels, 0)                   AS cancels
     , COALESCE(fmb1.cash_contribution_after_media, 0) -
       COALESCE(fmb2.cash_contribution_after_media, 0)                           AS cash_contribution_after_media
     , COALESCE(fmb1.net_change_in_vips, 0) -
       COALESCE(fmb2.net_change_in_vips, 0)                                      AS net_change_in_vips
     , COALESCE(fmb1.ending_vips, 0) - COALESCE(fmb2.ending_vips, 0)           AS ending_vips
     , COALESCE(fmb1.activating_gaap_gross_revenue, 0) -
       COALESCE(fmb2.activating_gaap_gross_revenue, 0)                         AS activating_gaap_gross_revenue
     , COALESCE(fmb1.activating_gaap_net_revenue, 0) -
       COALESCE(fmb2.activating_gaap_net_revenue, 0)                           AS activating_gaap_net_revenue
     , COALESCE(fmb1.activating_order_count, 0) -
       COALESCE(fmb2.activating_order_count, 0)                                AS activating_order_count
     , COALESCE(fmb1.activating_aov_incl_shipping_rev, 0) -
       COALESCE(fmb2.activating_aov_incl_shipping_rev, 0)                      AS activating_aov_incl_shipping_rev
     , COALESCE(fmb1.activating_units_per_transaction, 0) -
       COALESCE(fmb2.activating_units_per_transaction, 0)                      AS activating_units_per_transaction
     , COALESCE(fmb1.activating_gross_margin_$, 0) -
       COALESCE(fmb2.activating_gross_margin_$, 0)                             AS activating_gross_margin_$
     , COALESCE(fmb1.repeat_gaap_gross_revenue, 0) -
       COALESCE(fmb2.repeat_gaap_gross_revenue, 0)                             AS repeat_gaap_gross_revenue
     , COALESCE(fmb1.repeat_gaap_net_revenue, 0) -
       COALESCE(fmb2.repeat_gaap_net_revenue, 0)                               AS repeat_gaap_net_revenue
     , COALESCE(fmb1.repeat_order_count, 0) -
       COALESCE(fmb2.repeat_order_count, 0)                                    AS repeat_order_count
     , COALESCE(fmb1.repeat_aov_incl_shipping_rev, 0) -
       COALESCE(fmb2.repeat_aov_incl_shipping_rev, 0)                          AS repeat_aov_incl_shipping_rev
     , COALESCE(fmb1.repeat_units_per_transaction, 0) -
       COALESCE(fmb2.repeat_units_per_transaction, 0)                          AS repeat_units_per_transaction
     , COALESCE(fmb1.repeat_gaap_gross_margin_$, 0) -
       COALESCE(fmb2.repeat_gaap_gross_margin_$, 0)                            AS repeat_gaap_gross_margin_$
     , COALESCE(fmb1.repeat_net_unredeemed_credit_billings, 0) -
       COALESCE(fmb2.repeat_net_unredeemed_credit_billings, 0)                 AS repeat_net_unredeemed_credit_billings
     , COALESCE(fmb1.repeat_refund_credits, 0) -
       COALESCE(fmb2.repeat_refund_credits, 0)                                 AS repeat_refund_credits
     , COALESCE(fmb1.repeat_other_ad_rev_div_adaptive_adj, 0) -
       COALESCE(fmb2.repeat_other_ad_rev_div_adaptive_adj, 0)                  AS repeat_other_ad_rev_div_adaptive_adj
     , COALESCE(fmb1.membership_credits_charged, 0) -
       COALESCE(fmb2.membership_credits_charged, 0)                            AS membership_credits_charged
     , COALESCE(fmb1.membership_credits_redeemed, 0) -
       COALESCE(fmb2.membership_credits_redeemed, 0)                           AS membership_credits_redeemed
     , COALESCE(fmb1.membership_credits_refunded_plus_chargebacks, 0) -
       COALESCE(fmb2.membership_credits_refunded_plus_chargebacks, 0)          AS membership_credits_refunded_plus_chargebacks
     , COALESCE(fmb1.refund_credit_redeemed, 0) -
       COALESCE(fmb2.refund_credit_redeemed, 0)                                AS refund_credit_redeemed
     , COALESCE(fmb1.bom_vips, 0) - COALESCE(fmb2.bom_vips, 0)                 AS bom_vips
     , COALESCE(fmb1.total_units_shipped, 0) -
       COALESCE(fmb2.total_units_shipped, 0)                                   AS total_units_shipped
     , COALESCE(fmb1.repeat_cash_purchasers, 0) -
       COALESCE(fmb2.repeat_cash_purchasers, 0)                                AS repeat_cash_purchasers
     , COALESCE(fmb1.ebitda_minus_cash, 0) -
       COALESCE(fmb2.ebitda_minus_cash, 0)                                     AS ebitda_minus_cash
     , COALESCE(fmb1.repeatplusbizdev, 0) - COALESCE(fmb2.repeatplusbizdev, 0) AS repeatplusbizdev
     , COALESCE(fmb1.activating_units, 0) - COALESCE(fmb2.activating_units, 0) AS activating_units
     , COALESCE(fmb1.repeat_units, 0) - COALESCE(fmb2.repeat_units, 0)         AS repeat_units
     , COALESCE(fmb1.repeat_product_revenue_incl_shipping, 0) -
       COALESCE(fmb2.repeat_product_revenue_incl_shipping, 0)                  AS repeat_product_revenue_incl_shipping
     , COALESCE(fmb1.product_cost_calc, 0) -
       COALESCE(fmb2.product_cost_calc, 0)                                     AS product_cost_calc
     , COALESCE(fmb1.inventory_writedown, 0) -
       COALESCE(fmb2.inventory_writedown, 0)                                   AS inventory_writedown
     , COALESCE(fmb1.total_cogs_minus_cash, 0) -
       COALESCE(fmb2.total_cogs_minus_cash, 0)                                 AS total_cogs_minus_cash
     , COALESCE(fmb1.merchant_fees, 0) - COALESCE(fmb2.merchant_fees, 0)       AS merchant_fees
     , COALESCE(fmb1.gms_variable, 0) - COALESCE(fmb2.gms_variable, 0)         AS gms_variable
     , COALESCE(fmb1.fc_variable, 0) - COALESCE(fmb2.fc_variable, 0)           AS fc_variable
     , COALESCE(fmb1.freight_revenue, 0) - COALESCE(fmb2.freight_revenue, 0)   AS freight_revenue
     , COALESCE(fmb1.freight_out_cost, 0) - COALESCE(fmb2.freight_out_cost, 0) AS freight_out_cost
     , COALESCE(fmb1.outbound_shipping_supplies, 0) -
       COALESCE(fmb2.outbound_shipping_supplies, 0)                            AS outbound_shipping_supplies
     , COALESCE(fmb1.returns_shipping_cost, 0) -
       COALESCE(fmb2.returns_shipping_cost, 0)                                 AS returns_shipping_cost
     , COALESCE(fmb1.product_cost_markdown, 0) -
       COALESCE(fmb2.product_cost_markdown, 0)                                 AS product_cost_markdown
     , COALESCE(fmb1.acquisition_margin_$, 0) -
       COALESCE(fmb2.acquisition_margin_$, 0)                                  AS acquisition_margin_$
     , COALESCE(fmb1.product_cost_cust_returns_calc, 0) -
       COALESCE(fmb2.product_cost_cust_returns_calc, 0)                        AS product_cost_cust_returns_calc
     , COALESCE(fmb1.reship_exch_orders_shipped, 0) -
       COALESCE(fmb2.reship_exch_orders_shipped, 0)                            AS reship_exch_orders_shipped
     , COALESCE(fmb1.reship_exch_units_shipped, 0) -
       COALESCE(fmb2.reship_exch_units_shipped, 0)                             AS reship_exch_units_shipped
     , COALESCE(fmb1.cash_net_revenue, 0) - COALESCE(fmb2.cash_net_revenue, 0) AS cash_net_revenue
     , COALESCE(fmb1.activating_product_gross_revenue, 0) -
       COALESCE(fmb2.activating_product_gross_revenue, 0)                      AS activating_product_gross_revenue
     , COALESCE(fmb1.repeat_product_gross_revenue, 0) -
       COALESCE(fmb2.repeat_product_gross_revenue, 0)                          AS repeat_product_gross_revenue
     , COALESCE(fmb1.repeat_net_cash_revenue, 0) -
       COALESCE(fmb2.repeat_net_cash_revenue, 0)                                 AS repeat_net_cash_revenue
     , COALESCE(fmb1.repeat_cash_gross_margin, 0) -
       COALESCE(fmb2.repeat_cash_gross_margin, 0)                                AS repeat_cash_gross_margin
     , COALESCE(fmb1.repeat_shipped_order_cash_collected, 0) -
       COALESCE(fmb2.repeat_shipped_order_cash_collected, 0)                     AS repeat_shipped_order_cash_collected
     , COALESCE(fmb1.repeat_units_shipped, 0) -
       COALESCE(fmb2.repeat_units_shipped, 0)                                  AS repeat_units_shipped
     , COALESCE(fmb1.repeat_orders_shipped, 0) -
       COALESCE(fmb2.repeat_orders_shipped, 0)                                 AS repeat_orders_shipped
     , COALESCE(fmb1.net_unredeemed_credit_billings, 0) -
       COALESCE(fmb2.net_unredeemed_credit_billings, 0)                          AS net_unredeemed_credit_billings
     , COALESCE(fmb1.net_unredeemed_refund_credit, 0) -
       COALESCE(fmb2.net_unredeemed_refund_credit, 0)                            AS net_unredeemed_refund_credit
     , COALESCE(fmb1.advertising_expenses, 0) -
       COALESCE(fmb2.advertising_expenses, 0)                                  AS advertising_expenses
     , COALESCE(fmb1.merchant_fee, 0) - COALESCE(fmb2.merchant_fee, 0)         AS merchant_fee
     , COALESCE(fmb1.selling_expenses, 0) - COALESCE(fmb2.selling_expenses, 0) AS selling_expenses
     , COALESCE(fmb1.marketing_expenses, 0) -
       COALESCE(fmb2.marketing_expenses, 0)                                    AS marketing_expenses
     , COALESCE(fmb1.direct_general_and_administrative, 0) -
       COALESCE(fmb2.direct_general_and_administrative, 0)                     AS direct_general_and_administrative
     , COALESCE(fmb1.refunded_as_credit, 0) -
       COALESCE(fmb2.refunded_as_credit, 0)                                    AS refunded_as_credit
     , COALESCE(fmb1.deferred_revenue, 0) - COALESCE(fmb2.deferred_revenue, 0)     AS deferred_revenue
     , COALESCE(fmb1.product_revenue - fmb2.product_revenue, 0)                AS product_revenue
     , COALESCE(fmb1.total_opex - fmb2.total_opex, 0)                              AS total_opex
     , COALESCE(fmb1.product_gross_margin - fmb2.product_gross_margin, 0)      AS product_gross_margin
     , COALESCE(fmb1.cash_gross_profit - fmb2.cash_gross_profit, 0)            AS cash_gross_profit
     , COALESCE(fmb1.retail_redemption_revenue - fmb2.retail_redemption_revenue,
                0)                                                             AS retail_redemption_revenue
     , COALESCE(fmb1.total_cogs, 0) - COALESCE(fmb2.total_cogs, 0)     AS total_cogs
     , COALESCE(fmb1.paid_vips, 0) - COALESCE(fmb2.paid_vips, 0)     AS paid_vips
     , COALESCE(fmb1.cross_promo_ft_vips, 0) - COALESCE(fmb2.cross_promo_ft_vips, 0)     AS cross_promo_ft_vips
     , COALESCE(fmb1.global_finance_organization, 0) - COALESCE(fmb2.global_finance_organization, 0)     AS global_finance_organization
     , COALESCE(fmb1.global_ops_office_admin_and_facilities, 0) - COALESCE(fmb2.global_ops_office_admin_and_facilities, 0)     AS  global_ops_office_admin_and_facilities
     , COALESCE(fmb1.tfg_administration, 0) - COALESCE(fmb2.tfg_administration, 0)     AS tfg_administration
     , COALESCE(fmb1.global_people_team, 0) - COALESCE(fmb2.global_people_team, 0)     AS global_people_team
     , COALESCE(fmb1.legal, 0) - COALESCE(fmb2.legal, 0)     AS legal
     , COALESCE(fmb1.executive, 0) - COALESCE(fmb2.executive, 0)     AS executive
     , COALESCE(fmb1.non_allocable_corporate, 0) - COALESCE(fmb2.non_allocable_corporate, 0)     AS    non_allocable_corporate
     , COALESCE(fmb1.eu_media_buying, 0) - COALESCE(fmb2.eu_media_buying, 0)     AS  eu_media_buying
     , COALESCE(fmb1.eu_creative_and_marketing, 0) - COALESCE(fmb2.eu_creative_and_marketing, 0)     AS   eu_creative_and_marketing
     , COALESCE(fmb1.total_backoffice_step1, 0) - COALESCE(fmb2.total_backoffice_step1, 0)     AS  total_backoffice_step1
     , COALESCE(fmb1.net_revenue_cash, 0) - COALESCE(fmb2.net_revenue_cash, 0)     AS  net_revenue_cash
     , COALESCE(fmb1.selling_expense, 0) - COALESCE(fmb2.selling_expense, 0)     AS   selling_expense
     , COALESCE(fmb1.global_member_services_fixed, 0) - COALESCE(fmb2.global_member_services_fixed, 0)     AS   global_member_services_fixed
      , COALESCE(fmb1.fulfillment_centers_fixed, 0) - COALESCE(fmb2.fulfillment_centers_fixed, 0)     AS  fulfillment_centers_fixed
FROM (SELECT fmb.*, sg.*
      FROM _finance_monthly_budget_forecast_actual__ranked fmb
               JOIN _store_groupings_sub sg
                    ON fmb.hyperion_store_rg = sg.store_abbrev
      WHERE sg.store_order = 1) fmb1
         JOIN (SELECT fmb.*, sg.*
               FROM _finance_monthly_budget_forecast_actual__ranked fmb
                        JOIN _store_groupings_sub sg
                             ON fmb.hyperion_store_rg = sg.store_abbrev
               WHERE sg.store_order = 2) fmb2
              ON fmb1.version = fmb2.version AND fmb1.version_date = fmb2.version_date AND
                 fmb1.financial_date = fmb2.financial_date
                  AND fmb1.store_grouping = fmb2.store_grouping
;

CREATE OR REPLACE TEMP TABLE _finance_monthly_budget_forecast_actual__subtracted_composites AS
SELECT *
     , net_change_in_vips / NULLIFZERO(bom_vips)                                      AS vip_growth_percent
     , repeat_cash_gross_margin / NULLIFZERO(repeat_net_cash_revenue)                 AS repeat_cash_gross_margin_percent
     , net_unredeemed_credit_billings / NULLIFZERO(net_cash_revenue_total)            AS net_unredeemed_credit_billings_as_percent_of_net_cash_revenue
     , repeat_order_count / NULLIFZERO(bom_vips)                                      AS o_div_vip
     , acquisition_margin_$ / NULLIFZERO(activating_product_gross_revenue)            AS acquisition_margin_percent
     , acquisition_margin_$ / NULLIFZERO(activating_order_count)                      AS acquisition_margin_$__div__order
     , (ABS(cash_refunds) + ABS(chargebacks)) /
       NULLIFZERO(gross_cash_revenue)                                                 AS refunds_and_chargebacks_as_percent_of_gross_cash --needs work
     , cash_gross_margin / NULLIFZERO(net_cash_revenue_total)                         AS cash_gross_margin_percent_total                  --needs work
     , (media_spend) / NULLIFZERO(leads)                                              AS cpl                                              --needs work
     , (media_spend) / NULLIFZERO(total_new_vips)                                     AS vip_cpa                                          --needs work
     , (ABS(first_discounts)) / NULLIFZERO((ABS(first_discounts) +
                                            (ABS(activating_gaap_gross_revenue))))    AS activating_discount_percent                      --needs work
     , (activating_gross_margin_$) /
       NULLIFZERO(activating_gaap_net_revenue)                                        AS activating_gross_margin_percent                  --needs work
     , ABS((repeat_discounts)) / NULLIFZERO(ABS(repeat_discounts) +
                                            (repeat_product_revenue_incl_shipping))   AS repeat_discount_percent                          -- needs work, SHOULD WE USE        repeat_gaap_gross_revenue, repeat_product_revenue_incl_shipping,
     , (repeat_gaap_gross_margin_$) /
        NULLIFZERO(repeat_gaap_net_revenue)                                           AS repeat_gaap_gross_margin_percent                 --needs work
     , (membership_credits_charged_count) / NULLIFZERO(bom_vips)                      AS credit_billed_percent                            --needs work
     , (membership_credits_redeemed_count) /
       NULLIFZERO(membership_credits_charged_count)                                   AS credit_redeemed_percent                          --needs work
     , (membership_credits_cancelled_count) /
       NULLIFZERO(membership_credits_charged_count)                                   AS credit_canceled_percent                          --needs work
     , (repeat_cash_purchasers) / NULLIFZERO(bom_vips)                                AS cash_purchase_rate                               --needs work
     , cash_gross_margin / NULLIFZERO(cash_net_revenue)                               AS cash_gross_margin_percent                        --needs work
FROM _finance_monthly_budget_forecast_actual__subtracted;

INSERT INTO _finance_monthly_budget_forecast_actual__ranked (
        version,
        version_date,
        financial_date,
        hyperion_store_rg,
        bu,
        membership_credits_charged_count,
        membership_credits_redeemed_count,
        membership_credits_cancelled_count,
        first_msrp_revenue,
        first_discounts,
        repeat_msrp_revenue,
        repeat_discounts,
        leads,
        gross_cash_revenue,
        cash_refunds,
        chargebacks,
        net_cash_revenue_total,
        refunds_and_chargebacks_as_percent_of_gross_cash,
        cash_gross_margin,
        cash_gross_margin_percent_total,
        media_spend,
        cash_contribution_after_media,
        cpl,
        vip_cpa,
        m1_lead_to_vip,
        m1_vips,
        aged_lead_conversions,
        total_new_vips,
        cancels,
        net_change_in_vips,
        ending_vips,
        vip_growth_percent,
        activating_gaap_gross_revenue,
        activating_gaap_net_revenue,
        activating_order_count,
        activating_aov_incl_shipping_rev,
        activating_units_per_transaction,
        activating_discount_percent,
        activating_gross_margin_$,
        activating_gross_margin_percent,
        repeat_gaap_gross_revenue,
        repeat_gaap_net_revenue,
        repeat_order_count,
        repeat_aov_incl_shipping_rev,
        repeat_units_per_transaction,
        repeat_discount_percent,
        repeat_gaap_gross_margin_$,
        repeat_gaap_gross_margin_percent,
        repeat_net_unredeemed_credit_billings,
        repeat_refund_credits,
        repeat_other_ad_rev_div_adaptive_adj,
        repeat_net_cash_revenue,
        repeat_cash_gross_margin,
        repeat_cash_gross_margin_percent,
        membership_credits_charged,
        membership_credits_redeemed,
        membership_credits_refunded_plus_chargebacks,
        net_unredeemed_credit_billings,
        net_unredeemed_credit_billings_as_percent_of_net_cash_revenue,
        refunded_as_credit,
        refund_credit_redeemed,
        net_unredeemed_refund_credit,
        deferred_revenue,
        bom_vips,
        credit_billed_percent,
        credit_redeemed_percent,
        credit_canceled_percent,
        total_units_shipped,
        repeat_cash_purchasers,
        cash_purchase_rate,
        o_div_vip,
        ebitda_minus_cash,
        repeatplusbizdev,
        activating_units,
        repeat_units,
        repeat_product_revenue_incl_shipping,
        product_cost_calc,
        inventory_writedown,
        total_cogs_minus_cash,
        merchant_fees,
        gms_variable,
        fc_variable,
        freight_revenue,
        freight_out_cost,
        outbound_shipping_supplies,
        returns_shipping_cost,
        product_cost_markdown,
        repeat_shipped_order_cash_collected,
        acquisition_margin_$,
        acquisition_margin_percent,
        acquisition_margin_$__div__order,
        product_cost_cust_returns_calc,
        reship_exch_orders_shipped,
        reship_exch_units_shipped,
        cash_net_revenue,
        activating_product_gross_revenue,
        repeat_product_gross_revenue,
        cash_gross_margin_percent,
        repeat_units_shipped,
        repeat_orders_shipped,
        advertising_expenses,
        merchant_fee,
        selling_expenses,
        marketing_expenses,
        direct_general_and_administrative,
        total_opex,
        product_revenue,
        product_gross_margin,
        cash_gross_profit,
        retail_redemption_revenue,
        total_cogs,
        paid_vips,
        cross_promo_ft_vips,
        global_finance_organization,
        global_ops_office_admin_and_facilities,
        tfg_administration,
        global_people_team,
        legal,
        executive,
        non_allocable_corporate,
        eu_media_buying,
        eu_creative_and_marketing,
        total_backoffice_step1,
        net_revenue_cash,
        selling_expense,
        global_member_services_fixed,
        fulfillment_centers_fixed
)
SELECT version,
       version_date,
       financial_date,
       hyperion_store_rg,
       bu,
       membership_credits_charged_count,
       membership_credits_redeemed_count,
       membership_credits_cancelled_count,
       first_msrp_revenue,
       first_discounts,
       repeat_msrp_revenue,
       repeat_discounts,
       leads,
       gross_cash_revenue,
       cash_refunds,
       chargebacks,
       net_cash_revenue_total,
       refunds_and_chargebacks_as_percent_of_gross_cash,
       cash_gross_margin,
       cash_gross_margin_percent_total,
       media_spend,
       cash_contribution_after_media,
       cpl,
       vip_cpa,
       m1_lead_to_vip,
       m1_vips,
       aged_lead_conversions,
       total_new_vips,
       cancels,
       net_change_in_vips,
       ending_vips,
       vip_growth_percent,
       activating_gaap_gross_revenue,
       activating_gaap_net_revenue,
       activating_order_count,
       activating_aov_incl_shipping_rev,
       activating_units_per_transaction,
       activating_discount_percent,
       activating_gross_margin_$,
       activating_gross_margin_percent,
       repeat_gaap_gross_revenue,
       repeat_gaap_net_revenue,
       repeat_order_count,
       repeat_aov_incl_shipping_rev,
       repeat_units_per_transaction,
       repeat_discount_percent,
       repeat_gaap_gross_margin_$,
       repeat_gaap_gross_margin_percent,
       repeat_net_unredeemed_credit_billings,
       repeat_refund_credits,
       repeat_other_ad_rev_div_adaptive_adj,
       repeat_net_cash_revenue,
       repeat_cash_gross_margin,
       repeat_cash_gross_margin_percent,
       membership_credits_charged,
       membership_credits_redeemed,
       membership_credits_refunded_plus_chargebacks,
       net_unredeemed_credit_billings,
       net_unredeemed_credit_billings_as_percent_of_net_cash_revenue,
       refunded_as_credit,
       refund_credit_redeemed,
       net_unredeemed_refund_credit,
       deferred_revenue,
       bom_vips,
       credit_billed_percent,
       credit_redeemed_percent,
       credit_canceled_percent,
       total_units_shipped,
       repeat_cash_purchasers,
       cash_purchase_rate,
       o_div_vip,
       ebitda_minus_cash,
       repeatplusbizdev,
       activating_units,
       repeat_units,
       repeat_product_revenue_incl_shipping,
       product_cost_calc,
       inventory_writedown,
       total_cogs_minus_cash,
       merchant_fees,
       gms_variable,
       fc_variable,
       freight_revenue,
       freight_out_cost,
       outbound_shipping_supplies,
       returns_shipping_cost,
       product_cost_markdown,
       repeat_shipped_order_cash_collected,
       acquisition_margin_$,
       acquisition_margin_percent,
       acquisition_margin_$__div__order,
       product_cost_cust_returns_calc,
       reship_exch_orders_shipped,
       reship_exch_units_shipped,
       cash_net_revenue,
       activating_product_gross_revenue,
       repeat_product_gross_revenue,
       cash_gross_margin_percent,
       repeat_units_shipped,
       repeat_orders_shipped,
       advertising_expenses,
       merchant_fee,
       selling_expenses,
       marketing_expenses,
       direct_general_and_administrative,
       total_opex,
       product_revenue,
       product_gross_margin,
       cash_gross_profit,
       retail_redemption_revenue,
       total_cogs,
       paid_vips,
       cross_promo_ft_vips,
       global_finance_organization,
       global_ops_office_admin_and_facilities,
       tfg_administration,
       global_people_team,
       legal,
       executive,
       non_allocable_corporate,
       eu_media_buying,
       eu_creative_and_marketing,
       total_backoffice_step1,
       net_revenue_cash,
       selling_expense,
       global_member_services_fixed,
       fulfillment_centers_fixed
FROM _finance_monthly_budget_forecast_actual__subtracted_composites;


CREATE OR REPLACE TABLE reference.finance_monthly_budget_forecast_actual AS
SELECT *,
       $execution_start_time AS meta_create_datetime,
       $execution_start_time AS meta_update_datetime
FROM _finance_monthly_budget_forecast_actual__ranked
ORDER BY bu, version, version_date, financial_date;
