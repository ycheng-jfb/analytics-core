CREATE OR REPLACE VIEW reporting.weekly_kpi_ssrs_output
            (
             date,
             report_mapping,
             source,
             segment,
             cash_gross_revenue,
             cash_refund_amount,
             chargeback_amount,
             cash_net_revenue,
             refunds_and_chargebacks_as_percent_of_gross_cash,
             cash_gross_profit,
             cash_gross_profit_percent_net,
             cash_contribution_profit,
             media_spend,
             cash_contribution_profit_after_media,
             cpl,
             vip_cpa,
             m1_lead_to_vip,
             m1_vips,
             aged_lead_conversions,
             new_vips,
             cancels,
             net_change_in_vips,
             eop_vips,
             growth_percent,
             xcpa,
             activating_product_gross_revenue,
             activating_product_net_revenue,
             activating_product_order_count,
             activating_aov,
             activating_upt,
             activating_discount_rate_percent,
             activating_product_gross_profit,
             activating_product_gross_profit_percent_net,
             net_unredeemed_billed_credits,
             nonactivating_product_gross_revenue,
             nonactivating_product_net_revenue,
             nonactivating_product_order_count,
             nonactivating_aov,
             nonactivating_upt,
             nonactivating_discount_rate_percent,
             nonactivating_product_gross_profit,
             nonactivating_product_gross_profit_percent_net,
             nonactivating_cash_net_revenue,
             nonactivating_cash_gross_profit,
             nonactivating_cash_gross_profit_percent_net,
             guest_product_gross_revenue,
             guest_product_net_revenue,
             guest_product_order_count,
             guest_aov,
             guest_upt,
             guest_discount_percent,
             guest_product_gross_profit,
             guest_product_gross_profit_percent,
             repeat_vip_product_gross_revenue,
             repeat_vip_product_net_revenue,
             repeat_vip_product_order_count,
             repeat_vip_aov,
             repeat_vip_upt,
             repeat_vip_discount_percent,
             repeat_vip_product_gross_profit,
             repeat_vip_product_gross_profit_percent,
             billed_credits_transacted,
             billed_credits_redeemed,
             billed_credit_cash_refund_chargeback_amount,
             net_unredeemed_billed_credits_as_percent_of_net_cash_revenue,
             refund_cash_credit_issued_amount,
             refund_credit_redeemed,
             net_unredeemed_refund_credit,
             billed_credit_deferred_revenue,
             bop_vips,
             billed_credit_percent_of_bop_vips,
             billed_credit_redeemed_percent,
             billed_credit_cancelled_percent,
             skip_rate_percent,
             landed_product_cost,
             total_product_cost_net_of_returns,
             outbound_shipping_costs,
             shipping_supplies_costs,
             return_shipping_costs,
             misc_cogs,
             total_cogs,
             unit_count_incl_reship_exch,
             auc,
             product_cost_per_order,
             shipping_cost_incl_shipping_supplies_per_order,
             snapshot_datetime_data,
             snapshot_datetime_budget,
             snapshot_datetime_forecast
                )
AS
WITH _report_date AS (SELECT COALESCE(a.report_date,
                                      (SELECT MAX(d.date)
                                       FROM reporting.daily_cash_final_output d
                                       WHERE report_date_type = 'To Date')) AS report_date,
                             COALESCE((DAY(LAST_DAY(a.report_date)) / NULLIFZERO(DAY(a.report_date))),
                                      (SELECT (DAY(LAST_DAY(MAX(e.date))) / NULLIFZERO(DAY(MAX(e.date))))
                                       FROM reporting.daily_cash_final_output e
                                       WHERE report_date_type = 'To Date')) AS acq_run_rate_multiplier
                      FROM reference.config_ssrs_date_param a
                      WHERE a.report = 'Weekly KPI'),
     _daily_cash_final_output AS (SELECT *
                                  FROM reporting.daily_cash_final_output
                                  WHERE report_mapping IN
                                        (   'FK-TREV-NA',
                                            'FL-W-OREV-EU',
                                            'FL-M-OREV-EU',
                                            'FL-RREV-EU',
                                            'FL-R-OREV-EU',
                                            'FL-TREV-EU',
                                            'FL+SC-W-OREV-NA',
                                            'FL+SC-M-OREV-NA',
                                            'FL+SC-OREV-NA',
                                            'FL+SC-RREV-NA',
                                            'FL+SC-R-OREV-NA',
                                            'FL+SC-TREV-NA',
                                            'JF-TREV-EU',
                                            'JF-TREV-NA',
                                            'SD-TREV-NA',
                                            'SX-TREV-EU',
                                            'SX-OREV-NA',
                                            'SX-TREV-NA',
                                            'SX-RREV-US',
                                            'SX-R-OREV-US',
                                            'SX-TREV-Global',
                                            'SX-TREV-CA-US',
                                            'YT-OREV-NA'
                                            )
                                    AND date_object = 'placed'
                                    AND currency_type = 'USD'
                                    AND report_date_type = 'To Date'
                                    AND date <= (SELECT report_date FROM _report_date)),
     _daily_cash_snapshot_datetime AS (SELECT MAX(meta_update_datetime)       AS snapshot_datetime_data,
                                              MAX(snapshot_datetime_budget)   AS snapshot_datetime_budget,
                                              MAX(snapshot_datetime_forecast) AS snapshot_datetime_forecast
                                       FROM _daily_cash_final_output),
     _variables AS (SELECT (SELECT report_date FROM _report_date)             AS report_date,
                           'Report Date'                                      AS segment,
                           (SELECT report_date FROM _report_date)             AS full_date,
                           (SELECT acq_run_rate_multiplier FROM _report_date) AS acq_run_rate_multiplier
                    UNION ALL
                    SELECT DATE_TRUNC('MONTH', (SELECT report_date FROM _report_date)) AS report_date,
                           'Report Month'                                              AS segment,
                           DATE_TRUNC('MONTH', (SELECT report_date FROM _report_date)) AS full_date,
                           (SELECT acq_run_rate_multiplier FROM _report_date)          AS acq_run_rate_multiplier
                    UNION ALL
                    SELECT (SELECT report_date FROM _report_date)             AS report_date,
                           'MTD'                                              AS segment,
                           (SELECT report_date FROM _report_date)             AS full_date,
                           (SELECT acq_run_rate_multiplier FROM _report_date) AS acq_run_rate_multiplier
                    UNION ALL
                    SELECT LAST_DAY(DATEADD('MONTH', -1, (SELECT report_date FROM _report_date)))   AS report_date,
                           'LM'                                                                     AS segment,
                           DATE_TRUNC('MONTH',
                                      DATEADD('MONTH', -1, (SELECT report_date FROM _report_date))) AS full_date,
                           (SELECT acq_run_rate_multiplier FROM _report_date)                       AS acq_run_rate_multiplier
                    UNION ALL
                    SELECT LAST_DAY(DATEADD('YEAR', -1, (SELECT report_date FROM _report_date)))   AS report_date,
                           'LY'                                                                    AS segment,
                           DATE_TRUNC('MONTH',
                                      DATEADD('YEAR', -1, (SELECT report_date FROM _report_date))) AS full_date,
                           (SELECT acq_run_rate_multiplier FROM _report_date)                      AS acq_run_rate_multiplier),
     _report_months AS (SELECT DISTINCT report_mapping,
                                        LAST_DAY(date) AS last_day_of_month
                        FROM _daily_cash_final_output
                        WHERE date < (SELECT report_date
                                      FROM _variables
                                      WHERE segment = 'Report Month')),
     _actuals AS (SELECT v.report_date                                                                             AS date,
                         report_mapping,
                         'Actuals'                                                                                 AS source,
                         segment,
                         cash_gross_revenue_mtd                                                                    AS cash_gross_revenue,
                         COALESCE(product_order_cash_refund_amount_mtd, 0) +
                         COALESCE(billing_cash_refund_amount_mtd, 0)                                               AS cash_refund_amount,
                         COALESCE(product_order_cash_chargeback_amount_mtd, 0) +
                         COALESCE(billing_cash_chargeback_amount_mtd, 0)                                           AS chargeback_amount,
                         cash_net_revenue_mtd                                                                      AS cash_net_revenue,
                         (COALESCE(product_order_cash_refund_amount_mtd, 0) +
                          COALESCE(billing_cash_refund_amount_mtd, 0) +
                          COALESCE(product_order_cash_chargeback_amount_mtd, 0) +
                          COALESCE(billing_cash_chargeback_amount_mtd, 0)) /
                         NULLIFZERO(cash_gross_revenue_mtd)                                                        AS refunds_and_chargebacks_as_percent_of_gross_cash,
                         cash_gross_profit_mtd                                                                     AS cash_gross_profit,
                         COALESCE(cash_gross_profit_mtd, 0) / NULLIFZERO(cash_net_revenue_mtd)                     AS cash_gross_profit_percent_net,
                         cash_variable_contribution_profit_mtd                                                     AS cash_contribution_profit,
                         media_spend_mtd                                                                           AS media_spend,
                         COALESCE(cash_variable_contribution_profit_mtd, 0) -
                         COALESCE(media_spend_mtd, 0)                                                              AS cash_contribution_profit_after_media,
                         media_spend_mtd / NULLIFZERO(primary_leads_mtd)                                           AS cpl,
                         new_vips_mtd                                                                              AS new_vips,
                         media_spend_mtd / NULLIFZERO(new_vips_mtd)                                                AS vip_cpa,
                         new_vips_m1_mtd                                                                           AS m1_vips,
                         m1_vips / NULLIFZERO(leads_mtd)                                                           AS m1_lead_to_vip,
                         new_vips_mtd - m1_vips                                                                    AS aged_lead_conversions,
                         cancels_mtd                                                                               AS cancels,
                         COALESCE(new_vips_mtd, 0) - COALESCE(cancels_mtd, 0)                                      AS net_change_in_vips,
                         COALESCE(bop_vips_mtd, 0) + net_change_in_vips                                            AS eop_vips,
                         net_change_in_vips / NULLIFZERO(bop_vips_mtd)                                             AS growth_percent,
                         (COALESCE(media_spend_mtd, 0) - COALESCE(first_guest_product_margin_pre_return_mtd, 0)) /
                         NULLIFZERO(new_vips_mtd)                                                                  AS xcpa,
                         activating_product_gross_revenue_mtd                                                      AS activating_product_gross_revenue,
                         activating_product_net_revenue_mtd                                                        AS activating_product_net_revenue,
                         activating_product_order_count_mtd                                                        AS activating_product_order_count,
                         activating_product_gross_revenue_mtd /
                         NULLIFZERO(activating_product_order_count_mtd)                                            AS activating_aov,
                         activating_unit_count_mtd /
                         NULLIFZERO(activating_product_order_count_mtd)                                            AS activating_upt,
                         activating_product_order_product_discount_amount_mtd /
                         NULLIFZERO((COALESCE(activating_product_order_product_subtotal_amount_mtd, 0) -
                                     COALESCE(activating_product_order_noncash_credit_redeemed_amount_mtd,
                                              0)))                                                                 AS activating_discount_rate_percent,
                         activating_product_gross_profit_mtd                                                       AS activating_product_gross_profit,
                         activating_product_gross_profit_mtd /
                         NULLIFZERO(activating_product_net_revenue_mtd)                                            AS activating_product_gross_profit_percent_net,
                         nonactivating_product_gross_revenue_mtd                                                   AS nonactivating_product_gross_revenue,
                         nonactivating_product_net_revenue_mtd                                                     AS nonactivating_product_net_revenue,
                         nonactivating_product_order_count_mtd                                                     AS nonactivating_product_order_count,
                         nonactivating_product_gross_revenue_mtd /
                         NULLIFZERO(nonactivating_product_order_count_mtd)                                         AS nonactivating_aov,
                         nonactivating_unit_count_mtd /
                         NULLIFZERO(nonactivating_product_order_count_mtd)                                         AS nonactivating_upt,
                         nonactivating_product_order_product_discount_amount_mtd /
                         NULLIFZERO((COALESCE(nonactivating_product_order_product_subtotal_amount_mtd, 0) -
                                     COALESCE(nonactivating_product_order_noncash_credit_redeemed_amount_mtd,
                                              0)))                                                                 AS nonactivating_discount_rate_percent,
                         nonactivating_product_gross_profit_mtd                                                    AS nonactivating_product_gross_profit,
                         nonactivating_product_gross_profit_mtd /
                         NULLIFZERO(nonactivating_product_net_revenue_mtd)                                         AS nonactivating_product_gross_profit_percent_net,
                         guest_product_gross_revenue_mtd                                                           AS guest_product_gross_revenue,
                         guest_product_net_revenue_mtd                                                             AS guest_product_net_revenue,
                         guest_product_order_count_mtd                                                             AS guest_product_order_count,
                         guest_product_gross_profit_mtd                                                            AS guest_product_gross_profit,
                         guest_product_gross_revenue_mtd /
                         NULLIFZERO(guest_product_order_count_mtd)                                                 AS guest_aov,
                         guest_unit_count_mtd / NULLIFZERO(guest_product_order_count_mtd)                          AS guest_upt,
                         guest_product_order_product_discount_amount_mtd /
                         NULLIFZERO(COALESCE(guest_product_gross_revenue_mtd, 0) +
                                    COALESCE(guest_product_order_product_discount_amount_mtd, 0))                  AS guest_discount_percent,
                         guest_product_gross_profit_mtd /
                         NULLIFZERO(guest_product_net_revenue_mtd)                                                 AS guest_product_gross_profit_percent,
                         repeat_vip_product_gross_revenue_mtd                                                      AS repeat_vip_product_gross_revenue,
                         repeat_vip_product_net_revenue_mtd                                                        AS repeat_vip_product_net_revenue,
                         repeat_vip_product_order_count_mtd                                                        AS repeat_vip_product_order_count,
                         repeat_vip_product_gross_profit_mtd                                                       AS repeat_vip_product_gross_profit,
                         repeat_vip_product_gross_revenue_mtd /
                         NULLIFZERO(repeat_vip_product_order_count_mtd)                                            AS repeat_vip_aov,
                         repeat_vip_unit_count_mtd /
                         NULLIFZERO(repeat_vip_product_order_count_mtd)                                            AS repeat_vip_upt,
                         repeat_vip_product_order_product_discount_amount_mtd /
                         NULLIFZERO(COALESCE(repeat_vip_product_gross_revenue_mtd, 0) +
                                    COALESCE(repeat_vip_product_order_product_discount_amount_mtd, 0))             AS repeat_vip_discount_percent,
                         repeat_vip_product_gross_profit_mtd /
                         NULLIFZERO(repeat_vip_product_net_revenue_mtd)                                            AS repeat_vip_product_gross_profit_percent,
                         COALESCE(billed_credit_cash_transaction_amount_mtd, 0) -
                         COALESCE(billed_credit_cash_refund_chargeback_amount_mtd, 0) -
                         COALESCE(billed_cash_credit_redeemed_amount_mtd, 0)                                       AS net_unredeemed_billed_credits,
                         nonactivating_cash_net_revenue_mtd                                                        AS nonactivating_cash_net_revenue,
                         nonactivating_cash_gross_profit_mtd                                                       AS nonactivating_cash_gross_profit,
                         nonactivating_cash_gross_profit_mtd /
                         NULLIFZERO(nonactivating_cash_net_revenue_mtd)                                            AS nonactivating_cash_gross_profit_percent_net,
                         billed_credit_cash_transaction_amount_mtd                                                 AS billed_credits_transacted,
                         billed_cash_credit_redeemed_amount_mtd                                                    AS billed_credits_redeemed,
                         billed_credit_cash_refund_chargeback_amount_mtd                                           AS billed_credit_cash_refund_chargeback_amount,
                         net_unredeemed_billed_credits / NULLIFZERO(cash_net_revenue_mtd)                          AS net_unredeemed_billed_credits_as_percent_of_net_cash_revenue,
                         COALESCE(product_order_cash_credit_refund_amount_mtd, 0) +
                         COALESCE(product_order_noncash_credit_refund_amount_mtd, 0)                               AS refunded_as_credit,
                         refund_cash_credit_issued_amount_mtd                                                      AS refund_cash_credit_issued_amount,
                         refund_cash_credit_redeemed_amount_mtd                                                    AS refund_credit_redeemed,
                         COALESCE(refund_cash_credit_issued_amount_mtd, 0) -
                         COALESCE(refund_cash_credit_cancelled_amount_mtd, 0) -
                         COALESCE(refund_cash_credit_redeemed_amount_mtd, 0)                                       AS net_unredeemed_refund_credit,
                         COALESCE(net_unredeemed_billed_credits, 0) +
                         COALESCE(net_unredeemed_refund_credit, 0)                                                 AS billed_credit_deferred_revenue,
                         bop_vips_mtd                                                                              AS bop_vips,
                         billing_order_transaction_count_mtd / NULLIFZERO(bop_vips_mtd)                            AS billed_credit_percent_of_bop_vips,
                         billed_cash_credit_redeemed_equivalent_count_mtd /
                         NULLIFZERO(billed_cash_credit_issued_equivalent_count_mtd)                                AS billed_credit_redeemed_percent,
                         billed_cash_credit_cancelled_equivalent_count_mtd /
                         NULLIFZERO(billed_cash_credit_issued_equivalent_count_mtd)                                AS billed_credit_cancelled_percent,
                         COALESCE(skip_count, 0) / NULLIFZERO(bom_vips)                                            AS skip_rate_percent,
                         COALESCE(product_order_landed_product_cost_amount_mtd, 0) +
                         COALESCE(product_order_exchange_product_cost_amount_mtd, 0) +
                         COALESCE(product_order_reship_product_cost_amount_mtd, 0)                                 AS landed_product_cost,
                         COALESCE(landed_product_cost, 0) -
                         COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd, 0)                        AS total_product_cost_net_of_returns,
                         COALESCE(product_order_shipping_cost_amount_mtd, 0) +
                         COALESCE(product_order_reship_shipping_cost_amount_mtd, 0) +
                         COALESCE(product_order_exchange_shipping_cost_amount_mtd, 0)                              AS outbound_shipping_costs,
                         COALESCE(product_order_shipping_supplies_cost_amount_mtd, 0) +
                         COALESCE(product_order_exchange_shipping_supplies_cost_amount_mtd, 0) +
                         COALESCE(product_order_reship_shipping_supplies_cost_amount_mtd, 0)                       AS shipping_supplies_costs,
                         return_shipping_costs_incl_reship_exch_mtd                                                AS return_shipping_costs,
                         misc_cogs_amount_mtd                                                                      AS misc_cogs,
                         (COALESCE(product_order_landed_product_cost_amount_mtd, 0)
                              + COALESCE(product_order_exchange_product_cost_amount_mtd, 0)
                              + COALESCE(product_order_reship_product_cost_amount_mtd, 0)
                              + COALESCE(product_order_shipping_cost_amount_mtd, 0)
                              + COALESCE(product_order_reship_shipping_cost_amount_mtd, 0)
                              + COALESCE(product_order_exchange_shipping_cost_amount_mtd, 0)
                              + COALESCE(return_shipping_costs_incl_reship_exch_mtd, 0)
                              + COALESCE(product_order_shipping_supplies_cost_amount_mtd, 0)
                              + COALESCE(product_order_exchange_shipping_supplies_cost_amount_mtd, 0)
                              + COALESCE(product_order_reship_shipping_supplies_cost_amount_mtd, 0)
                              + COALESCE(misc_cogs_amount_mtd, 0)
                             - COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd, 0)
                             )                                                                                     AS total_cogs,
                         COALESCE(unit_count_mtd, 0) + COALESCE(product_order_reship_unit_count_mtd, 0) +
                         COALESCE(product_order_exchange_unit_count_mtd, 0)                                        AS unit_count_incl_reship_exch,
                         (COALESCE(product_order_landed_product_cost_amount_mtd, 0) +
                          COALESCE(product_order_exchange_product_cost_amount_mtd, 0) +
                          COALESCE(product_order_reship_product_cost_amount_mtd, 0))
                             /
                         NULLIFZERO(COALESCE(unit_count_mtd, 0) + COALESCE(product_order_reship_unit_count_mtd, 0) +
                                    COALESCE(product_order_exchange_unit_count_mtd, 0))                            AS auc,
                         total_cogs / NULLIFZERO(COALESCE(product_order_count_mtd, 0) +
                                                 COALESCE(product_order_reship_order_count_mtd, 0) +
                                                 COALESCE(product_order_exchange_order_count_mtd, 0))              AS product_cost_per_order,
                         outbound_shipping_costs / NULLIFZERO(COALESCE(product_order_count_mtd, 0) +
                                                              COALESCE(product_order_reship_order_count_mtd, 0) +
                                                              COALESCE(product_order_exchange_order_count_mtd, 0)) AS shipping_cost_incl_shipping_supplies_per_order
                  FROM _daily_cash_final_output dcfo
                           JOIN (SELECT report_date, segment
                                 FROM _variables
                                 WHERE segment IN ('MTD', 'LM', 'LY')
                                 UNION
                                 SELECT last_day_of_month AS report_date, 'EOM' AS segment
                                 FROM _report_months) v ON v.report_date = dcfo.date
                  UNION ALL
                  SELECT date,
                         dcfo.report_mapping,
                         'Actuals'                                                                                                                      AS source,
                         'RR'                                                                                                                           AS segment,
                         COALESCE(activating_cash_gross_revenue_mtd * (activating_cash_gross_revenue_month_tot_ly /
                                                                       NULLIFZERO(activating_cash_gross_revenue_mtd_ly)),
                                  0)
                             + COALESCE(nonactivating_cash_gross_revenue_mtd *
                                        (nonactivating_cash_gross_revenue_month_tot_ly /
                                         NULLIFZERO(nonactivating_cash_gross_revenue_mtd_ly)),
                                        0)                                                                                                              AS cash_gross_revenue,
                         COALESCE(COALESCE(product_order_cash_refund_amount_mtd, 0) *
                                  (COALESCE(product_order_cash_refund_amount_month_tot_ly, 0)
                                      / NULLIFZERO(product_order_cash_refund_amount_mtd_ly)), 0)
                             + COALESCE(COALESCE(billing_cash_refund_amount_mtd, 0) *
                                        (COALESCE(billing_cash_refund_amount_month_tot_ly, 0)
                                            / NULLIFZERO(billing_cash_refund_amount_mtd_ly)),
                                        0)                                                                                                              AS cash_refund_amount,
                         COALESCE(COALESCE(product_order_cash_chargeback_amount_mtd, 0) *
                                  (COALESCE(product_order_cash_chargeback_amount_month_tot_ly, 0) /
                                   NULLIFZERO(product_order_cash_chargeback_amount_mtd_ly)), 0)
                             + COALESCE(COALESCE(billing_cash_chargeback_amount_mtd, 0) *
                                        (COALESCE(billing_cash_chargeback_amount_month_tot_ly, 0) /
                                         NULLIFZERO(billing_cash_chargeback_amount_mtd_ly)),
                                        0)                                                                                                              AS chargeback_amount,
                         (COALESCE(activating_cash_net_revenue_mtd, 0) *
                          (COALESCE(activating_cash_net_revenue_month_tot_ly, 0)
                              / NULLIFZERO(activating_cash_net_revenue_mtd_ly)))
                             + (COALESCE(nonactivating_cash_net_revenue_mtd, 0) *
                                (COALESCE(nonactivating_cash_net_revenue_month_tot_ly, 0)
                                    /
                                 NULLIFZERO(nonactivating_cash_net_revenue_mtd_ly)))                                                                    AS cash_net_revenue,
                         (COALESCE(product_order_cash_refund_amount_mtd *
                                   (product_order_cash_refund_amount_month_tot_ly /
                                    NULLIFZERO(product_order_cash_refund_amount_mtd_ly)),
                                   0)
                             + COALESCE(billing_cash_refund_amount_mtd * (billing_cash_refund_amount_month_tot_ly /
                                                                          NULLIFZERO(billing_cash_refund_amount_mtd_ly)),
                                        0)
                             + COALESCE(product_order_cash_chargeback_amount_mtd *
                                        (product_order_cash_chargeback_amount_month_tot_ly /
                                         NULLIFZERO(product_order_cash_chargeback_amount_mtd_ly)), 0)
                             +
                          COALESCE(billing_cash_chargeback_amount_mtd * (billing_cash_chargeback_amount_month_tot_ly /
                                                                         NULLIFZERO(billing_cash_chargeback_amount_mtd_ly)),
                                   0))
                             / NULLIFZERO((cash_gross_revenue_mtd *
                                           (cash_gross_revenue_month_tot_ly / NULLIFZERO(cash_gross_revenue_mtd_ly))))                                  AS refunds_and_chargebacks_as_percent_of_gross_cash,

                         COALESCE(cash_gross_profit_mtd, 0) * (COALESCE(cash_gross_profit_month_tot_ly, 0) /
                                                               NULLIFZERO(cash_gross_profit_mtd_ly))                                                    AS cash_gross_profit,

                         COALESCE(cash_gross_profit_mtd, 0) *
                         (COALESCE(cash_gross_profit_month_tot_ly, 0) / NULLIFZERO(cash_gross_profit_mtd_ly)) /
                         NULLIFZERO(COALESCE(cash_net_revenue_mtd, 0) * (COALESCE(cash_net_revenue_month_tot_ly, 0) /
                                                                         NULLIFZERO(cash_net_revenue_mtd_ly)))                                          AS cash_gross_profit_percent_net,
                         COALESCE(cash_variable_contribution_profit_mtd, 0) *
                         (COALESCE(cash_variable_contribution_profit_month_tot_ly, 0) /
                          NULLIFZERO(cash_variable_contribution_profit_mtd_ly))                                                                         AS cash_contribution_profit,
                         COALESCE(media_spend_mtd, 0) * v.acq_run_rate_multiplier                                                                       AS media_spend,
                         COALESCE(COALESCE(cash_variable_contribution_profit_mtd, 0) *
                                  (COALESCE(cash_variable_contribution_profit_month_tot_ly, 0) /
                                   NULLIFZERO(cash_variable_contribution_profit_mtd_ly)), 0) - COALESCE(
                                     COALESCE(media_spend_mtd, 0) *
                                     (COALESCE(media_spend_month_tot_ly, 0) / NULLIFZERO(media_spend_mtd_ly)),
                                     0)                                                                                                                 AS cash_contribution_profit_after_media,
                         media_spend_mtd / NULLIFZERO(primary_leads_mtd)                                                                                AS cpl,
                         COALESCE(new_vips_mtd, 0) * v.acq_run_rate_multiplier                                                                          AS new_vips,
                         media_spend_mtd / NULLIFZERO(new_vips_mtd)                                                                                     AS vip_cpa,
                         new_vips_m1_mtd * v.acq_run_rate_multiplier                                                                                    AS m1_vips,
                         new_vips_m1_mtd / NULLIFZERO(leads_mtd)                                                                                        AS m1_lead_to_vip,
                         (new_vips_mtd - new_vips_m1_mtd) * v.acq_run_rate_multiplier                                                                   AS aged_lead_conversions,
                         COALESCE(cancels_mtd, 0) * v.acq_run_rate_multiplier                                                                           AS cancels,
                         (COALESCE(new_vips_mtd, 0) - COALESCE(cancels_mtd, 0)) *
                         v.acq_run_rate_multiplier                                                                                                      AS net_change_in_vips,
                         COALESCE(bop_vips_mtd, 0) + net_change_in_vips                                                                                 AS eop_vips,
                         net_change_in_vips / NULLIFZERO(bop_vips_mtd)                                                                                  AS growth_percent,
                         ((COALESCE(media_spend_mtd, 0) * v.acq_run_rate_multiplier) -
                          (COALESCE(first_guest_product_margin_pre_return_mtd, 0) *
                           (COALESCE(first_guest_product_margin_pre_return_month_tot_ly, 0) /
                            NULLIFZERO(first_guest_product_margin_pre_return_mtd_ly)))) /
                         NULLIFZERO(COALESCE(new_vips_mtd, 0) * v.acq_run_rate_multiplier)                                                              AS xcpa,
                         COALESCE(activating_product_gross_revenue_mtd, 0) *
                         (COALESCE(activating_product_gross_revenue_month_tot_ly, 0) /
                          NULLIFZERO(activating_product_gross_revenue_mtd_ly))                                                                          AS activating_product_gross_revenue,
                         COALESCE(activating_product_net_revenue_mtd, 0) *
                         (COALESCE(activating_product_net_revenue_month_tot_ly, 0) /
                          NULLIFZERO(activating_product_net_revenue_mtd_ly))                                                                            AS activating_product_net_revenue,
                         COALESCE(activating_product_order_count_mtd, 0) *
                         (COALESCE(activating_product_order_count_month_tot_ly, 0) /
                          NULLIFZERO(activating_product_order_count_mtd_ly))                                                                            AS activating_product_order_count,
                         COALESCE(activating_product_gross_revenue_mtd, 0) *
                         (COALESCE(activating_product_gross_revenue_month_tot_ly, 0) /
                          NULLIFZERO(activating_product_gross_revenue_mtd_ly)) /
                         NULLIFZERO(activating_product_order_count_mtd * (activating_product_order_count_month_tot_ly /
                                                                          NULLIFZERO(activating_product_order_count_mtd_ly)))                           AS activating_aov,
                         (COALESCE(activating_unit_count_mtd, 0) *
                          (COALESCE(activating_unit_count_month_tot_ly, 0) / NULLIFZERO(activating_unit_count_mtd_ly)))
                             /
                         NULLIFZERO(activating_product_order_count_mtd * (activating_product_order_count_month_tot_ly /
                                                                          NULLIFZERO(activating_product_order_count_mtd_ly)))                           AS activating_upt,
                         (COALESCE(activating_product_order_product_discount_amount_mtd, 0) *
                          (COALESCE(activating_product_order_product_discount_amount_month_tot_ly, 0)
                              / NULLIFZERO(activating_product_order_product_discount_amount_mtd_ly)))
                             / NULLIFZERO((COALESCE(activating_product_order_product_subtotal_amount_mtd, 0) *
                                           (COALESCE(activating_product_order_product_subtotal_amount_month_tot_ly, 0)
                                               / NULLIFZERO(activating_product_order_product_subtotal_amount_mtd_ly)))
                             - (COALESCE(activating_product_order_noncash_credit_redeemed_amount_mtd, 0) *
                                (COALESCE(activating_product_order_noncash_credit_redeemed_amount_month_tot_ly, 0)
                                    /
                                 NULLIFZERO(activating_product_order_noncash_credit_redeemed_amount_mtd_ly))))                                          AS activating_discount_rate_percent,
                         COALESCE(activating_product_gross_profit_mtd, 0) *
                         (COALESCE(activating_product_gross_profit_month_tot_ly, 0) /
                          NULLIFZERO(activating_product_gross_profit_mtd_ly))                                                                           AS activating_product_gross_profit,
                         (COALESCE(activating_product_gross_profit_mtd, 0) *
                          (COALESCE(activating_product_gross_profit_month_tot_ly, 0) /
                           NULLIFZERO(activating_product_gross_profit_mtd_ly))) /
                         NULLIFZERO(activating_product_net_revenue_mtd * (activating_product_net_revenue_month_tot_ly /
                                                                          NULLIFZERO(activating_product_net_revenue_mtd_ly)))                           AS activating_product_gross_profit_percent_net,
                         COALESCE(nonactivating_product_gross_revenue_mtd, 0) *
                         (COALESCE(nonactivating_product_gross_revenue_month_tot_ly, 0) /
                          NULLIFZERO(nonactivating_product_gross_revenue_mtd_ly))                                                                       AS nonactivating_product_gross_revenue,
                         COALESCE(nonactivating_product_net_revenue_mtd, 0) *
                         (COALESCE(nonactivating_product_net_revenue_month_tot_ly, 0) /
                          NULLIFZERO(nonactivating_product_net_revenue_mtd_ly))                                                                         AS nonactivating_product_net_revenue,
                         COALESCE(nonactivating_product_order_count_mtd, 0) *
                         (COALESCE(nonactivating_product_order_count_month_tot_ly, 0) /
                          NULLIFZERO(nonactivating_product_order_count_mtd_ly))                                                                         AS nonactivating_product_order_count,
                         COALESCE(nonactivating_product_gross_revenue_mtd, 0) *
                         (COALESCE(nonactivating_product_gross_revenue_month_tot_ly, 0) /
                          NULLIFZERO(nonactivating_product_gross_revenue_mtd_ly)) /
                         NULLIFZERO(nonactivating_product_order_count_mtd *
                                    (nonactivating_product_order_count_month_tot_ly /
                                     NULLIFZERO(nonactivating_product_order_count_mtd_ly)))                                                             AS nonactivating_aov,
                         (COALESCE(nonactivating_unit_count_mtd, 0) *
                          (COALESCE(nonactivating_unit_count_month_tot_ly, 0) /
                           NULLIFZERO(nonactivating_unit_count_mtd_ly))) /
                         NULLIFZERO((nonactivating_product_order_count_mtd *
                                     (nonactivating_product_order_count_month_tot_ly /
                                      NULLIFZERO(nonactivating_product_order_count_mtd_ly))))                                                           AS nonactivating_upt,
                         (COALESCE(nonactivating_product_order_product_discount_amount_mtd, 0) *
                          (COALESCE(nonactivating_product_order_product_discount_amount_month_tot_ly, 0)
                              / NULLIFZERO(nonactivating_product_order_product_discount_amount_mtd_ly)))
                             / NULLIFZERO((COALESCE(nonactivating_product_order_product_subtotal_amount_mtd, 0) *
                                           (COALESCE(nonactivating_product_order_product_subtotal_amount_month_tot_ly,
                                                     0)
                                               /
                                            NULLIFZERO(nonactivating_product_order_product_subtotal_amount_mtd_ly)))
                             - (COALESCE(nonactivating_product_order_noncash_credit_redeemed_amount_mtd, 0) *
                                (COALESCE(nonactivating_product_order_noncash_credit_redeemed_amount_month_tot_ly, 0)
                                    /
                                 NULLIFZERO(nonactivating_product_order_noncash_credit_redeemed_amount_mtd_ly))))                                       AS nonactivating_discount_rate_percent,
                         COALESCE(nonactivating_product_gross_profit_mtd, 0) *
                         (COALESCE(nonactivating_product_gross_profit_month_tot_ly, 0) /
                          NULLIFZERO(nonactivating_product_gross_profit_mtd_ly))                                                                        AS nonactivating_product_gross_profit,
                         (COALESCE(nonactivating_product_gross_profit_mtd, 0) *
                          (COALESCE(nonactivating_product_gross_profit_month_tot_ly, 0) /
                           NULLIFZERO(nonactivating_product_gross_profit_mtd_ly))) /
                         NULLIFZERO(nonactivating_product_net_revenue_mtd *
                                    (nonactivating_product_net_revenue_month_tot_ly /
                                     NULLIFZERO(nonactivating_product_net_revenue_mtd_ly)))                                                             AS nonactivating_product_gross_profit_percent_net,
                         COALESCE(guest_product_gross_revenue_mtd, 0) *
                         (COALESCE(guest_product_gross_revenue_month_tot_ly, 0) /
                          NULLIFZERO(guest_product_gross_revenue_mtd_ly))                                                                               AS guest_product_gross_revenue,
                         COALESCE(guest_product_net_revenue_mtd, 0) *
                         (COALESCE(guest_product_net_revenue_month_tot_ly, 0) /
                          NULLIFZERO(guest_product_net_revenue_mtd_ly))                                                                                 AS guest_product_net_revenue,
                         COALESCE(guest_product_order_count_mtd, 0) *
                         (COALESCE(guest_product_order_count_month_tot_ly, 0) /
                          NULLIFZERO(guest_product_order_count_mtd_ly))                                                                                 AS guest_product_order_count,
                         COALESCE(guest_product_gross_profit_mtd, 0) *
                         (COALESCE(guest_product_gross_profit_month_tot_ly, 0) /
                          NULLIFZERO(guest_product_gross_profit_mtd_ly))                                                                                AS guest_product_gross_profit,
                         (COALESCE(guest_product_gross_revenue_mtd, 0) *
                          (COALESCE(guest_product_gross_revenue_month_tot_ly, 0) /
                           NULLIFZERO(guest_product_gross_revenue_mtd_ly))) /
                         NULLIFZERO((COALESCE(guest_product_order_count_mtd, 0) *
                                     (COALESCE(guest_product_order_count_month_tot_ly, 0) /
                                      NULLIFZERO(guest_product_order_count_mtd_ly))))                                                                   AS guest_aov,
                         (COALESCE(guest_unit_count_mtd, 0) *
                          (COALESCE(guest_unit_count_month_tot_ly, 0) / NULLIFZERO(guest_unit_count_mtd_ly))) /
                         NULLIFZERO((COALESCE(guest_product_order_count_mtd, 0) *
                                     (COALESCE(guest_product_order_count_month_tot_ly, 0) /
                                      NULLIFZERO(guest_product_order_count_mtd_ly))))                                                                   AS guest_upt,
                         (COALESCE(guest_product_order_product_discount_amount_mtd, 0) *
                          (COALESCE(guest_product_order_product_discount_amount_month_tot_ly, 0) /
                           NULLIFZERO(guest_product_order_product_discount_amount_mtd_ly))) /
                         NULLIFZERO((COALESCE(guest_product_gross_revenue_mtd, 0) *
                                     (COALESCE(guest_product_gross_revenue_month_tot_ly, 0) /
                                      NULLIFZERO(guest_product_gross_revenue_mtd_ly))) +
                                    (COALESCE(guest_product_order_product_discount_amount_mtd, 0) *
                                     (COALESCE(guest_product_order_product_discount_amount_month_tot_ly, 0) /
                                      NULLIFZERO(guest_product_order_product_discount_amount_mtd_ly))))                                                 AS guest_discount_percent,
                         (COALESCE(guest_product_gross_profit_mtd, 0) *
                          (COALESCE(guest_product_gross_profit_month_tot_ly, 0) /
                           NULLIFZERO(guest_product_gross_profit_mtd_ly))) /
                         NULLIFZERO(COALESCE(guest_product_net_revenue_mtd, 0) *
                                    (COALESCE(guest_product_net_revenue_month_tot_ly, 0) /
                                     NULLIFZERO(guest_product_net_revenue_mtd_ly)))                                                                     AS guest_product_gross_profit_percent,
                         COALESCE(repeat_vip_product_gross_revenue_mtd, 0) *
                         (COALESCE(repeat_vip_product_gross_revenue_month_tot_ly, 0) /
                          NULLIFZERO(repeat_vip_product_gross_revenue_mtd_ly))                                                                          AS repeat_vip_product_gross_revenue,
                         COALESCE(repeat_vip_product_net_revenue_mtd, 0) *
                         (COALESCE(repeat_vip_product_net_revenue_month_tot_ly, 0) /
                          NULLIFZERO(repeat_vip_product_net_revenue_mtd_ly))                                                                            AS repeat_vip_product_net_revenue,
                         COALESCE(repeat_vip_product_order_count_mtd, 0) *
                         (COALESCE(repeat_vip_product_order_count_month_tot_ly, 0) /
                          NULLIFZERO(repeat_vip_product_order_count_mtd_ly))                                                                            AS repeat_vip_product_order_count,
                         COALESCE(repeat_vip_product_gross_profit_mtd, 0) *
                         (COALESCE(repeat_vip_product_gross_profit_month_tot_ly, 0) /
                          NULLIFZERO(repeat_vip_product_gross_profit_mtd_ly))                                                                           AS repeat_vip_product_gross_profit,
                         (COALESCE(repeat_vip_product_gross_revenue_mtd, 0) *
                          (COALESCE(repeat_vip_product_gross_revenue_month_tot_ly, 0) /
                           NULLIFZERO(repeat_vip_product_gross_revenue_mtd_ly))) /
                         NULLIFZERO((COALESCE(repeat_vip_product_order_count_mtd, 0) *
                                     (COALESCE(repeat_vip_product_order_count_month_tot_ly, 0) /
                                      NULLIFZERO(repeat_vip_product_order_count_mtd_ly))))                                                              AS repeat_vip_aov,
                         (COALESCE(repeat_vip_unit_count_mtd, 0) *
                          (COALESCE(repeat_vip_unit_count_month_tot_ly, 0) /
                           NULLIFZERO(repeat_vip_unit_count_mtd_ly))) /
                         NULLIFZERO((COALESCE(repeat_vip_product_order_count_mtd, 0) *
                                     (COALESCE(repeat_vip_product_order_count_month_tot_ly, 0) /
                                      NULLIFZERO(repeat_vip_product_order_count_mtd_ly))))                                                              AS repeat_vip_upt,

                         (COALESCE(repeat_vip_product_order_product_discount_amount_mtd, 0) *
                          (COALESCE(repeat_vip_product_order_product_discount_amount_month_tot_ly, 0) /
                           NULLIFZERO(repeat_vip_product_order_product_discount_amount_mtd_ly))) /
                         NULLIFZERO((COALESCE(repeat_vip_product_gross_revenue_mtd, 0) *
                                     (COALESCE(repeat_vip_product_gross_revenue_month_tot_ly, 0) /
                                      NULLIFZERO(repeat_vip_product_gross_revenue_mtd_ly))) +
                                    (COALESCE(repeat_vip_product_order_product_discount_amount_mtd, 0) *
                                     (COALESCE(repeat_vip_product_order_product_discount_amount_month_tot_ly, 0) /
                                      NULLIFZERO(repeat_vip_product_order_product_discount_amount_mtd_ly))))                                            AS repeat_vip_discount_percent,

                         (COALESCE(repeat_vip_product_gross_profit_mtd, 0) *
                          (COALESCE(repeat_vip_product_gross_profit_month_tot_ly, 0) /
                           NULLIFZERO(repeat_vip_product_gross_profit_mtd_ly))) /
                         NULLIFZERO(COALESCE(repeat_vip_product_net_revenue_mtd, 0) *
                                    (COALESCE(repeat_vip_product_net_revenue_month_tot_ly, 0) /
                                     NULLIFZERO(repeat_vip_product_net_revenue_mtd_ly)))                                                                AS repeat_vip_product_gross_profit_percent,

                         COALESCE(billed_credit_cash_transaction_amount_mtd *
                                  (billed_credit_cash_transaction_amount_month_tot_ly /
                                   NULLIFZERO(billed_credit_cash_transaction_amount_mtd_ly)), 0) - COALESCE(
                                     billed_credit_cash_refund_chargeback_amount_mtd *
                                     (billed_credit_cash_refund_chargeback_amount_month_tot_ly /
                                      NULLIFZERO(billed_credit_cash_refund_chargeback_amount_mtd_ly)), 0) - COALESCE(
                                     billed_cash_credit_redeemed_amount_mtd *
                                     (billed_cash_credit_redeemed_amount_month_tot_ly /
                                      NULLIFZERO(billed_cash_credit_redeemed_amount_mtd_ly)),
                                     0)                                                                                                                 AS net_unredeemed_billed_credits,

                         COALESCE(nonactivating_cash_net_revenue_mtd, 0) *
                         (COALESCE(nonactivating_cash_net_revenue_month_tot_ly, 0) /
                          NULLIFZERO(nonactivating_cash_net_revenue_mtd_ly))                                                                            AS nonactivating_cash_net_revenue,

                         COALESCE(nonactivating_cash_gross_profit_mtd, 0) *
                         (COALESCE(nonactivating_cash_gross_profit_month_tot_ly, 0) /
                          NULLIFZERO(nonactivating_cash_gross_profit_mtd_ly))                                                                           AS nonactivating_cash_gross_profit,

                         (COALESCE(nonactivating_cash_gross_profit_mtd, 0) *
                          (COALESCE(nonactivating_cash_gross_profit_month_tot_ly, 0) /
                           NULLIFZERO(nonactivating_cash_gross_profit_mtd_ly))) /
                         NULLIFZERO(nonactivating_cash_net_revenue_mtd * (nonactivating_cash_net_revenue_month_tot_ly /
                                                                          NULLIFZERO(nonactivating_cash_net_revenue_mtd_ly)))                           AS nonactivating_cash_gross_profit_percent_net,

                         COALESCE(billed_credit_cash_transaction_amount_mtd, 0) *
                         (COALESCE(billed_credit_cash_transaction_amount_month_tot_ly, 0) /
                          NULLIFZERO(billed_credit_cash_transaction_amount_mtd_ly))                                                                     AS billed_credits_transacted,

                         COALESCE(billed_cash_credit_redeemed_amount_mtd, 0) *
                         (COALESCE(billed_cash_credit_redeemed_amount_month_tot_ly, 0) /
                          NULLIFZERO(billed_cash_credit_redeemed_amount_mtd_ly))                                                                        AS billed_credits_redeemed,

                         COALESCE(billed_credit_cash_refund_chargeback_amount_mtd, 0) *
                         (COALESCE(billed_credit_cash_refund_chargeback_amount_month_tot_ly, 0) /
                          NULLIFZERO(billed_credit_cash_refund_chargeback_amount_mtd_ly))                                                               AS billed_credit_cash_refund_chargeback_amount,

                         COALESCE(net_unredeemed_billed_credits, 0) / NULLIFZERO(cash_net_revenue_mtd *
                                                                                 (cash_net_revenue_month_tot_ly / NULLIFZERO(cash_net_revenue_mtd_ly))) AS net_unredeemed_billed_credits_as_percent_of_net_cash_revenue,


                         COALESCE(product_order_cash_credit_refund_amount_mtd *
                                  (product_order_cash_credit_refund_amount_month_tot_ly /
                                   NULLIFZERO(product_order_cash_credit_refund_amount_mtd_ly)), 0) + COALESCE(
                                     product_order_noncash_credit_refund_amount_mtd *
                                     (product_order_noncash_credit_refund_amount_month_tot_ly /
                                      NULLIFZERO(product_order_noncash_credit_refund_amount_mtd_ly)),
                                     0)                                                                                                                 AS refunded_as_credit,

                         COALESCE(refund_cash_credit_issued_amount_mtd, 0) *
                         (COALESCE(refund_cash_credit_issued_amount_month_tot_ly, 0) /
                          NULLIFZERO(refund_cash_credit_issued_amount_mtd_ly))                                                                          AS refund_cash_credit_issued_amount,


                         COALESCE(refund_cash_credit_redeemed_amount_mtd, 0) *
                         (COALESCE(refund_cash_credit_redeemed_amount_month_tot_ly, 0) /
                          NULLIFZERO(refund_cash_credit_redeemed_amount_mtd_ly))                                                                        AS refund_credit_redeemed,

                         COALESCE(refund_cash_credit_issued_amount, 0) -
                         (COALESCE(refund_cash_credit_cancelled_amount_mtd, 0) *
                          (COALESCE(refund_cash_credit_cancelled_amount_month_tot_ly, 0) /
                           NULLIFZERO(refund_cash_credit_cancelled_amount_mtd_ly))) -
                         COALESCE(refund_cash_credit_redeemed_amount, 0)                                                                                AS net_unredeemed_refund_credit,

                         COALESCE(net_unredeemed_billed_credits, 0) +
                         COALESCE(net_unredeemed_refund_credit, 0)                                                                                      AS billed_credit_deferred_revenue,

                         COALESCE(bop_vips_mtd, 0) *
                         (COALESCE(bop_vips_month_tot_ly, 0) / NULLIFZERO(bop_vips_mtd_ly))                                                             AS bop_vips,

                         (COALESCE(billing_order_transaction_count_mtd, 0) *
                          (COALESCE(billing_order_transaction_count_month_tot_ly, 0) /
                           NULLIFZERO(billing_order_transaction_count_mtd_ly))) /
                         NULLIFZERO(bop_vips_mtd * (bop_vips_month_tot_ly / NULLIFZERO(bop_vips_mtd_ly)))                                               AS billed_credit_percent_of_bop_vips,

                         (COALESCE(billed_cash_credit_redeemed_equivalent_count_mtd, 0) *
                          (COALESCE(billed_cash_credit_redeemed_equivalent_count_month_tot_ly, 0) /
                           NULLIFZERO(billed_cash_credit_redeemed_equivalent_count_mtd_ly)))
                             / NULLIFZERO(COALESCE(billed_cash_credit_issued_equivalent_count_mtd, 0) *
                                          (COALESCE(billed_cash_credit_issued_equivalent_count_month_tot_ly, 0) /
                                           NULLIFZERO(billed_cash_credit_issued_equivalent_count_mtd_ly)))                                              AS billed_credit_redeemed_percent,


                         (COALESCE(billed_cash_credit_cancelled_equivalent_count_mtd, 0) *
                          (COALESCE(billed_cash_credit_cancelled_equivalent_count_month_tot_ly, 0) /
                           NULLIFZERO(billed_cash_credit_cancelled_equivalent_count_mtd_ly)))
                             / NULLIFZERO(COALESCE(billed_cash_credit_issued_equivalent_count_mtd, 0) *
                                          (COALESCE(billed_cash_credit_issued_equivalent_count_month_tot_ly, 0) /
                                           NULLIFZERO(billed_cash_credit_issued_equivalent_count_mtd_ly)))                                              AS billed_credit_cancelled_percent,

                         NULL                                                                                                                           AS skip_rate_percent,

                         (COALESCE(product_order_landed_product_cost_amount_mtd, 0) *
                          (COALESCE(product_order_landed_product_cost_amount_month_tot_ly, 0) /
                           NULLIFZERO(product_order_landed_product_cost_amount_mtd_ly)))
                             + (COALESCE(product_order_exchange_product_cost_amount_mtd, 0) *
                                (COALESCE(product_order_exchange_product_cost_amount_month_tot_ly, 0) /
                                 NULLIFZERO(product_order_exchange_product_cost_amount_mtd_ly)))
                             + (COALESCE(product_order_reship_product_cost_amount_mtd, 0) *
                                (COALESCE(product_order_reship_product_cost_amount_month_tot_ly, 0) /
                                 NULLIFZERO(product_order_reship_product_cost_amount_mtd_ly)))                                                          AS landed_product_cost,
                         COALESCE((COALESCE(COALESCE(product_order_landed_product_cost_amount_mtd, 0) +
                                            COALESCE(product_order_exchange_product_cost_amount_mtd, 0) +
                                            COALESCE(product_order_reship_product_cost_amount_mtd, 0), 0) -
                                   COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd, 0)), 0) *
                         (COALESCE((
                                           COALESCE((
                                                            COALESCE(product_order_landed_product_cost_amount_month_tot_ly, 0) +
                                                            COALESCE(product_order_exchange_product_cost_amount_month_tot_ly, 0) +
                                                            COALESCE(product_order_reship_product_cost_amount_month_tot_ly, 0)),
                                                    0) -
                                           COALESCE(product_cost_returned_resaleable_incl_reship_exch_month_tot_ly,
                                                    0)), 0) /
                          NULLIFZERO(COALESCE((
                                                      COALESCE((
                                                                       COALESCE(product_order_landed_product_cost_amount_mtd_ly, 0) +
                                                                       COALESCE(product_order_exchange_product_cost_amount_mtd_ly, 0) +
                                                                       COALESCE(product_order_reship_product_cost_amount_mtd_ly, 0)),
                                                               0) -
                                                      COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd_ly, 0)),
                                              0)))                                                                                                      AS total_product_cost_net_of_returns,

                         (COALESCE(product_order_shipping_cost_amount_mtd, 0) *
                          (COALESCE(product_order_shipping_cost_amount_month_tot_ly, 0) /
                           NULLIFZERO(product_order_shipping_cost_amount_mtd_ly)))
                             + (COALESCE(product_order_reship_shipping_cost_amount_mtd, 0) *
                                (COALESCE(product_order_reship_shipping_cost_amount_month_tot_ly, 0) /
                                 NULLIFZERO(product_order_reship_shipping_cost_amount_mtd_ly)))
                             + (COALESCE(product_order_exchange_shipping_cost_amount_mtd, 0) *
                                (COALESCE(product_order_exchange_shipping_cost_amount_month_tot_ly, 0) /
                                 NULLIFZERO(product_order_exchange_shipping_cost_amount_mtd_ly)))                                                       AS outbound_shipping_costs,

                         (COALESCE(product_order_shipping_supplies_cost_amount_mtd, 0) *
                          (COALESCE(product_order_shipping_supplies_cost_amount_month_tot_ly, 0)
                              / NULLIFZERO(product_order_shipping_supplies_cost_amount_mtd_ly)))
                             + (COALESCE(product_order_exchange_shipping_supplies_cost_amount_mtd, 0) *
                                (COALESCE(product_order_exchange_shipping_supplies_cost_amount_month_tot_ly, 0)
                                    / NULLIFZERO(product_order_exchange_shipping_supplies_cost_amount_mtd_ly)))
                             + (COALESCE(product_order_reship_shipping_supplies_cost_amount_mtd, 0) *
                                (COALESCE(product_order_reship_shipping_supplies_cost_amount_month_tot_ly, 0)
                                    /
                                 NULLIFZERO(product_order_reship_shipping_supplies_cost_amount_mtd_ly)))                                                AS shipping_supplies_costs,
                         COALESCE(return_shipping_costs_incl_reship_exch_mtd, 0) *
                         (COALESCE(return_shipping_costs_incl_reship_exch_month_tot_ly, 0) /
                          NULLIFZERO(return_shipping_costs_incl_reship_exch_mtd_ly))                                                                    AS return_shipping_costs,
                         COALESCE(misc_cogs_amount_mtd, 0) * (COALESCE(misc_cogs_amount_month_tot_ly, 0) /
                                                              NULLIFZERO(misc_cogs_amount_mtd_ly))                                                      AS misc_cogs,
                         (COALESCE(product_order_landed_product_cost_amount_mtd, 0) *
                          (COALESCE(product_order_landed_product_cost_amount_month_tot_ly, 0) /
                           NULLIFZERO(product_order_landed_product_cost_amount_mtd_ly)))
                             + (COALESCE(product_order_exchange_product_cost_amount_mtd, 0) *
                                (COALESCE(product_order_exchange_product_cost_amount_month_tot_ly, 0) /
                                 NULLIFZERO(product_order_exchange_product_cost_amount_mtd_ly)))
                             + (COALESCE(product_order_reship_product_cost_amount_mtd, 0) *
                                (COALESCE(product_order_reship_product_cost_amount_month_tot_ly, 0) /
                                 NULLIFZERO(product_order_reship_product_cost_amount_mtd_ly)))
                             + (COALESCE(product_order_shipping_cost_amount_mtd, 0) *
                                (COALESCE(product_order_shipping_cost_amount_month_tot_ly, 0) /
                                 NULLIFZERO(product_order_shipping_cost_amount_mtd_ly)))
                             + (COALESCE(product_order_reship_shipping_cost_amount_mtd, 0) *
                                (COALESCE(product_order_reship_shipping_cost_amount_month_tot_ly, 0) /
                                 NULLIFZERO(product_order_reship_shipping_cost_amount_mtd_ly)))
                             + (COALESCE(product_order_exchange_shipping_cost_amount_mtd, 0) *
                                (COALESCE(product_order_exchange_shipping_cost_amount_month_tot_ly, 0) /
                                 NULLIFZERO(product_order_exchange_shipping_cost_amount_mtd_ly)))
                             + (COALESCE(return_shipping_costs_incl_reship_exch_mtd, 0) *
                                (COALESCE(return_shipping_costs_incl_reship_exch_month_tot_ly, 0) /
                                 NULLIFZERO(return_shipping_costs_incl_reship_exch_mtd_ly)))
                             + (COALESCE(product_order_shipping_supplies_cost_amount_mtd, 0) *
                                (COALESCE(product_order_shipping_supplies_cost_amount_month_tot_ly, 0) /
                                 NULLIFZERO(product_order_shipping_supplies_cost_amount_mtd_ly)))
                             + (COALESCE(product_order_exchange_shipping_supplies_cost_amount_mtd, 0) *
                                (COALESCE(product_order_exchange_shipping_supplies_cost_amount_month_tot_ly, 0) /
                                 NULLIFZERO(product_order_exchange_shipping_supplies_cost_amount_mtd_ly)))
                             + (COALESCE(product_order_reship_shipping_supplies_cost_amount_mtd, 0) *
                                (COALESCE(product_order_reship_shipping_supplies_cost_amount_month_tot_ly, 0) /
                                 NULLIFZERO(product_order_reship_shipping_supplies_cost_amount_mtd_ly)))
                             + (COALESCE(misc_cogs_amount_mtd, 0) *
                                (COALESCE(misc_cogs_amount_month_tot_ly, 0) / NULLIFZERO(misc_cogs_amount_mtd_ly)))
                             - (COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd, 0) *
                                (COALESCE(product_cost_returned_resaleable_incl_reship_exch_month_tot_ly, 0) /
                                 NULLIFZERO(product_cost_returned_resaleable_incl_reship_exch_mtd_ly)))
                                                                                                                                                        AS total_cogs,
                         (COALESCE(unit_count_mtd, 0) *
                          (COALESCE(unit_count_month_tot_ly, 0) / NULLIFZERO(unit_count_mtd_ly))) +
                         (COALESCE(product_order_reship_unit_count_mtd, 0) *
                          (COALESCE(product_order_reship_unit_count_month_tot_ly, 0) /
                           NULLIFZERO(product_order_reship_unit_count_mtd_ly))) +
                         (COALESCE(product_order_exchange_unit_count_mtd, 0) *
                          (COALESCE(product_order_exchange_unit_count_month_tot_ly, 0) /
                           NULLIFZERO(product_order_exchange_unit_count_mtd_ly)))                                                                       AS unit_count_incl_reship_exch,
                         (COALESCE(product_order_landed_product_cost_amount_mtd, 0) *
                          (COALESCE(product_order_landed_product_cost_amount_month_tot_ly, 0) /
                           NULLIFZERO(product_order_landed_product_cost_amount_mtd_ly)) +
                          COALESCE(product_order_exchange_product_cost_amount_mtd, 0) *
                          (COALESCE(product_order_exchange_product_cost_amount_month_tot_ly, 0) /
                           NULLIFZERO(product_order_exchange_product_cost_amount_mtd_ly)) +
                          COALESCE(product_order_reship_product_cost_amount_mtd, 0) *
                          (COALESCE(product_order_reship_product_cost_amount_month_tot_ly, 0) /
                           NULLIFZERO(product_order_reship_product_cost_amount_mtd_ly))
                             ) / NULLIFZERO(COALESCE(unit_count_mtd, 0) *
                                            (COALESCE(unit_count_month_tot_ly, 0) / NULLIFZERO(unit_count_mtd_ly)) +
                                            COALESCE(product_order_reship_unit_count_mtd, 0) *
                                            (COALESCE(product_order_reship_unit_count_month_tot_ly, 0) /
                                             NULLIFZERO(product_order_reship_unit_count_mtd_ly)) +
                                            COALESCE(product_order_exchange_unit_count_mtd, 0) *
                                            (COALESCE(product_order_exchange_unit_count_month_tot_ly, 0) /
                                             NULLIFZERO(product_order_exchange_unit_count_mtd_ly)))                                                     AS auc,
                         (total_cogs / NULLIFZERO((COALESCE(product_order_count_mtd, 0) *
                                                   (COALESCE(product_order_count_month_tot_ly, 0) /
                                                    NULLIFZERO(product_order_count_mtd_ly)))
                             + (COALESCE(product_order_reship_order_count_mtd, 0) *
                                (COALESCE(product_order_reship_order_count_month_tot_ly, 0) /
                                 NULLIFZERO(product_order_reship_order_count_mtd_ly)))
                             + COALESCE(product_order_exchange_order_count_mtd, 0) *
                               (COALESCE(product_order_exchange_order_count_month_tot_ly, 0) /
                                NULLIFZERO(product_order_exchange_order_count_mtd_ly))))                                                                AS product_cost_per_order,
                         (COALESCE(COALESCE(product_order_shipping_cost_amount_mtd, 0) +
                                   COALESCE(product_order_reship_shipping_cost_amount_mtd, 0) +
                                   COALESCE(product_order_exchange_shipping_cost_amount_mtd, 0), 0) *
                          (COALESCE((
                                            COALESCE(product_order_shipping_cost_amount_month_tot_ly, 0) +
                                            COALESCE(product_order_reship_shipping_cost_amount_month_tot_ly, 0) +
                                            COALESCE(product_order_exchange_shipping_cost_amount_month_tot_ly, 0)), 0) /
                           NULLIFZERO((
                                   COALESCE(product_order_shipping_cost_amount_mtd_ly, 0) +
                                   COALESCE(product_order_reship_shipping_cost_amount_mtd_ly, 0) +
                                   COALESCE(product_order_exchange_shipping_cost_amount_mtd_ly, 0)))))
                             / NULLIFZERO(
                                     (COALESCE(product_order_count_mtd, 0) *
                                      (COALESCE(product_order_count_month_tot_ly, 0) /
                                       NULLIFZERO(product_order_count_mtd_ly)))
                                     + (COALESCE(product_order_reship_order_count_mtd, 0) *
                                        (COALESCE(product_order_reship_order_count_month_tot_ly, 0) /
                                         NULLIFZERO(product_order_reship_order_count_mtd_ly)))
                                     + (COALESCE(product_order_exchange_order_count_mtd, 0) *
                                        (COALESCE(product_order_exchange_order_count_month_tot_ly, 0) /
                                         NULLIFZERO(product_order_exchange_order_count_mtd_ly)))
                             )                                                                                                                          AS shipping_cost_incl_shipping_supplies_per_order
                  FROM _daily_cash_final_output dcfo
                           JOIN _variables v ON v.report_date = dcfo.date
                  WHERE segment IN ('Report Date')),
     _budget AS (SELECT v.report_date                                                        AS date,
                        report_mapping,
                        'Budget'                                                             AS source,
                        segment,
                        cash_gross_revenue_budget                                            AS cash_gross_revenue,
                        product_order_and_billing_cash_refund_budget                         AS cash_refund_amount,
                        product_order_and_billing_chargebacks_budget                         AS chargeback_amount,
                        cash_net_revenue_budget                                              AS cash_net_revenue,
                        (COALESCE(product_order_and_billing_cash_refund_budget, 0) +
                         COALESCE(product_order_and_billing_chargebacks_budget, 0)) /
                        NULLIFZERO(cash_gross_revenue_budget)                                AS refunds_and_chargebacks_as_percent_of_gross_cash,
                        cash_gross_profit_budget                                             AS cash_gross_profit,
                        cash_gross_profit_budget / NULLIFZERO(cash_net_revenue_budget)       AS cash_gross_profit_percent_net,
                        COALESCE(cash_contribution_after_media_budget, 0) +
                        COALESCE(media_spend_budget, 0)                                      AS cash_contribution_profit,
                        media_spend_budget                                                   AS media_spend,
                        COALESCE(cash_contribution_after_media_budget, 0)                    AS cash_contribution_profit_after_media,
                        media_spend_budget / NULLIFZERO(leads_budget)                        AS cpl,
                        new_vips_budget                                                      AS new_vips,
                        media_spend_budget / NULLIFZERO(new_vips_budget)                     AS vip_cpa,
                        new_vips_m1_budget                                                   AS m1_vips,
                        new_vips_m1_budget / NULLIFZERO(leads_budget)                        AS m1_lead_to_vip,
                        new_vips_budget - new_vips_m1_budget                                 AS aged_lead_conversions,
                        cancels_budget                                                       AS cancels,
                        COALESCE(new_vips_budget, 0) - COALESCE(cancels_budget, 0)           AS net_change_in_vips,
                        COALESCE(bop_vips_budget, 0) + COALESCE(net_change_in_vips, 0)       AS eop_vips,
                        net_change_in_vips / NULLIFZERO(bop_vips_budget)                     AS growth_percent,
                        NULL                                                                 AS xcpa,
                        activating_product_gross_revenue_budget                              AS activating_product_gross_revenue,
                        activating_product_net_revenue_budget                                AS activating_product_net_revenue,
                        activating_product_order_count_budget                                AS activating_product_order_count,
                        activating_product_gross_revenue_budget /
                        NULLIFZERO(activating_product_order_count_budget)                    AS activating_aov,
                        activating_unit_count_budget /
                        NULLIFZERO(activating_product_order_count_budget)                    AS activating_upt,
                        activating_discount_percent_budget                                   AS activating_discount_rate_percent,
                        activating_product_gross_profit_budget                               AS activating_product_gross_profit,
                        activating_product_gross_profit_budget /
                        NULLIFZERO(activating_product_net_revenue_budget)                    AS activating_product_gross_profit_percent_net,
                        nonactivating_product_gross_revenue_budget                           AS nonactivating_product_gross_revenue,
                        nonactivating_product_net_revenue_budget                             AS nonactivating_product_net_revenue,
                        nonactivating_product_order_count_budget                             AS nonactivating_product_order_count,
                        nonactivating_product_gross_revenue_budget /
                        NULLIFZERO(nonactivating_product_order_count_budget)                 AS nonactivating_aov,
                        nonactivating_unit_count_budget /
                        NULLIFZERO(nonactivating_product_order_count_budget)                 AS nonactivating_upt,
                        nonactivating_discount_percent_budget                                AS nonactivating_discount_rate_percent,
                        nonactivating_product_gross_profit_budget                            AS nonactivating_product_gross_profit,
                        nonactivating_product_gross_profit_budget /
                        NULLIFZERO(nonactivating_product_net_revenue_budget)                 AS nonactivating_product_gross_profit_percent_net,
                        NULL                                                                 AS guest_product_gross_revenue,
                        NULL                                                                 AS guest_product_net_revenue,
                        NULL                                                                 AS guest_product_order_count,
                        NULL                                                                 AS guest_product_gross_profit,
                        NULL                                                                 AS guest_aov,
                        NULL                                                                 AS guest_upt,
                        NULL                                                                 AS guest_discount_percent,
                        NULL                                                                 AS guest_product_gross_profit_percent,
                        NULL                                                                 AS repeat_vip_product_gross_revenue,
                        NULL                                                                 AS repeat_vip_product_net_revenue,
                        NULL                                                                 AS repeat_vip_product_order_count,
                        NULL                                                                 AS repeat_vip_product_gross_profit,
                        NULL                                                                 AS repeat_vip_aov,
                        NULL                                                                 AS repeat_vip_upt,
                        NULL                                                                 AS repeat_vip_discount_percent,
                        NULL                                                                 AS repeat_vip_product_gross_profit_percent,
                        net_unredeemed_credit_billings_budget                                AS net_unredeemed_billed_credits,
                        nonactivating_cash_net_revenue_budget                                AS nonactivating_cash_net_revenue,
                        nonactivating_cash_gross_profit_budget                               AS nonactivating_cash_gross_profit,
                        nonactivating_cash_gross_profit_budget /
                        NULLIFZERO(nonactivating_cash_net_revenue_budget)                    AS nonactivating_cash_gross_profit_percent_net,
                        billed_credit_cash_transaction_amount_budget                         AS billed_credits_transacted,
                        billed_cash_credit_redeemed_amount_budget                            AS billed_credits_redeemed,
                        billed_credit_cash_refund_chargeback_amount_budget                   AS billed_credit_cash_refund_chargeback_amount,
                        net_unredeemed_credit_billings_budget /
                        NULLIFZERO(cash_net_revenue_budget)                                  AS net_unredeemed_billed_credits_as_percent_of_net_cash_revenue,
                        COALESCE(product_order_cash_credit_refund_amount_budget, 0) +
                        COALESCE(product_order_noncash_credit_refund_amount_budget, 0)       AS refunded_as_credit,
                        refund_cash_credit_issued_amount_budget                              AS refund_cash_credit_issued_amount,
                        refund_cash_credit_redeemed_amount_budget                            AS refund_credit_redeemed,
                        COALESCE(net_unredeemed_refund_credit_budget, 0)                     AS net_unredeemed_refund_credit,
                        COALESCE(net_unredeemed_credit_billings_budget, 0) +
                        COALESCE(net_unredeemed_refund_credit_budget, 0)                     AS billed_credit_deferred_revenue,
                        bop_vips_budget                                                      AS bop_vips,
                        billing_order_transaction_count_budget / NULLIFZERO(bop_vips_budget) AS billed_credit_percent_of_bop_vips,
                        membership_credits_redeemed_count_budget /
                        NULLIFZERO(billing_order_transaction_count_budget)                   AS billed_credit_redeemed_percent,
                        membership_credits_cancelled_count_budget /
                        NULLIFZERO(billing_order_transaction_count_budget)                   AS billed_credit_cancelled_percent,
                        NULL                                                                 AS skip_rate_percent,
                        product_cost_calc_budget                                             AS landed_product_cost,
                        NULL                                                                 AS total_product_cost_net_of_returns,
                        shipping_cost_incl_reship_exch_budget                                AS outbound_shipping_costs,
                        shipping_supplies_cost_incl_reship_exch_budget                       AS shipping_supplies_costs,
                        return_shipping_costs_incl_reship_exch_budget                        AS return_shipping_costs,
                        misc_cogs_amount_budget                                              AS misc_cogs,
                        total_cogs_budget                                                    AS total_cogs,
                        COALESCE(unit_count_budget, 0)                                       AS unit_count_incl_reship_exch,
                        NULL                                                                 AS auc,
                        NULL                                                                 AS product_cost_per_order,
                        NULL                                                                 AS shipping_cost_incl_shipping_supplies_per_order
                 FROM _daily_cash_final_output dcfo
                          JOIN (SELECT report_date, segment
                                FROM _variables
                                WHERE segment IN ('MTD', 'LM', 'LY')
                                UNION
                                SELECT last_day_of_month AS report_date, 'EOM' AS segment
                                FROM _report_months) v ON v.report_date = dcfo.date

                 UNION ALL

                 SELECT date,
                        dcfo.report_mapping,
                        'Budget'                                                             AS source,
                        'RR'                                                                 AS segment,
                        cash_gross_revenue_budget                                            AS cash_gross_revenue,
                        product_order_and_billing_cash_refund_budget                         AS cash_refund_amount,
                        product_order_and_billing_chargebacks_budget                         AS chargeback_amount,
                        cash_net_revenue_budget                                              AS cash_net_revenue,
                        (COALESCE(product_order_and_billing_cash_refund_budget, 0) +
                         COALESCE(product_order_and_billing_chargebacks_budget, 0)) /
                        NULLIFZERO(cash_gross_revenue_budget)                                AS refunds_and_chargebacks_as_percent_of_gross_cash,
                        cash_gross_profit_budget                                             AS cash_gross_profit,
                        cash_gross_profit_budget / NULLIFZERO(cash_net_revenue_budget)       AS cash_gross_profit_percent_net,
                        COALESCE(cash_contribution_after_media_budget, 0) +
                        COALESCE(media_spend_budget, 0)                                      AS cash_contribution_profit,
                        media_spend_budget                                                   AS media_spend,
                        COALESCE(cash_contribution_after_media_budget, 0)                    AS cash_contribution_profit_after_media,
                        media_spend_budget / NULLIFZERO(leads_budget)                        AS cpl,
                        new_vips_budget                                                      AS new_vips,
                        media_spend_budget / NULLIFZERO(new_vips_budget)                     AS vip_cpa,
                        new_vips_m1_budget                                                   AS m1_vips,
                        new_vips_m1_budget / NULLIFZERO(leads_budget)                        AS m1_lead_to_vip,
                        new_vips_budget - new_vips_m1_budget                                 AS aged_lead_conversions,
                        cancels_budget                                                       AS cancels,
                        COALESCE(new_vips_budget, 0) - COALESCE(cancels_budget, 0)           AS net_change_in_vips,
                        COALESCE(bop_vips_budget, 0) + COALESCE(net_change_in_vips, 0)       AS eop_vips,
                        net_change_in_vips / NULLIFZERO(bop_vips_budget)                     AS growth_percent,
                        NULL                                                                 AS xcpa,
                        activating_product_gross_revenue_budget                              AS activating_product_gross_revenue,
                        activating_product_net_revenue_budget                                AS activating_product_net_revenue,
                        activating_product_order_count_budget                                AS activating_product_order_count,
                        activating_product_gross_revenue_budget /
                        NULLIFZERO(activating_product_order_count_budget)                    AS activating_aov,
                        activating_unit_count_budget /
                        NULLIFZERO(activating_product_order_count_budget)                    AS activating_upt,
                        activating_discount_percent_budget                                   AS activating_discount_rate_percent,
                        activating_product_gross_profit_budget                               AS activating_product_gross_profit,
                        activating_product_gross_profit_budget /
                        NULLIFZERO(activating_product_net_revenue_budget)                    AS activating_product_gross_profit_percent_net,
                        nonactivating_product_gross_revenue_budget                           AS nonactivating_product_gross_revenue,
                        nonactivating_product_net_revenue_budget                             AS nonactivating_product_net_revenue,
                        nonactivating_product_order_count_budget                             AS nonactivating_product_order_count,
                        nonactivating_product_gross_revenue_budget /
                        NULLIFZERO(nonactivating_product_order_count_budget)                 AS nonactivating_aov,
                        nonactivating_unit_count_budget /
                        NULLIFZERO(nonactivating_product_order_count_budget)                 AS nonactivating_upt,
                        nonactivating_discount_percent_budget                                AS nonactivating_discount_rate_percent,
                        nonactivating_product_gross_profit_budget                            AS nonactivating_product_gross_profit,
                        nonactivating_product_gross_profit_budget /
                        NULLIFZERO(nonactivating_product_net_revenue_budget)                 AS nonactivating_product_gross_profit_percent_net,
                        NULL                                                                 AS guest_product_gross_revenue,
                        NULL                                                                 AS guest_product_net_revenue,
                        NULL                                                                 AS guest_product_order_count,
                        NULL                                                                 AS guest_product_gross_profit,
                        NULL                                                                 AS guest_aov,
                        NULL                                                                 AS guest_upt,
                        NULL                                                                 AS guest_discount_percent,
                        NULL                                                                 AS guest_product_gross_profit_percent,
                        NULL                                                                 AS repeat_vip_product_gross_revenue,
                        NULL                                                                 AS repeat_vip_product_net_revenue,
                        NULL                                                                 AS repeat_vip_product_order_count,
                        NULL                                                                 AS repeat_vip_product_gross_profit,
                        NULL                                                                 AS repeat_vip_aov,
                        NULL                                                                 AS repeat_vip_upt,
                        NULL                                                                 AS repeat_vip_discount_percent,
                        NULL                                                                 AS repeat_vip_product_gross_profit_percent,
                        net_unredeemed_credit_billings_budget                                AS net_unredeemed_billed_credits,
                        nonactivating_cash_net_revenue_budget                                AS nonactivating_cash_net_revenue,
                        nonactivating_cash_gross_profit_budget                               AS nonactivating_cash_gross_profit,
                        nonactivating_cash_gross_profit_budget /
                        NULLIFZERO(nonactivating_cash_net_revenue_budget)                    AS nonactivating_cash_gross_profit_percent_net,
                        billed_credit_cash_transaction_amount_budget                         AS billed_credits_transacted,
                        billed_cash_credit_redeemed_amount_budget                            AS billed_credits_redeemed,
                        billed_credit_cash_refund_chargeback_amount_budget                   AS billed_credit_cash_refund_chargeback_amount,
                        net_unredeemed_credit_billings_budget /
                        NULLIFZERO(cash_net_revenue_budget)                                  AS net_unredeemed_billed_credits_as_percent_of_net_cash_revenue,
                        COALESCE(product_order_cash_credit_refund_amount_budget, 0) +
                        COALESCE(product_order_noncash_credit_refund_amount_budget, 0)       AS refunded_as_credit,
                        refund_cash_credit_issued_amount_budget                              AS refund_cash_credit_issued_amount,
                        refund_cash_credit_redeemed_amount_budget                            AS refund_credit_redeemed,
                        COALESCE(net_unredeemed_refund_credit_budget, 0)                     AS net_unredeemed_refund_credit,
                        COALESCE(net_unredeemed_credit_billings_budget, 0) +
                        COALESCE(net_unredeemed_refund_credit_budget, 0)                     AS billed_credit_deferred_revenue,
                        bop_vips_budget                                                      AS bop_vips,
                        billing_order_transaction_count_budget / NULLIFZERO(bop_vips_budget) AS billed_credit_percent_of_bop_vips,
                        membership_credits_redeemed_count_budget /
                        NULLIFZERO(billing_order_transaction_count_budget)                   AS billed_credit_redeemed_percent,
                        membership_credits_cancelled_count_budget /
                        NULLIFZERO(billing_order_transaction_count_budget)                   AS billed_credit_cancelled_percent,
                        NULL                                                                 AS skip_rate_percent,
                        product_cost_calc_budget                                             AS landed_product_cost,
                        NULL                                                                 AS total_product_cost_net_of_returns,
                        shipping_cost_incl_reship_exch_budget                                AS outbound_shipping_costs,
                        shipping_supplies_cost_incl_reship_exch_budget                       AS shipping_supplies_costs,
                        return_shipping_costs_incl_reship_exch_budget                        AS return_shipping_costs,
                        misc_cogs_amount_budget                                              AS misc_cogs,
                        total_cogs_budget                                                    AS total_cogs,
                        COALESCE(unit_count_budget, 0)                                       AS unit_count_incl_reship_exch,
                        NULL                                                                 AS auc,
                        NULL                                                                 AS product_cost_per_order,
                        NULL                                                                 AS shipping_cost_incl_shipping_supplies_per_order
                 FROM _daily_cash_final_output dcfo
                          JOIN _variables v ON v.report_date = dcfo.date
                 WHERE segment IN ('Report Date')),
     _forecast AS (SELECT v.report_date                                                      AS date,
                          report_mapping,
                          'Forecast'                                                         AS source,
                          segment,
                          cash_gross_revenue_forecast                                        AS cash_gross_revenue,
                          product_order_and_billing_cash_refund_forecast                     AS cash_refund_amount,
                          product_order_and_billing_chargebacks_forecast                     AS chargeback_amount,
                          cash_net_revenue_forecast                                          AS cash_net_revenue,
                          (COALESCE(product_order_and_billing_cash_refund_forecast, 0) +
                           COALESCE(product_order_and_billing_chargebacks_forecast, 0)) /
                          NULLIFZERO(cash_gross_revenue_forecast)                            AS refunds_and_chargebacks_as_percent_of_gross_cash,
                          cash_gross_profit_forecast                                         AS cash_gross_profit,
                          cash_gross_profit_forecast / NULLIFZERO(cash_net_revenue_forecast) AS cash_gross_profit_percent_net,
                          COALESCE(cash_contribution_after_media_forecast, 0) +
                          COALESCE(media_spend_forecast, 0)                                  AS cash_contribution_profit,
                          media_spend_forecast                                               AS media_spend,
                          COALESCE(cash_contribution_after_media_forecast, 0)                AS cash_contribution_profit_after_media,
                          media_spend_forecast / NULLIFZERO(leads_forecast)                  AS cpl,
                          new_vips_forecast                                                  AS new_vips,
                          media_spend_forecast / NULLIFZERO(new_vips_forecast)               AS vip_cpa,
                          new_vips_m1_forecast                                               AS m1_vips,
                          new_vips_m1_forecast / NULLIFZERO(leads_forecast)                  AS m1_lead_to_vip,
                          new_vips_forecast - new_vips_m1_forecast                           AS aged_lead_conversions,
                          cancels_forecast                                                   AS cancels,
                          COALESCE(new_vips_forecast, 0) - COALESCE(cancels_forecast, 0)     AS net_change_in_vips,
                          COALESCE(bop_vips_forecast, 0) + COALESCE(net_change_in_vips, 0)   AS eop_vips,
                          net_change_in_vips / NULLIFZERO(bop_vips_forecast)                 AS growth_percent,
                          NULL                                                               AS xcpa,
                          activating_product_gross_revenue_forecast                          AS activating_product_gross_revenue,
                          activating_product_net_revenue_forecast                            AS activating_product_net_revenue,
                          activating_product_order_count_forecast                            AS activating_product_order_count,
                          activating_product_gross_revenue_forecast /
                          NULLIFZERO(activating_product_order_count_forecast)                AS activating_aov,
                          activating_unit_count_forecast /
                          NULLIFZERO(activating_product_order_count_forecast)                AS activating_upt,
                          activating_discount_percent_forecast                               AS activating_discount_rate_percent,
                          activating_product_gross_profit_forecast                           AS activating_product_gross_profit,
                          activating_product_gross_profit_forecast /
                          NULLIFZERO(activating_product_net_revenue_forecast)                AS activating_product_gross_profit_percent_net,
                          nonactivating_product_gross_revenue_forecast                       AS nonactivating_product_gross_revenue,
                          nonactivating_product_net_revenue_forecast                         AS nonactivating_product_net_revenue,
                          nonactivating_product_order_count_forecast                         AS nonactivating_product_order_count,
                          nonactivating_product_gross_revenue_forecast /
                          NULLIFZERO(nonactivating_product_order_count_forecast)             AS nonactivating_aov,
                          nonactivating_unit_count_forecast /
                          NULLIFZERO(nonactivating_product_order_count_forecast)             AS nonactivating_upt,
                          nonactivating_discount_percent_forecast                            AS nonactivating_discount_rate_percent,
                          nonactivating_product_gross_profit_forecast                        AS nonactivating_product_gross_profit,
                          nonactivating_product_gross_profit_forecast /
                          NULLIFZERO(nonactivating_product_net_revenue_forecast)             AS nonactivating_product_gross_profit_percent_net,
                          NULL                                                               AS guest_product_gross_revenue,
                          NULL                                                               AS guest_product_net_revenue,
                          NULL                                                               AS guest_product_order_count,
                          NULL                                                               AS guest_product_gross_profit,
                          NULL                                                               AS guest_aov,
                          NULL                                                               AS guest_upt,
                          NULL                                                               AS guest_discount_percent,
                          NULL                                                               AS guest_product_gross_profit_percent,
                          NULL                                                               AS repeat_vip_product_gross_revenue,
                          NULL                                                               AS repeat_vip_product_net_revenue,
                          NULL                                                               AS repeat_vip_product_order_count,
                          NULL                                                               AS repeat_vip_product_gross_profit,
                          NULL                                                               AS repeat_vip_aov,
                          NULL                                                               AS repeat_vip_upt,
                          NULL                                                               AS repeat_vip_discount_percent,
                          NULL                                                               AS repeat_vip_product_gross_profit_percent,
                          net_unredeemed_credit_billings_forecast                            AS net_unredeemed_billed_credits,
                          nonactivating_cash_net_revenue_forecast                            AS nonactivating_cash_net_revenue,
                          nonactivating_cash_gross_profit_forecast                           AS nonactivating_cash_gross_profit,
                          nonactivating_cash_gross_profit_forecast /
                          NULLIFZERO(nonactivating_cash_net_revenue_forecast)                AS nonactivating_cash_gross_profit_percent_net,
                          billed_credit_cash_transaction_amount_forecast                     AS billed_credits_transacted,
                          billed_cash_credit_redeemed_amount_forecast                        AS billed_credits_redeemed,
                          billed_credit_cash_refund_chargeback_amount_forecast               AS billed_credit_cash_refund_chargeback_amount,
                          net_unredeemed_credit_billings_forecast /
                          NULLIFZERO(cash_net_revenue_forecast)                              AS net_unredeemed_billed_credits_as_percent_of_net_cash_revenue,
                          COALESCE(product_order_cash_credit_refund_amount_forecast, 0) +
                          COALESCE(product_order_noncash_credit_refund_amount_forecast, 0)   AS refunded_as_credit,
                          refund_cash_credit_issued_amount_forecast                          AS refund_cash_credit_issued_amount,
                          refund_cash_credit_redeemed_amount_forecast                        AS refund_credit_redeemed,
                          COALESCE(net_unredeemed_refund_credit_forecast, 0)                 AS net_unredeemed_refund_credit,
                          COALESCE(net_unredeemed_credit_billings_forecast, 0) +
                          COALESCE(net_unredeemed_refund_credit_forecast, 0)                 AS billed_credit_deferred_revenue,
                          bop_vips_forecast                                                  AS bop_vips,
                          billing_order_transaction_count_forecast /
                          NULLIFZERO(bop_vips_forecast)                                      AS billed_credit_percent_of_bop_vips,
                          membership_credits_redeemed_count_forecast /
                          NULLIFZERO(billing_order_transaction_count_forecast)               AS billed_credit_redeemed_percent,
                          membership_credits_cancelled_count_forecast /
                          NULLIFZERO(billing_order_transaction_count_forecast)               AS billed_credit_cancelled_percent,
                          NULL                                                               AS skip_rate_percent,
                          product_cost_calc_forecast                                         AS landed_product_cost,
                          NULL                                                               AS total_product_cost_net_of_returns,
                          shipping_cost_incl_reship_exch_forecast                            AS outbound_shipping_costs,
                          shipping_supplies_cost_incl_reship_exch_forecast                   AS shipping_supplies_costs,
                          return_shipping_costs_incl_reship_exch_forecast                    AS return_shipping_costs,
                          misc_cogs_amount_forecast                                          AS misc_cogs,
                          total_cogs_forecast                                                AS total_cogs,
                          COALESCE(unit_count_forecast, 0)                                   AS unit_count_incl_reship_exch,
                          NULL                                                               AS auc,
                          NULL                                                               AS product_cost_per_order,
                          NULL                                                               AS shipping_cost_incl_shipping_supplies_per_order
                   FROM _daily_cash_final_output dcfo
                            JOIN (SELECT report_date, segment
                                  FROM _variables
                                  WHERE segment IN ('MTD', 'LM', 'LY')
                                  UNION
                                  SELECT last_day_of_month AS report_date, 'EOM' AS segment
                                  FROM _report_months) v ON v.report_date = dcfo.date

                   UNION ALL

                   SELECT date,
                          dcfo.report_mapping,
                          'Forecast'                                                         AS source,
                          'RR'                                                               AS segment,
                          cash_gross_revenue_forecast                                        AS cash_gross_revenue,
                          product_order_and_billing_cash_refund_forecast                     AS cash_refund_amount,
                          product_order_and_billing_chargebacks_forecast                     AS chargeback_amount,
                          cash_net_revenue_forecast                                          AS cash_net_revenue,
                          (COALESCE(product_order_and_billing_cash_refund_forecast, 0) +
                           COALESCE(product_order_and_billing_chargebacks_forecast, 0)) /
                          NULLIFZERO(cash_gross_revenue_forecast)                            AS refunds_and_chargebacks_as_percent_of_gross_cash,
                          cash_gross_profit_forecast                                         AS cash_gross_profit,
                          cash_gross_profit_forecast / NULLIFZERO(cash_net_revenue_forecast) AS cash_gross_profit_percent_net,
                          COALESCE(cash_contribution_after_media_forecast, 0) +
                          COALESCE(media_spend_forecast, 0)                                  AS cash_contribution_profit,
                          media_spend_forecast                                               AS media_spend,
                          COALESCE(cash_contribution_after_media_forecast, 0)                AS cash_contribution_profit_after_media,
                          media_spend_forecast / NULLIFZERO(leads_forecast)                  AS cpl,
                          new_vips_forecast                                                  AS new_vips,
                          media_spend_forecast / NULLIFZERO(new_vips_forecast)               AS vip_cpa,
                          new_vips_m1_forecast                                               AS m1_vips,
                          new_vips_m1_forecast / NULLIFZERO(leads_forecast)                  AS m1_lead_to_vip,
                          new_vips_forecast - new_vips_m1_forecast                           AS aged_lead_conversions,
                          cancels_forecast                                                   AS cancels,
                          COALESCE(new_vips_forecast, 0) - COALESCE(cancels_forecast, 0)     AS net_change_in_vips,
                          COALESCE(bop_vips_forecast, 0) + COALESCE(net_change_in_vips, 0)   AS eop_vips,
                          net_change_in_vips / NULLIFZERO(bop_vips_forecast)                 AS growth_percent,
                          NULL                                                               AS xcpa,
                          activating_product_gross_revenue_forecast                          AS activating_product_gross_revenue,
                          activating_product_net_revenue_forecast                            AS activating_product_net_revenue,
                          activating_product_order_count_forecast                            AS activating_product_order_count,
                          activating_product_gross_revenue_forecast /
                          NULLIFZERO(activating_product_order_count_forecast)                AS activating_aov,
                          activating_unit_count_forecast /
                          NULLIFZERO(activating_product_order_count_forecast)                AS activating_upt,
                          activating_discount_percent_forecast                               AS activating_discount_rate_percent,
                          activating_product_gross_profit_forecast                           AS activating_product_gross_profit,
                          activating_product_gross_profit_forecast /
                          NULLIFZERO(activating_product_net_revenue_forecast)                AS activating_product_gross_profit_percent_net,
                          nonactivating_product_gross_revenue_forecast                       AS nonactivating_product_gross_revenue,
                          nonactivating_product_net_revenue_forecast                         AS nonactivating_product_net_revenue,
                          nonactivating_product_order_count_forecast                         AS nonactivating_product_order_count,
                          nonactivating_product_gross_revenue_forecast /
                          NULLIFZERO(nonactivating_product_order_count_forecast)             AS nonactivating_aov,
                          nonactivating_unit_count_forecast /
                          NULLIFZERO(nonactivating_product_order_count_forecast)             AS nonactivating_upt,
                          nonactivating_discount_percent_forecast                            AS nonactivating_discount_rate_percent,
                          nonactivating_product_gross_profit_forecast                        AS nonactivating_product_gross_profit,
                          nonactivating_product_gross_profit_forecast /
                          NULLIFZERO(nonactivating_product_net_revenue_forecast)             AS nonactivating_product_gross_profit_percent_net,
                          NULL                                                               AS guest_product_gross_revenue,
                          NULL                                                               AS guest_product_net_revenue,
                          NULL                                                               AS guest_product_order_count,
                          NULL                                                               AS guest_product_gross_profit,
                          NULL                                                               AS guest_aov,
                          NULL                                                               AS guest_upt,
                          NULL                                                               AS guest_discount_percent,
                          NULL                                                               AS guest_product_gross_profit_percent,
                          NULL                                                               AS repeat_vip_product_gross_revenue,
                          NULL                                                               AS repeat_vip_product_net_revenue,
                          NULL                                                               AS repeat_vip_product_order_count,
                          NULL                                                               AS repeat_vip_product_gross_profit,
                          NULL                                                               AS repeat_vip_aov,
                          NULL                                                               AS repeat_vip_upt,
                          NULL                                                               AS repeat_vip_discount_percent,
                          NULL                                                               AS repeat_vip_product_gross_profit_percent,
                          net_unredeemed_credit_billings_forecast                            AS net_unredeemed_billed_credits,
                          nonactivating_cash_net_revenue_forecast                            AS nonactivating_cash_net_revenue,
                          nonactivating_cash_gross_profit_forecast                           AS nonactivating_cash_gross_profit,
                          nonactivating_cash_gross_profit_forecast /
                          NULLIFZERO(nonactivating_cash_net_revenue_forecast)                AS nonactivating_cash_gross_profit_percent_net,
                          billed_credit_cash_transaction_amount_forecast                     AS billed_credits_transacted,
                          billed_cash_credit_redeemed_amount_forecast                        AS billed_credits_redeemed,
                          billed_credit_cash_refund_chargeback_amount_forecast               AS billed_credit_cash_refund_chargeback_amount,
                          net_unredeemed_credit_billings_forecast /
                          NULLIFZERO(cash_net_revenue_forecast)                              AS net_unredeemed_billed_credits_as_percent_of_net_cash_revenue,
                          COALESCE(product_order_cash_credit_refund_amount_forecast, 0) +
                          COALESCE(product_order_noncash_credit_refund_amount_forecast, 0)   AS refunded_as_credit,
                          refund_cash_credit_issued_amount_forecast                          AS refund_cash_credit_issued_amount,
                          refund_cash_credit_redeemed_amount_forecast                        AS refund_credit_redeemed,
                          COALESCE(net_unredeemed_refund_credit_forecast, 0)                 AS net_unredeemed_refund_credit,
                          COALESCE(net_unredeemed_credit_billings_forecast, 0) +
                          COALESCE(net_unredeemed_refund_credit_forecast, 0)                 AS billed_credit_deferred_revenue,
                          bop_vips_forecast                                                  AS bop_vips,
                          billing_order_transaction_count_forecast /
                          NULLIFZERO(bop_vips_forecast)                                      AS billed_credit_percent_of_bop_vips,
                          membership_credits_redeemed_count_forecast /
                          NULLIFZERO(billing_order_transaction_count_forecast)               AS billed_credit_redeemed_percent,
                          membership_credits_cancelled_count_forecast /
                          NULLIFZERO(billing_order_transaction_count_forecast)               AS billed_credit_cancelled_percent,
                          NULL                                                               AS skip_rate_percent,
                          product_cost_calc_forecast                                         AS landed_product_cost,
                          NULL                                                               AS total_product_cost_net_of_returns,
                          shipping_cost_incl_reship_exch_forecast                            AS outbound_shipping_costs,
                          shipping_supplies_cost_incl_reship_exch_forecast                   AS shipping_supplies_costs,
                          return_shipping_costs_incl_reship_exch_forecast                    AS return_shipping_costs,
                          misc_cogs_amount_forecast                                          AS misc_cogs,
                          total_cogs_forecast                                                AS total_cogs,
                          COALESCE(unit_count_forecast, 0)                                   AS unit_count_incl_reship_exch,
                          NULL                                                               AS auc,
                          NULL                                                               AS product_cost_per_order,
                          NULL                                                               AS shipping_cost_incl_shipping_supplies_per_order
                   FROM _daily_cash_final_output dcfo
                            JOIN _variables v ON v.report_date = dcfo.date
                   WHERE segment IN ('Report Date')),
     _rr_bt AS (SELECT a.date
                     , a.report_mapping
                     , 'RR - BT'                                                           AS source
                     , a.segment
                     , COALESCE(a.cash_gross_revenue, 0) -
                       COALESCE(b.cash_gross_revenue, 0)                                   AS cash_gross_revenue
                     , COALESCE(a.cash_refund_amount, 0) -
                       COALESCE(b.cash_refund_amount, 0)                                   AS cash_refund_amount
                     , COALESCE(a.chargeback_amount, 0) - COALESCE(b.chargeback_amount, 0) AS chargeback_amount
                     , COALESCE(a.cash_net_revenue, 0) - COALESCE(b.cash_net_revenue, 0)   AS cash_net_revenue
                     , COALESCE(a.refunds_and_chargebacks_as_percent_of_gross_cash, 0) -
                       COALESCE(b.refunds_and_chargebacks_as_percent_of_gross_cash, 0)     AS refunds_and_chargebacks_as_percent_of_gross_cash
                     , COALESCE(a.cash_gross_profit, 0) - COALESCE(b.cash_gross_profit, 0) AS cash_gross_profit
                     , COALESCE(a.cash_gross_profit_percent_net, 0) -
                       COALESCE(b.cash_gross_profit_percent_net, 0)                        AS cash_gross_profit_percent_net
                     , COALESCE(a.cash_contribution_profit, 0) -
                       COALESCE(b.cash_contribution_profit, 0)                             AS cash_contribution_profit
                     , COALESCE(a.media_spend, 0) - COALESCE(b.media_spend, 0)             AS media_spend
                     , COALESCE(a.cash_contribution_profit_after_media, 0) -
                       COALESCE(b.cash_contribution_profit_after_media, 0)                 AS cash_contribution_profit_after_media
                     , COALESCE(a.cpl, 0) - COALESCE(b.cpl, 0)                             AS cpl
                     , COALESCE(a.vip_cpa, 0) - COALESCE(b.vip_cpa, 0)                     AS vip_cpa
                     , COALESCE(a.m1_lead_to_vip, 0) - COALESCE(b.m1_lead_to_vip, 0)       AS m1_lead_to_vip
                     , COALESCE(a.m1_vips, 0) - COALESCE(b.m1_vips, 0)                     AS m1_vips
                     , COALESCE(a.aged_lead_conversions, 0) -
                       COALESCE(b.aged_lead_conversions, 0)                                AS aged_lead_conversions
                     , COALESCE(a.new_vips, 0) - COALESCE(b.new_vips, 0)                   AS new_vips
                     , COALESCE(a.cancels, 0) - COALESCE(b.cancels, 0)                     AS cancels
                     , COALESCE(a.net_change_in_vips, 0) -
                       COALESCE(b.net_change_in_vips, 0)                                   AS net_change_in_vips
                     , COALESCE(a.eop_vips, 0) - COALESCE(b.eop_vips, 0)                   AS eop_vips
                     , COALESCE(a.growth_percent, 0) - COALESCE(b.growth_percent, 0)       AS growth_percent
                     , COALESCE(a.xcpa, 0) - COALESCE(b.xcpa, 0)                           AS xcpa
                     , COALESCE(a.activating_product_gross_revenue, 0) -
                       COALESCE(b.activating_product_gross_revenue, 0)                     AS activating_product_gross_revenue
                     , COALESCE(a.activating_product_net_revenue, 0) -
                       COALESCE(b.activating_product_net_revenue, 0)                       AS activating_product_net_revenue
                     , COALESCE(a.activating_product_order_count, 0) -
                       COALESCE(b.activating_product_order_count, 0)                       AS activating_product_order_count
                     , COALESCE(a.activating_aov, 0) - COALESCE(b.activating_aov, 0)       AS activating_aov
                     , COALESCE(a.activating_upt, 0) - COALESCE(b.activating_upt, 0)       AS activating_upt
                     , COALESCE(a.activating_discount_rate_percent, 0) -
                       COALESCE(b.activating_discount_rate_percent, 0)                     AS activating_discount_rate_percent
                     , COALESCE(a.activating_product_gross_profit, 0) -
                       COALESCE(b.activating_product_gross_profit, 0)                      AS activating_product_gross_profit
                     , COALESCE(a.activating_product_gross_profit_percent_net, 0) -
                       COALESCE(b.activating_product_gross_profit_percent_net, 0)          AS activating_product_gross_profit_percent_net
                     , COALESCE(a.net_unredeemed_billed_credits, 0) -
                       COALESCE(b.net_unredeemed_billed_credits, 0)                        AS net_unredeemed_billed_credits
                     , COALESCE(a.nonactivating_product_gross_revenue, 0) -
                       COALESCE(b.nonactivating_product_gross_revenue, 0)                  AS nonactivating_product_gross_revenue
                     , COALESCE(a.nonactivating_product_net_revenue, 0) -
                       COALESCE(b.nonactivating_product_net_revenue, 0)                    AS nonactivating_product_net_revenue
                     , COALESCE(a.nonactivating_product_order_count, 0) -
                       COALESCE(b.nonactivating_product_order_count, 0)                    AS nonactivating_product_order_count
                     , COALESCE(a.nonactivating_aov, 0) - COALESCE(b.nonactivating_aov, 0) AS nonactivating_aov
                     , COALESCE(a.nonactivating_upt, 0) - COALESCE(b.nonactivating_upt, 0) AS nonactivating_upt
                     , COALESCE(a.nonactivating_discount_rate_percent, 0) -
                       COALESCE(b.nonactivating_discount_rate_percent, 0)                  AS nonactivating_discount_rate_percent
                     , COALESCE(a.nonactivating_product_gross_profit, 0) -
                       COALESCE(b.nonactivating_product_gross_profit, 0)                   AS nonactivating_product_gross_profit
                     , COALESCE(a.nonactivating_product_gross_profit_percent_net, 0) -
                       COALESCE(b.nonactivating_product_gross_profit_percent_net, 0)       AS nonactivating_product_gross_profit_percent_net
                     , COALESCE(a.nonactivating_cash_net_revenue, 0) -
                       COALESCE(b.nonactivating_cash_net_revenue, 0)                       AS nonactivating_cash_net_revenue
                     , COALESCE(a.nonactivating_cash_gross_profit, 0) -
                       COALESCE(b.nonactivating_cash_gross_profit, 0)                      AS nonactivating_cash_gross_profit
                     , COALESCE(a.nonactivating_cash_gross_profit_percent_net, 0) -
                       COALESCE(b.nonactivating_cash_gross_profit_percent_net, 0)          AS nonactivating_cash_gross_profit_percent_net
                     , COALESCE(a.guest_product_gross_revenue, 0) -
                       COALESCE(b.guest_product_gross_revenue, 0)                          AS guest_product_gross_revenue
                     , COALESCE(a.guest_product_net_revenue, 0) -
                       COALESCE(b.guest_product_net_revenue, 0)                            AS guest_product_net_revenue
                     , COALESCE(a.guest_product_order_count, 0) -
                       COALESCE(b.guest_product_order_count, 0)                            AS guest_product_order_count
                     , COALESCE(a.guest_aov, 0) - COALESCE(b.guest_aov, 0)                 AS guest_aov
                     , COALESCE(a.guest_upt, 0) - COALESCE(b.guest_upt, 0)                 AS guest_upt
                     , COALESCE(a.guest_discount_percent, 0) -
                       COALESCE(b.guest_discount_percent, 0)                               AS guest_discount_percent
                     , COALESCE(a.guest_product_gross_profit, 0) -
                       COALESCE(b.guest_product_gross_profit, 0)                           AS guest_product_gross_profit
                     , COALESCE(a.guest_product_gross_profit_percent, 0) -
                       COALESCE(b.guest_product_gross_profit_percent, 0)                   AS guest_product_gross_profit_percent
                     , COALESCE(a.repeat_vip_product_gross_revenue, 0) -
                       COALESCE(b.repeat_vip_product_gross_revenue, 0)                     AS repeat_vip_product_gross_revenue
                     , COALESCE(a.repeat_vip_product_net_revenue, 0) -
                       COALESCE(b.repeat_vip_product_net_revenue, 0)                       AS repeat_vip_product_net_revenue
                     , COALESCE(a.repeat_vip_product_order_count, 0) -
                       COALESCE(b.repeat_vip_product_order_count, 0)                       AS repeat_vip_product_order_count
                     , COALESCE(a.repeat_vip_aov, 0) - COALESCE(b.repeat_vip_aov, 0)       AS repeat_vip_aov
                     , COALESCE(a.repeat_vip_upt, 0) - COALESCE(b.repeat_vip_upt, 0)       AS repeat_vip_upt
                     , COALESCE(a.repeat_vip_discount_percent, 0) -
                       COALESCE(b.repeat_vip_discount_percent, 0)                          AS repeat_vip_discount_percent
                     , COALESCE(a.repeat_vip_product_gross_profit, 0) -
                       COALESCE(b.repeat_vip_product_gross_profit, 0)                      AS repeat_vip_product_gross_profit
                     , COALESCE(a.repeat_vip_product_gross_profit_percent, 0) -
                       COALESCE(b.repeat_vip_product_gross_profit_percent, 0)              AS repeat_vip_product_gross_profit_percent
                     , COALESCE(a.billed_credits_transacted, 0) -
                       COALESCE(b.billed_credits_transacted, 0)                            AS billed_credits_transacted
                     , COALESCE(a.billed_credits_redeemed, 0) -
                       COALESCE(b.billed_credits_redeemed, 0)                              AS billed_credits_redeemed
                     , COALESCE(a.billed_credit_cash_refund_chargeback_amount, 0) -
                       COALESCE(b.billed_credit_cash_refund_chargeback_amount, 0)          AS billed_credit_cash_refund_chargeback_amount
                     , COALESCE(a.net_unredeemed_billed_credits_as_percent_of_net_cash_revenue, 0) -
                       COALESCE(b.net_unredeemed_billed_credits_as_percent_of_net_cash_revenue,
                                0)                                                         AS net_unredeemed_billed_credits_as_percent_of_net_cash_revenue
                     , COALESCE(a.refund_cash_credit_issued_amount, 0) -
                       COALESCE(b.refund_cash_credit_issued_amount, 0)                     AS refund_cash_credit_issued_amount
                     , COALESCE(a.refund_credit_redeemed, 0) -
                       COALESCE(b.refund_credit_redeemed, 0)                               AS refund_credit_redeemed
                     , COALESCE(a.net_unredeemed_refund_credit, 0) -
                       COALESCE(b.net_unredeemed_refund_credit, 0)                         AS net_unredeemed_refund_credit
                     , COALESCE(a.billed_credit_deferred_revenue, 0) -
                       COALESCE(b.billed_credit_deferred_revenue, 0)                       AS billed_credit_deferred_revenue
                     , COALESCE(a.bop_vips, 0) - COALESCE(b.bop_vips, 0)                   AS bop_vips
                     , COALESCE(a.billed_credit_percent_of_bop_vips, 0) -
                       COALESCE(b.billed_credit_percent_of_bop_vips, 0)                    AS billed_credit_percent_of_bop_vips
                     , COALESCE(a.billed_credit_redeemed_percent, 0) -
                       COALESCE(b.billed_credit_redeemed_percent, 0)                       AS billed_credit_redeemed_percent
                     , COALESCE(a.billed_credit_cancelled_percent, 0) -
                       COALESCE(b.billed_credit_cancelled_percent, 0)                      AS billed_credit_cancelled_percent
                     , COALESCE(a.skip_rate_percent, 0) - COALESCE(b.skip_rate_percent, 0) AS skip_rate_percent
                     , COALESCE(a.landed_product_cost, 0) -
                       COALESCE(b.landed_product_cost, 0)                                  AS landed_product_cost
                     , COALESCE(a.total_product_cost_net_of_returns, 0) -
                       COALESCE(b.total_product_cost_net_of_returns, 0)                    AS total_product_cost_net_of_returns
                     , COALESCE(a.outbound_shipping_costs, 0) -
                       COALESCE(b.outbound_shipping_costs, 0)                              AS outbound_shipping_costs
                     , COALESCE(a.shipping_supplies_costs, 0) -
                       COALESCE(b.shipping_supplies_costs, 0)                              AS shipping_supplies_costs
                     , COALESCE(a.return_shipping_costs, 0) -
                       COALESCE(b.return_shipping_costs, 0)                                AS return_shipping_costs
                     , COALESCE(a.misc_cogs, 0) - COALESCE(b.misc_cogs, 0)                 AS misc_cogs
                     , COALESCE(a.total_cogs, 0) - COALESCE(b.total_cogs, 0)               AS total_cogs
                     , COALESCE(a.unit_count_incl_reship_exch, 0) -
                       COALESCE(b.unit_count_incl_reship_exch, 0)                          AS unit_count_incl_reship_exch
                     , COALESCE(a.auc, 0) - COALESCE(b.auc, 0)                             AS auc
                     , COALESCE(a.product_cost_per_order, 0) -
                       COALESCE(b.product_cost_per_order, 0)                               AS product_cost_per_order
                     , COALESCE(a.shipping_cost_incl_shipping_supplies_per_order, 0) -
                       COALESCE(b.shipping_cost_incl_shipping_supplies_per_order, 0)       AS shipping_cost_incl_shipping_supplies_per_order
                FROM _actuals a
                         JOIN _budget b ON b.report_mapping = a.report_mapping
                    AND b.date = a.date
                    AND b.segment = a.segment),
     _rr_fc AS (SELECT a.date
                     , a.report_mapping
                     , 'RR - FC'                                                           AS source
                     , a.segment
                     , COALESCE(a.cash_gross_revenue, 0) -
                       COALESCE(f.cash_gross_revenue, 0)                                   AS cash_gross_revenue
                     , COALESCE(a.cash_refund_amount, 0) -
                       COALESCE(f.cash_refund_amount, 0)                                   AS cash_refund_amount
                     , COALESCE(a.chargeback_amount, 0) - COALESCE(f.chargeback_amount, 0) AS chargeback_amount
                     , COALESCE(a.cash_net_revenue, 0) - COALESCE(f.cash_net_revenue, 0)   AS cash_net_revenue
                     , COALESCE(a.refunds_and_chargebacks_as_percent_of_gross_cash, 0) -
                       COALESCE(f.refunds_and_chargebacks_as_percent_of_gross_cash, 0)     AS refunds_and_chargebacks_as_percent_of_gross_cash
                     , COALESCE(a.cash_gross_profit, 0) - COALESCE(f.cash_gross_profit, 0) AS cash_gross_profit
                     , COALESCE(a.cash_gross_profit_percent_net, 0) -
                       COALESCE(f.cash_gross_profit_percent_net, 0)                        AS cash_gross_profit_percent_net
                     , COALESCE(a.cash_contribution_profit, 0) -
                       COALESCE(f.cash_contribution_profit, 0)                             AS cash_contribution_profit
                     , COALESCE(a.media_spend, 0) - COALESCE(f.media_spend, 0)             AS media_spend
                     , COALESCE(a.cash_contribution_profit_after_media, 0) -
                       COALESCE(f.cash_contribution_profit_after_media, 0)                 AS cash_contribution_profit_after_media
                     , COALESCE(a.cpl, 0) - COALESCE(f.cpl, 0)                             AS cpl
                     , COALESCE(a.vip_cpa, 0) - COALESCE(f.vip_cpa, 0)                     AS vip_cpa
                     , COALESCE(a.m1_lead_to_vip, 0) - COALESCE(f.m1_lead_to_vip, 0)       AS m1_lead_to_vip
                     , COALESCE(a.m1_vips, 0) - COALESCE(f.m1_vips, 0)                     AS m1_vips
                     , COALESCE(a.aged_lead_conversions, 0) -
                       COALESCE(f.aged_lead_conversions, 0)                                AS aged_lead_conversions
                     , COALESCE(a.new_vips, 0) - COALESCE(f.new_vips, 0)                   AS new_vips
                     , COALESCE(a.cancels, 0) - COALESCE(f.cancels, 0)                     AS cancels
                     , COALESCE(a.net_change_in_vips, 0) -
                       COALESCE(f.net_change_in_vips, 0)                                   AS net_change_in_vips
                     , COALESCE(a.eop_vips, 0) - COALESCE(f.eop_vips, 0)                   AS eop_vips
                     , COALESCE(a.growth_percent, 0) - COALESCE(f.growth_percent, 0)       AS growth_percent
                     , COALESCE(a.xcpa, 0) - COALESCE(f.xcpa, 0)                           AS xcpa
                     , COALESCE(a.activating_product_gross_revenue, 0) -
                       COALESCE(f.activating_product_gross_revenue, 0)                     AS activating_product_gross_revenue
                     , COALESCE(a.activating_product_net_revenue, 0) -
                       COALESCE(f.activating_product_net_revenue, 0)                       AS activating_product_net_revenue
                     , COALESCE(a.activating_product_order_count, 0) -
                       COALESCE(f.activating_product_order_count, 0)                       AS activating_product_order_count
                     , COALESCE(a.activating_aov, 0) - COALESCE(f.activating_aov, 0)       AS activating_aov
                     , COALESCE(a.activating_upt, 0) - COALESCE(f.activating_upt, 0)       AS activating_upt
                     , COALESCE(a.activating_discount_rate_percent, 0) -
                       COALESCE(f.activating_discount_rate_percent, 0)                     AS activating_discount_rate_percent
                     , COALESCE(a.activating_product_gross_profit, 0) -
                       COALESCE(f.activating_product_gross_profit, 0)                      AS activating_product_gross_profit
                     , COALESCE(a.activating_product_gross_profit_percent_net, 0) -
                       COALESCE(f.activating_product_gross_profit_percent_net, 0)          AS activating_product_gross_profit_percent_net
                     , COALESCE(a.net_unredeemed_billed_credits, 0) -
                       COALESCE(f.net_unredeemed_billed_credits, 0)                        AS net_unredeemed_billed_credits
                     , COALESCE(a.nonactivating_product_gross_revenue, 0) -
                       COALESCE(f.nonactivating_product_gross_revenue, 0)                  AS nonactivating_product_gross_revenue
                     , COALESCE(a.nonactivating_product_net_revenue, 0) -
                       COALESCE(f.nonactivating_product_net_revenue, 0)                    AS nonactivating_product_net_revenue
                     , COALESCE(a.nonactivating_product_order_count, 0) -
                       COALESCE(f.nonactivating_product_order_count, 0)                    AS nonactivating_product_order_count
                     , COALESCE(a.nonactivating_aov, 0) - COALESCE(f.nonactivating_aov, 0) AS nonactivating_aov
                     , COALESCE(a.nonactivating_upt, 0) - COALESCE(f.nonactivating_upt, 0) AS nonactivating_upt
                     , COALESCE(a.nonactivating_discount_rate_percent, 0) -
                       COALESCE(f.nonactivating_discount_rate_percent, 0)                  AS nonactivating_discount_rate_percent
                     , COALESCE(a.nonactivating_product_gross_profit, 0) -
                       COALESCE(f.nonactivating_product_gross_profit, 0)                   AS nonactivating_product_gross_profit
                     , COALESCE(a.nonactivating_product_gross_profit_percent_net, 0) -
                       COALESCE(f.nonactivating_product_gross_profit_percent_net, 0)       AS nonactivating_product_gross_profit_percent_net
                     , COALESCE(a.nonactivating_cash_net_revenue, 0) -
                       COALESCE(f.nonactivating_cash_net_revenue, 0)                       AS nonactivating_cash_net_revenue
                     , COALESCE(a.nonactivating_cash_gross_profit, 0) -
                       COALESCE(f.nonactivating_cash_gross_profit, 0)                      AS nonactivating_cash_gross_profit
                     , COALESCE(a.nonactivating_cash_gross_profit_percent_net, 0) -
                       COALESCE(f.nonactivating_cash_gross_profit_percent_net, 0)          AS nonactivating_cash_gross_profit_percent_net
                     , COALESCE(a.guest_product_gross_revenue, 0) -
                       COALESCE(f.guest_product_gross_revenue, 0)                          AS guest_product_gross_revenue
                     , COALESCE(a.guest_product_net_revenue, 0) -
                       COALESCE(f.guest_product_net_revenue, 0)                            AS guest_product_net_revenue
                     , COALESCE(a.guest_product_order_count, 0) -
                       COALESCE(f.guest_product_order_count, 0)                            AS guest_product_order_count
                     , COALESCE(a.guest_aov, 0) - COALESCE(f.guest_aov, 0)                 AS guest_aov
                     , COALESCE(a.guest_upt, 0) - COALESCE(f.guest_upt, 0)                 AS guest_upt
                     , COALESCE(a.guest_discount_percent, 0) -
                       COALESCE(f.guest_discount_percent, 0)                               AS guest_discount_percent
                     , COALESCE(a.guest_product_gross_profit, 0) -
                       COALESCE(f.guest_product_gross_profit, 0)                           AS guest_product_gross_profit
                     , COALESCE(a.guest_product_gross_profit_percent, 0) -
                       COALESCE(f.guest_product_gross_profit_percent, 0)                   AS guest_product_gross_profit_percent
                     , COALESCE(a.repeat_vip_product_gross_revenue, 0) -
                       COALESCE(f.repeat_vip_product_gross_revenue, 0)                     AS repeat_vip_product_gross_revenue
                     , COALESCE(a.repeat_vip_product_net_revenue, 0) -
                       COALESCE(f.repeat_vip_product_net_revenue, 0)                       AS repeat_vip_product_net_revenue
                     , COALESCE(a.repeat_vip_product_order_count, 0) -
                       COALESCE(f.repeat_vip_product_order_count, 0)                       AS repeat_vip_product_order_count
                     , COALESCE(a.repeat_vip_aov, 0) - COALESCE(f.repeat_vip_aov, 0)       AS repeat_vip_aov
                     , COALESCE(a.repeat_vip_upt, 0) - COALESCE(f.repeat_vip_upt, 0)       AS repeat_vip_upt
                     , COALESCE(a.repeat_vip_discount_percent, 0) -
                       COALESCE(f.repeat_vip_discount_percent, 0)                          AS repeat_vip_discount_percent
                     , COALESCE(a.repeat_vip_product_gross_profit, 0) -
                       COALESCE(f.repeat_vip_product_gross_profit, 0)                      AS repeat_vip_product_gross_profit
                     , COALESCE(a.repeat_vip_product_gross_profit_percent, 0) -
                       COALESCE(f.repeat_vip_product_gross_profit_percent, 0)              AS repeat_vip_product_gross_profit_percent
                     , COALESCE(a.billed_credits_transacted, 0) -
                       COALESCE(f.billed_credits_transacted, 0)                            AS billed_credits_transacted
                     , COALESCE(a.billed_credits_redeemed, 0) -
                       COALESCE(f.billed_credits_redeemed, 0)                              AS billed_credits_redeemed
                     , COALESCE(a.billed_credit_cash_refund_chargeback_amount, 0) -
                       COALESCE(f.billed_credit_cash_refund_chargeback_amount, 0)          AS billed_credit_cash_refund_chargeback_amount
                     , COALESCE(a.net_unredeemed_billed_credits_as_percent_of_net_cash_revenue, 0) -
                       COALESCE(f.net_unredeemed_billed_credits_as_percent_of_net_cash_revenue,
                                0)                                                         AS net_unredeemed_billed_credits_as_percent_of_net_cash_revenue
                     , COALESCE(a.refund_cash_credit_issued_amount, 0) -
                       COALESCE(f.refund_cash_credit_issued_amount, 0)                     AS refund_cash_credit_issued_amount
                     , COALESCE(a.refund_credit_redeemed, 0) -
                       COALESCE(f.refund_credit_redeemed, 0)                               AS refund_credit_redeemed
                     , COALESCE(a.net_unredeemed_refund_credit, 0) -
                       COALESCE(f.net_unredeemed_refund_credit, 0)                         AS net_unredeemed_refund_credit
                     , COALESCE(a.billed_credit_deferred_revenue, 0) -
                       COALESCE(f.billed_credit_deferred_revenue, 0)                       AS billed_credit_deferred_revenue
                     , COALESCE(a.bop_vips, 0) - COALESCE(f.bop_vips, 0)                   AS bop_vips
                     , COALESCE(a.billed_credit_percent_of_bop_vips, 0) -
                       COALESCE(f.billed_credit_percent_of_bop_vips, 0)                    AS billed_credit_percent_of_bop_vips
                     , COALESCE(a.billed_credit_redeemed_percent, 0) -
                       COALESCE(f.billed_credit_redeemed_percent, 0)                       AS billed_credit_redeemed_percent
                     , COALESCE(a.billed_credit_cancelled_percent, 0) -
                       COALESCE(f.billed_credit_cancelled_percent, 0)                      AS billed_credit_cancelled_percent
                     , COALESCE(a.skip_rate_percent, 0) - COALESCE(f.skip_rate_percent, 0) AS skip_rate_percent
                     , COALESCE(a.landed_product_cost, 0) -
                       COALESCE(f.landed_product_cost, 0)                                  AS landed_product_cost
                     , COALESCE(a.total_product_cost_net_of_returns, 0) -
                       COALESCE(f.total_product_cost_net_of_returns, 0)                    AS total_product_cost_net_of_returns
                     , COALESCE(a.outbound_shipping_costs, 0) -
                       COALESCE(f.outbound_shipping_costs, 0)                              AS outbound_shipping_costs
                     , COALESCE(a.shipping_supplies_costs, 0) -
                       COALESCE(f.shipping_supplies_costs, 0)                              AS shipping_supplies_costs
                     , COALESCE(a.return_shipping_costs, 0) -
                       COALESCE(f.return_shipping_costs, 0)                                AS return_shipping_costs
                     , COALESCE(a.misc_cogs, 0) - COALESCE(f.misc_cogs, 0)                 AS misc_cogs
                     , COALESCE(a.total_cogs, 0) - COALESCE(f.total_cogs, 0)               AS total_cogs
                     , COALESCE(a.unit_count_incl_reship_exch, 0) -
                       COALESCE(f.unit_count_incl_reship_exch, 0)                          AS unit_count_incl_reship_exch
                     , COALESCE(a.auc, 0) - COALESCE(f.auc, 0)                             AS auc
                     , COALESCE(a.product_cost_per_order, 0) -
                       COALESCE(f.product_cost_per_order, 0)                               AS product_cost_per_order
                     , COALESCE(a.shipping_cost_incl_shipping_supplies_per_order, 0) -
                       COALESCE(f.shipping_cost_incl_shipping_supplies_per_order, 0)       AS shipping_cost_incl_shipping_supplies_per_order
                FROM _actuals a
                         JOIN _forecast f ON f.report_mapping = a.report_mapping
                    AND f.date = a.date
                    AND f.segment = a.segment),
     _base_output AS (SELECT date
                           , report_mapping
                           , source
                           , segment
                           , cash_gross_revenue
                           , cash_refund_amount
                           , chargeback_amount
                           , cash_net_revenue
                           , refunds_and_chargebacks_as_percent_of_gross_cash
                           , cash_gross_profit
                           , cash_gross_profit_percent_net
                           , cash_contribution_profit
                           , media_spend
                           , cash_contribution_profit_after_media
                           , cpl
                           , vip_cpa
                           , m1_lead_to_vip
                           , m1_vips
                           , aged_lead_conversions
                           , new_vips
                           , cancels
                           , net_change_in_vips
                           , eop_vips
                           , growth_percent
                           , xcpa
                           , activating_product_gross_revenue
                           , activating_product_net_revenue
                           , activating_product_order_count
                           , activating_aov
                           , activating_upt
                           , activating_discount_rate_percent
                           , activating_product_gross_profit
                           , activating_product_gross_profit_percent_net
                           , net_unredeemed_billed_credits
                           , nonactivating_product_gross_revenue
                           , nonactivating_product_net_revenue
                           , nonactivating_product_order_count
                           , nonactivating_aov
                           , nonactivating_upt
                           , nonactivating_discount_rate_percent
                           , nonactivating_product_gross_profit
                           , nonactivating_product_gross_profit_percent_net
                           , nonactivating_cash_net_revenue
                           , nonactivating_cash_gross_profit
                           , nonactivating_cash_gross_profit_percent_net
                           , guest_product_gross_revenue
                           , guest_product_net_revenue
                           , guest_product_order_count
                           , guest_aov
                           , guest_upt
                           , guest_discount_percent
                           , guest_product_gross_profit
                           , guest_product_gross_profit_percent
                           , repeat_vip_product_gross_revenue
                           , repeat_vip_product_net_revenue
                           , repeat_vip_product_order_count
                           , repeat_vip_aov
                           , repeat_vip_upt
                           , repeat_vip_discount_percent
                           , repeat_vip_product_gross_profit
                           , repeat_vip_product_gross_profit_percent
                           , billed_credits_transacted
                           , billed_credits_redeemed
                           , billed_credit_cash_refund_chargeback_amount
                           , net_unredeemed_billed_credits_as_percent_of_net_cash_revenue
                           , refund_cash_credit_issued_amount
                           , refund_credit_redeemed
                           , net_unredeemed_refund_credit
                           , billed_credit_deferred_revenue
                           , bop_vips
                           , billed_credit_percent_of_bop_vips
                           , billed_credit_redeemed_percent
                           , billed_credit_cancelled_percent
                           , skip_rate_percent
                           , landed_product_cost
                           , total_product_cost_net_of_returns
                           , outbound_shipping_costs
                           , shipping_supplies_costs
                           , return_shipping_costs
                           , misc_cogs
                           , total_cogs
                           , unit_count_incl_reship_exch
                           , auc
                           , product_cost_per_order
                           , shipping_cost_incl_shipping_supplies_per_order
                      FROM _actuals

                      UNION ALL

                      SELECT date
                           , report_mapping
                           , source
                           , segment
                           , cash_gross_revenue
                           , cash_refund_amount
                           , chargeback_amount
                           , cash_net_revenue
                           , refunds_and_chargebacks_as_percent_of_gross_cash
                           , cash_gross_profit
                           , cash_gross_profit_percent_net
                           , cash_contribution_profit
                           , media_spend
                           , cash_contribution_profit_after_media
                           , cpl
                           , vip_cpa
                           , m1_lead_to_vip
                           , m1_vips
                           , aged_lead_conversions
                           , new_vips
                           , cancels
                           , net_change_in_vips
                           , eop_vips
                           , growth_percent
                           , xcpa
                           , activating_product_gross_revenue
                           , activating_product_net_revenue
                           , activating_product_order_count
                           , activating_aov
                           , activating_upt
                           , activating_discount_rate_percent
                           , activating_product_gross_profit
                           , activating_product_gross_profit_percent_net
                           , net_unredeemed_billed_credits
                           , nonactivating_product_gross_revenue
                           , nonactivating_product_net_revenue
                           , nonactivating_product_order_count
                           , nonactivating_aov
                           , nonactivating_upt
                           , nonactivating_discount_rate_percent
                           , nonactivating_product_gross_profit
                           , nonactivating_product_gross_profit_percent_net
                           , nonactivating_cash_net_revenue
                           , nonactivating_cash_gross_profit
                           , nonactivating_cash_gross_profit_percent_net
                           , guest_product_gross_revenue
                           , guest_product_net_revenue
                           , guest_product_order_count
                           , guest_aov
                           , guest_upt
                           , guest_discount_percent
                           , guest_product_gross_profit
                           , guest_product_gross_profit_percent
                           , repeat_vip_product_gross_revenue
                           , repeat_vip_product_net_revenue
                           , repeat_vip_product_order_count
                           , repeat_vip_aov
                           , repeat_vip_upt
                           , repeat_vip_discount_percent
                           , repeat_vip_product_gross_profit
                           , repeat_vip_product_gross_profit_percent
                           , billed_credits_transacted
                           , billed_credits_redeemed
                           , billed_credit_cash_refund_chargeback_amount
                           , net_unredeemed_billed_credits_as_percent_of_net_cash_revenue
                           , refund_cash_credit_issued_amount
                           , refund_credit_redeemed
                           , net_unredeemed_refund_credit
                           , billed_credit_deferred_revenue
                           , bop_vips
                           , billed_credit_percent_of_bop_vips
                           , billed_credit_redeemed_percent
                           , billed_credit_cancelled_percent
                           , skip_rate_percent
                           , landed_product_cost
                           , total_product_cost_net_of_returns
                           , outbound_shipping_costs
                           , shipping_supplies_costs
                           , return_shipping_costs
                           , misc_cogs
                           , total_cogs
                           , unit_count_incl_reship_exch
                           , auc
                           , product_cost_per_order
                           , shipping_cost_incl_shipping_supplies_per_order
                      FROM _budget

                      UNION ALL

                      SELECT date
                           , report_mapping
                           , source
                           , segment
                           , cash_gross_revenue
                           , cash_refund_amount
                           , chargeback_amount
                           , cash_net_revenue
                           , refunds_and_chargebacks_as_percent_of_gross_cash
                           , cash_gross_profit
                           , cash_gross_profit_percent_net
                           , cash_contribution_profit
                           , media_spend
                           , cash_contribution_profit_after_media
                           , cpl
                           , vip_cpa
                           , m1_lead_to_vip
                           , m1_vips
                           , aged_lead_conversions
                           , new_vips
                           , cancels
                           , net_change_in_vips
                           , eop_vips
                           , growth_percent
                           , xcpa
                           , activating_product_gross_revenue
                           , activating_product_net_revenue
                           , activating_product_order_count
                           , activating_aov
                           , activating_upt
                           , activating_discount_rate_percent
                           , activating_product_gross_profit
                           , activating_product_gross_profit_percent_net
                           , net_unredeemed_billed_credits
                           , nonactivating_product_gross_revenue
                           , nonactivating_product_net_revenue
                           , nonactivating_product_order_count
                           , nonactivating_aov
                           , nonactivating_upt
                           , nonactivating_discount_rate_percent
                           , nonactivating_product_gross_profit
                           , nonactivating_product_gross_profit_percent_net
                           , nonactivating_cash_net_revenue
                           , nonactivating_cash_gross_profit
                           , nonactivating_cash_gross_profit_percent_net
                           , guest_product_gross_revenue
                           , guest_product_net_revenue
                           , guest_product_order_count
                           , guest_aov
                           , guest_upt
                           , guest_discount_percent
                           , guest_product_gross_profit
                           , guest_product_gross_profit_percent
                           , repeat_vip_product_gross_revenue
                           , repeat_vip_product_net_revenue
                           , repeat_vip_product_order_count
                           , repeat_vip_aov
                           , repeat_vip_upt
                           , repeat_vip_discount_percent
                           , repeat_vip_product_gross_profit
                           , repeat_vip_product_gross_profit_percent
                           , billed_credits_transacted
                           , billed_credits_redeemed
                           , billed_credit_cash_refund_chargeback_amount
                           , net_unredeemed_billed_credits_as_percent_of_net_cash_revenue
                           , refund_cash_credit_issued_amount
                           , refund_credit_redeemed
                           , net_unredeemed_refund_credit
                           , billed_credit_deferred_revenue
                           , bop_vips
                           , billed_credit_percent_of_bop_vips
                           , billed_credit_redeemed_percent
                           , billed_credit_cancelled_percent
                           , skip_rate_percent
                           , landed_product_cost
                           , total_product_cost_net_of_returns
                           , outbound_shipping_costs
                           , shipping_supplies_costs
                           , return_shipping_costs
                           , misc_cogs
                           , total_cogs
                           , unit_count_incl_reship_exch
                           , auc
                           , product_cost_per_order
                           , shipping_cost_incl_shipping_supplies_per_order
                      FROM _forecast

                      UNION ALL

                      SELECT date
                           , report_mapping
                           , source
                           , segment
                           , cash_gross_revenue
                           , cash_refund_amount
                           , chargeback_amount
                           , cash_net_revenue
                           , refunds_and_chargebacks_as_percent_of_gross_cash
                           , cash_gross_profit
                           , cash_gross_profit_percent_net
                           , cash_contribution_profit
                           , media_spend
                           , cash_contribution_profit_after_media
                           , cpl
                           , vip_cpa
                           , m1_lead_to_vip
                           , m1_vips
                           , aged_lead_conversions
                           , new_vips
                           , cancels
                           , net_change_in_vips
                           , eop_vips
                           , growth_percent
                           , xcpa
                           , activating_product_gross_revenue
                           , activating_product_net_revenue
                           , activating_product_order_count
                           , activating_aov
                           , activating_upt
                           , activating_discount_rate_percent
                           , activating_product_gross_profit
                           , activating_product_gross_profit_percent_net
                           , net_unredeemed_billed_credits
                           , nonactivating_product_gross_revenue
                           , nonactivating_product_net_revenue
                           , nonactivating_product_order_count
                           , nonactivating_aov
                           , nonactivating_upt
                           , nonactivating_discount_rate_percent
                           , nonactivating_product_gross_profit
                           , nonactivating_product_gross_profit_percent_net
                           , nonactivating_cash_net_revenue
                           , nonactivating_cash_gross_profit
                           , nonactivating_cash_gross_profit_percent_net
                           , guest_product_gross_revenue
                           , guest_product_net_revenue
                           , guest_product_order_count
                           , guest_aov
                           , guest_upt
                           , guest_discount_percent
                           , guest_product_gross_profit
                           , guest_product_gross_profit_percent
                           , repeat_vip_product_gross_revenue
                           , repeat_vip_product_net_revenue
                           , repeat_vip_product_order_count
                           , repeat_vip_aov
                           , repeat_vip_upt
                           , repeat_vip_discount_percent
                           , repeat_vip_product_gross_profit
                           , repeat_vip_product_gross_profit_percent
                           , billed_credits_transacted
                           , billed_credits_redeemed
                           , billed_credit_cash_refund_chargeback_amount
                           , net_unredeemed_billed_credits_as_percent_of_net_cash_revenue
                           , refund_cash_credit_issued_amount
                           , refund_credit_redeemed
                           , net_unredeemed_refund_credit
                           , billed_credit_deferred_revenue
                           , bop_vips
                           , billed_credit_percent_of_bop_vips
                           , billed_credit_redeemed_percent
                           , billed_credit_cancelled_percent
                           , skip_rate_percent
                           , landed_product_cost
                           , total_product_cost_net_of_returns
                           , outbound_shipping_costs
                           , shipping_supplies_costs
                           , return_shipping_costs
                           , misc_cogs
                           , total_cogs
                           , unit_count_incl_reship_exch
                           , auc
                           , product_cost_per_order
                           , shipping_cost_incl_shipping_supplies_per_order
                      FROM _rr_bt

                      UNION ALL

                      SELECT date
                           , report_mapping
                           , source
                           , segment
                           , cash_gross_revenue
                           , cash_refund_amount
                           , chargeback_amount
                           , cash_net_revenue
                           , refunds_and_chargebacks_as_percent_of_gross_cash
                           , cash_gross_profit
                           , cash_gross_profit_percent_net
                           , cash_contribution_profit
                           , media_spend
                           , cash_contribution_profit_after_media
                           , cpl
                           , vip_cpa
                           , m1_lead_to_vip
                           , m1_vips
                           , aged_lead_conversions
                           , new_vips
                           , cancels
                           , net_change_in_vips
                           , eop_vips
                           , growth_percent
                           , xcpa
                           , activating_product_gross_revenue
                           , activating_product_net_revenue
                           , activating_product_order_count
                           , activating_aov
                           , activating_upt
                           , activating_discount_rate_percent
                           , activating_product_gross_profit
                           , activating_product_gross_profit_percent_net
                           , net_unredeemed_billed_credits
                           , nonactivating_product_gross_revenue
                           , nonactivating_product_net_revenue
                           , nonactivating_product_order_count
                           , nonactivating_aov
                           , nonactivating_upt
                           , nonactivating_discount_rate_percent
                           , nonactivating_product_gross_profit
                           , nonactivating_product_gross_profit_percent_net
                           , nonactivating_cash_net_revenue
                           , nonactivating_cash_gross_profit
                           , nonactivating_cash_gross_profit_percent_net
                           , guest_product_gross_revenue
                           , guest_product_net_revenue
                           , guest_product_order_count
                           , guest_aov
                           , guest_upt
                           , guest_discount_percent
                           , guest_product_gross_profit
                           , guest_product_gross_profit_percent
                           , repeat_vip_product_gross_revenue
                           , repeat_vip_product_net_revenue
                           , repeat_vip_product_order_count
                           , repeat_vip_aov
                           , repeat_vip_upt
                           , repeat_vip_discount_percent
                           , repeat_vip_product_gross_profit
                           , repeat_vip_product_gross_profit_percent
                           , billed_credits_transacted
                           , billed_credits_redeemed
                           , billed_credit_cash_refund_chargeback_amount
                           , net_unredeemed_billed_credits_as_percent_of_net_cash_revenue
                           , refund_cash_credit_issued_amount
                           , refund_credit_redeemed
                           , net_unredeemed_refund_credit
                           , billed_credit_deferred_revenue
                           , bop_vips
                           , billed_credit_percent_of_bop_vips
                           , billed_credit_redeemed_percent
                           , billed_credit_cancelled_percent
                           , skip_rate_percent
                           , landed_product_cost
                           , total_product_cost_net_of_returns
                           , outbound_shipping_costs
                           , shipping_supplies_costs
                           , return_shipping_costs
                           , misc_cogs
                           , total_cogs
                           , unit_count_incl_reship_exch
                           , auc
                           , product_cost_per_order
                           , shipping_cost_incl_shipping_supplies_per_order
                      FROM _rr_fc),
     _comparison_mapping AS (SELECT 'RR' AS lhs, 'LM' AS rhs
                             UNION
                             SELECT 'RR' AS lhs, 'LY' AS rhs),
     _comparison_mapping2 AS (SELECT 'Actuals' AS map, 'Actuals' AS source
                              UNION
                              SELECT 'Actuals' AS map, 'RR - BT' AS source
                              UNION
                              SELECT 'Actuals' AS map, 'RR - FC' AS source),
     _comparison_rate AS (SELECT lhs.date
                               , lhs.report_mapping
                               , cm2.source                 AS source
                               , lhs.segment || rhs.segment AS segment
                               , lhs.cash_gross_revenue / NULLIFZERO(rhs.cash_gross_revenue)
                                                            AS cash_gross_revenue
                               , lhs.cash_refund_amount / NULLIFZERO(rhs.cash_refund_amount)
                                                            AS cash_refund_amount
                               , lhs.chargeback_amount / NULLIFZERO(rhs.chargeback_amount)
                                                            AS chargeback_amount
                               , lhs.cash_net_revenue / NULLIFZERO(rhs.cash_net_revenue)
                                                            AS cash_net_revenue
                               , lhs.refunds_and_chargebacks_as_percent_of_gross_cash /
                                 NULLIFZERO(rhs.refunds_and_chargebacks_as_percent_of_gross_cash)
                                                            AS refunds_and_chargebacks_as_percent_of_gross_cash
                               , lhs.cash_gross_profit / NULLIFZERO(rhs.cash_gross_profit)
                                                            AS cash_gross_profit
                               , lhs.cash_gross_profit_percent_net /
                                 NULLIFZERO(rhs.cash_gross_profit_percent_net)
                                                            AS cash_gross_profit_percent_net
                               , lhs.cash_contribution_profit /
                                 NULLIFZERO(rhs.cash_contribution_profit)
                                                            AS cash_contribution_profit
                               , lhs.media_spend / NULLIFZERO(rhs.media_spend)
                                                            AS media_spend
                               , lhs.cash_contribution_profit_after_media /
                                 NULLIFZERO(rhs.cash_contribution_profit_after_media)
                                                            AS cash_contribution_profit_after_media
                               , lhs.cpl / NULLIFZERO(rhs.cpl)
                                                            AS cpl
                               , lhs.vip_cpa / NULLIFZERO(rhs.vip_cpa)
                                                            AS vip_cpa
                               , lhs.m1_lead_to_vip / NULLIFZERO(rhs.m1_lead_to_vip)
                                                            AS m1_lead_to_vip
                               , lhs.m1_vips / NULLIFZERO(rhs.m1_vips)
                                                            AS m1_vips
                               , lhs.aged_lead_conversions / NULLIFZERO(rhs.aged_lead_conversions)
                                                            AS aged_lead_conversions
                               , lhs.new_vips / NULLIFZERO(rhs.new_vips)
                                                            AS new_vips
                               , lhs.cancels / NULLIFZERO(rhs.cancels)
                                                            AS cancels
                               , lhs.net_change_in_vips / NULLIFZERO(rhs.net_change_in_vips)
                                                            AS net_change_in_vips
                               , lhs.eop_vips / NULLIFZERO(rhs.eop_vips)
                                                            AS eop_vips
                               , lhs.growth_percent / NULLIFZERO(rhs.growth_percent)
                                                            AS growth_percent
                               , lhs.xcpa / NULLIFZERO(rhs.xcpa)
                                                            AS xcpa
                               , lhs.activating_product_gross_revenue /
                                 NULLIFZERO(rhs.activating_product_gross_revenue)
                                                            AS activating_product_gross_revenue
                               , lhs.activating_product_net_revenue /
                                 NULLIFZERO(rhs.activating_product_net_revenue)
                                                            AS activating_product_net_revenue
                               , lhs.activating_product_order_count /
                                 NULLIFZERO(rhs.activating_product_order_count)
                                                            AS activating_product_order_count
                               , lhs.activating_aov / NULLIFZERO(rhs.activating_aov)
                                                            AS activating_aov
                               , lhs.activating_upt / NULLIFZERO(rhs.activating_upt)
                                                            AS activating_upt
                               , lhs.activating_discount_rate_percent /
                                 NULLIFZERO(rhs.activating_discount_rate_percent)
                                                            AS activating_discount_rate_percent
                               , lhs.activating_product_gross_profit /
                                 NULLIFZERO(rhs.activating_product_gross_profit)
                                                            AS activating_product_gross_profit
                               , lhs.activating_product_gross_profit_percent_net /
                                 NULLIFZERO(rhs.activating_product_gross_profit_percent_net)
                                                            AS activating_product_gross_profit_percent_net
                               , lhs.net_unredeemed_billed_credits /
                                 NULLIFZERO(rhs.net_unredeemed_billed_credits)
                                                            AS net_unredeemed_billed_credits
                               , lhs.nonactivating_product_gross_revenue /
                                 NULLIFZERO(rhs.nonactivating_product_gross_revenue)
                                                            AS nonactivating_product_gross_revenue
                               , lhs.nonactivating_product_net_revenue /
                                 NULLIFZERO(rhs.nonactivating_product_net_revenue)
                                                            AS nonactivating_product_net_revenue
                               , lhs.nonactivating_product_order_count /
                                 NULLIFZERO(rhs.nonactivating_product_order_count)
                                                            AS nonactivating_product_order_count
                               , lhs.nonactivating_aov /
                                 NULLIFZERO(rhs.nonactivating_aov)
                                                            AS nonactivating_aov
                               , lhs.nonactivating_upt /
                                 NULLIFZERO(rhs.nonactivating_upt)
                                                            AS nonactivating_upt
                               , lhs.nonactivating_discount_rate_percent /
                                 NULLIFZERO(rhs.nonactivating_discount_rate_percent)
                                                            AS nonactivating_discount_rate_percent
                               , lhs.nonactivating_product_gross_profit /
                                 NULLIFZERO(rhs.nonactivating_product_gross_profit)
                                                            AS nonactivating_product_gross_profit
                               , lhs.nonactivating_product_gross_profit_percent_net /
                                 NULLIFZERO(rhs.nonactivating_product_gross_profit_percent_net)
                                                            AS nonactivating_product_gross_profit_percent_net
                               , lhs.nonactivating_cash_net_revenue /
                                 NULLIFZERO(rhs.nonactivating_cash_net_revenue)
                                                            AS nonactivating_cash_net_revenue
                               , lhs.nonactivating_cash_gross_profit /
                                 NULLIFZERO(rhs.nonactivating_cash_gross_profit)
                                                            AS nonactivating_cash_gross_profit
                               , lhs.nonactivating_cash_gross_profit_percent_net /
                                 NULLIFZERO(rhs.nonactivating_cash_gross_profit_percent_net)
                                                            AS nonactivating_cash_gross_profit_percent_net
                               , lhs.guest_product_gross_revenue /
                                 NULLIFZERO(rhs.guest_product_gross_revenue)
                                                            AS guest_product_gross_revenue
                               , lhs.guest_product_net_revenue /
                                 NULLIFZERO(rhs.guest_product_net_revenue)
                                                            AS guest_product_net_revenue
                               , lhs.guest_product_order_count /
                                 NULLIFZERO(rhs.guest_product_order_count)
                                                            AS guest_product_order_count
                               , lhs.guest_aov / NULLIFZERO(rhs.guest_aov)
                                                            AS guest_aov
                               , lhs.guest_upt / NULLIFZERO(rhs.guest_upt)
                                                            AS guest_upt
                               , lhs.guest_discount_percent / NULLIFZERO(rhs.guest_discount_percent)
                                                            AS guest_discount_percent
                               , lhs.guest_product_gross_profit /
                                 NULLIFZERO(rhs.guest_product_gross_profit)
                                                            AS guest_product_gross_profit
                               , lhs.guest_product_gross_profit_percent /
                                 NULLIFZERO(rhs.guest_product_gross_profit_percent)
                                                            AS guest_product_gross_profit_percent
                               , lhs.repeat_vip_product_gross_revenue /
                                 NULLIFZERO(rhs.repeat_vip_product_gross_revenue)
                                                            AS repeat_vip_product_gross_revenue
                               , lhs.repeat_vip_product_net_revenue /
                                 NULLIFZERO(rhs.repeat_vip_product_net_revenue)
                                                            AS repeat_vip_product_net_revenue
                               , lhs.repeat_vip_product_order_count /
                                 NULLIFZERO(rhs.repeat_vip_product_order_count)
                                                            AS repeat_vip_product_order_count
                               , lhs.repeat_vip_aov / NULLIFZERO(rhs.repeat_vip_aov)
                                                            AS repeat_vip_aov
                               , lhs.repeat_vip_upt / NULLIFZERO(rhs.repeat_vip_upt)
                                                            AS repeat_vip_upt
                               , lhs.repeat_vip_discount_percent /
                                 NULLIFZERO(rhs.repeat_vip_discount_percent)
                                                            AS repeat_vip_discount_percent
                               , lhs.repeat_vip_product_gross_profit /
                                 NULLIFZERO(rhs.repeat_vip_product_gross_profit)
                                                            AS repeat_vip_product_gross_profit
                               , lhs.repeat_vip_product_gross_profit_percent /
                                 NULLIFZERO(rhs.repeat_vip_product_gross_profit_percent)
                                                            AS repeat_vip_product_gross_profit_percent
                               , lhs.billed_credits_transacted /
                                 NULLIFZERO(rhs.billed_credits_transacted)
                                                            AS billed_credits_transacted
                               , lhs.billed_credits_redeemed / NULLIFZERO(rhs.billed_credits_redeemed)
                                                            AS billed_credits_redeemed
                               , lhs.billed_credit_cash_refund_chargeback_amount /
                                 NULLIFZERO(rhs.billed_credit_cash_refund_chargeback_amount)
                                                            AS billed_credit_cash_refund_chargeback_amount
                               , lhs.net_unredeemed_billed_credits_as_percent_of_net_cash_revenue /
                                 NULLIFZERO(rhs.net_unredeemed_billed_credits_as_percent_of_net_cash_revenue)
                                                            AS net_unredeemed_billed_credits_as_percent_of_net_cash_revenue
                               , lhs.refund_cash_credit_issued_amount /
                                 NULLIFZERO(rhs.refund_cash_credit_issued_amount)
                                                            AS refund_cash_credit_issued_amount
                               , lhs.refund_credit_redeemed / NULLIFZERO(rhs.refund_credit_redeemed)
                                                            AS refund_credit_redeemed
                               , lhs.net_unredeemed_refund_credit /
                                 NULLIFZERO(rhs.net_unredeemed_refund_credit)
                                                            AS net_unredeemed_refund_credit
                               , lhs.billed_credit_deferred_revenue /
                                 NULLIFZERO(rhs.billed_credit_deferred_revenue)
                                                            AS billed_credit_deferred_revenue
                               , lhs.bop_vips / NULLIFZERO(rhs.bop_vips)
                                                            AS bop_vips
                               , lhs.billed_credit_percent_of_bop_vips /
                                 NULLIFZERO(rhs.billed_credit_percent_of_bop_vips)
                                                            AS billed_credit_percent_of_bop_vips
                               , lhs.billed_credit_redeemed_percent /
                                 NULLIFZERO(rhs.billed_credit_redeemed_percent)
                                                            AS billed_credit_redeemed_percent
                               , lhs.billed_credit_cancelled_percent /
                                 NULLIFZERO(rhs.billed_credit_cancelled_percent)
                                                            AS billed_credit_cancelled_percent
                               , lhs.skip_rate_percent / NULLIFZERO(rhs.skip_rate_percent)
                                                            AS skip_rate_percent
                               , lhs.landed_product_cost / NULLIFZERO(rhs.landed_product_cost)
                                                            AS landed_product_cost
                               , lhs.total_product_cost_net_of_returns /
                                 NULLIFZERO(rhs.total_product_cost_net_of_returns)
                                                            AS total_product_cost_net_of_returns
                               , lhs.outbound_shipping_costs / NULLIFZERO(rhs.outbound_shipping_costs)
                                                            AS outbound_shipping_costs
                               , lhs.shipping_supplies_costs / NULLIFZERO(rhs.shipping_supplies_costs)
                                                            AS shipping_supplies_costs
                               , lhs.return_shipping_costs / NULLIFZERO(rhs.return_shipping_costs)
                                                            AS return_shipping_costs
                               , lhs.misc_cogs / NULLIFZERO(rhs.misc_cogs)
                                                            AS misc_cogs
                               , lhs.total_cogs / NULLIFZERO(rhs.total_cogs)
                                                            AS total_cogs
                               , lhs.unit_count_incl_reship_exch /
                                 NULLIFZERO(rhs.unit_count_incl_reship_exch)
                                                            AS unit_count_incl_reship_exch
                               , lhs.auc / NULLIFZERO(rhs.auc)
                                                            AS auc
                               , lhs.product_cost_per_order / NULLIFZERO(rhs.product_cost_per_order)
                                                            AS product_cost_per_order
                               , lhs.shipping_cost_incl_shipping_supplies_per_order /
                                 NULLIFZERO(rhs.shipping_cost_incl_shipping_supplies_per_order)
                                                            AS shipping_cost_incl_shipping_supplies_per_order
                          FROM _base_output lhs
                                   JOIN _comparison_mapping cm ON cm.lhs = lhs.segment
                                   JOIN _base_output rhs ON rhs.segment = cm.rhs
                              AND lhs.report_mapping = rhs.report_mapping
                              AND lhs.source = rhs.source
                                   JOIN _comparison_mapping2 cm2 ON lhs.source = cm2.map
                          WHERE lhs.source NOT IN ('Budget', 'Forecast'))

SELECT *,
       (SELECT snapshot_datetime_data FROM _daily_cash_snapshot_datetime) AS snapshot_datetime_data,
       (SELECT snapshot_datetime_budget
        FROM _daily_cash_snapshot_datetime)                               AS snapshot_datetime_budget,
       (SELECT snapshot_datetime_forecast
        FROM _daily_cash_snapshot_datetime)                               AS snapshot_datetime_forecast
FROM _base_output
UNION ALL
SELECT *,
       (SELECT snapshot_datetime_data FROM _daily_cash_snapshot_datetime) AS snapshot_datetime_data,
       (SELECT snapshot_datetime_budget
        FROM _daily_cash_snapshot_datetime)                               AS snapshot_datetime_budget,
       (SELECT snapshot_datetime_forecast
        FROM _daily_cash_snapshot_datetime)                               AS snapshot_datetime_forecast
FROM _comparison_rate;
