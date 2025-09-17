--SET end_date = DATEADD(DAY, -1, CURRENT_DATE());
SET end_date = DATEADD(DAY, -1, '2025-07-01');
SET start_date = DATE_TRUNC(MONTH, $end_date);


----Actual
--ACQUISITION
CREATE OR REPLACE TEMPORARY TABLE _acquisition_actual AS
SELECT UPPER(CASE
                 WHEN a.store_name = 'ShoeDazzle NA' THEN 'SHOEDAZZLE'
                 WHEN a.store_name = 'FabKids NA' THEN 'FABKIDS'
                 ELSE a.store_name END)                        AS business_unit
     , a.date
     , a.media_spend
     , a.cpl
     , a.vips_from_leads_m1_per                                AS m1_lead_to_vip
     , a.total_vips_on_date_cac                                AS total_vip_cpa
     , a.primary_leads
     , a.vips_from_leads_m1
     , a.total_vips_on_date - a.vips_from_leads_m1             AS aged_vip
     , a.total_vips_on_date
     , a.cancels_on_date                                       AS total_cancels
     , a.bop_vips
     , a.bop_vips + a.total_vips_on_date - a.cancels_on_date   AS eop_vips
     , a.cancels_on_date / (a.bop_vips + a.total_vips_on_date) AS attrition_rate
FROM reporting_media_prod.attribution.total_cac_output a
WHERE a.store_name IN ('JustFab NA', 'JustFab EU', 'ShoeDazzle NA', 'FabKids NA')
  AND a.source IN ('DailyTY', 'DailyLM')
  AND a.currency = 'USD'
  AND a.report_month = $start_date
  AND a.date >= DATEADD(MONTH, -1, $start_date)
  AND a.date <= $end_date;


--daily cash
CREATE OR REPLACE TEMPORARY TABLE _daily_cash_actual AS
SELECT (CASE
            WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
            WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
            WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
            WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
    END)                                                                      AS business_unit
     , a.date
     --activating
     , a.activating_product_order_count                                       AS activating_product_orders
     , a.activating_unit_count                                                AS activating_product_units
     , a.activating_unit_count * 1.0 / a.activating_product_order_count       AS activating_upt
     , a.activating_product_gross_revenue_excl_shipping                       AS activating_product_gross_revenue_excl_shipping
     , (a.activating_product_gross_revenue_excl_shipping) * 1.0 /
       a.activating_unit_count                                                AS activating_aur
     , a.activating_shipping_revenue * 1.0 / a.activating_product_order_count AS activating_shipping_revenue_per_order
     , (a.activating_product_order_landed_product_cost_amount +
        a.activating_product_order_exchange_product_cost_amount + a.activating_product_order_reship_product_cost_amount)
    / (a.activating_unit_count + a.activating_product_order_reship_unit_count +
       a.activating_product_order_exchange_unit_count)                        AS activating_auc
     , a.activating_product_gross_revenue * 1.0 /
       a.activating_product_order_count                                       AS activating_aov_incl_shipping
     , a.activating_product_gross_revenue                                     AS activating_gaap_revenue
     , a.activating_product_net_revenue                                       AS activating_gaap_net_revenue
     , a.activating_product_order_landed_product_cost_amount                  AS activaitng_total_cogs
     , activating_product_gross_profit                                        AS activating_product_gross_margin --activating_product_gross_margin/activating_gaap_net_revenue
     --repeat
     , a.nonactivating_product_order_count                                    AS repeat_product_orders
     , a.nonactivating_unit_count                                             AS repeat_product_units
     , a.nonactivating_unit_count * 1.0 / a.nonactivating_product_order_count AS repeat_upt
     , a.nonactivating_product_gross_revenue_excl_shipping                    AS nonactivating_product_gross_revenue_excl_shipping
     , (a.nonactivating_product_gross_revenue_excl_shipping) * 1.0 /
       a.nonactivating_unit_count                                             AS repeat_aur
     , a.nonactivating_shipping_revenue * 1.0 /
       a.nonactivating_product_order_count                                    AS repeat_shipping_revenue_per_order
     , (a.nonactivating_product_order_landed_product_cost_amount +
        a.nonactivating_product_order_exchange_product_cost_amount +
        a.nonactivating_product_order_reship_product_cost_amount)
    / (a.nonactivating_unit_count + a.nonactivating_product_order_reship_unit_count +
       a.nonactivating_product_order_exchange_unit_count)                     AS repeat_auc
     , a.nonactivating_product_gross_revenue * 1.0 /
       a.nonactivating_product_order_count                                    AS repeat_aov_incl_shipping
     , a.nonactivating_product_gross_revenue                                  AS repeat_gaap_revenue
     , a.nonactivating_product_net_revenue                                    AS repeat_gaap_net_revenue
     , a.nonactivating_product_order_landed_product_cost_amount               AS repeat_total_cogs
     , nonactivating_product_gross_profit                                     AS repeat_product_gross_margin     --repeat_product_gross_margin/repeat_gaap_net_revenue
     --membership credit
     , a.billed_credit_cash_transaction_amount                                AS credit_billings
     , a.billed_cash_credit_redeemed_amount                                   AS credit_redemptions
     , a.billed_credit_cash_refund_chargeback_amount                          AS credit_cancels
     , a.billed_credit_cash_transaction_amount - a.billed_cash_credit_redeemed_amount -
       a.billed_credit_cash_refund_chargeback_amount                          AS net_unredeemed_credit
     , a.billed_credit_cash_transaction_count                                 AS billing_count
     --daily report topline
     , a.billed_cash_credit_redeemed_amount * fc.refund_credit_redeemed_rate  AS refund_credit_redeemed
     , a.cash_net_revenue                                                     AS total_net_cash_revenue
     , a.activating_product_order_landed_product_cost_amount +
       a.nonactivating_product_order_landed_product_cost_amount               AS total_cogs
     , a.cash_gross_profit                                                    AS cash_gm
     , cash_gm * 1.0 / total_net_cash_revenue                                 AS cash_gm_percent
FROM edw_prod.reporting.daily_cash_final_output a
         LEFT JOIN
     (
         SELECT DISTINCT (CASE
                              WHEN mbfa.bu = 'JFNA' THEN 'JUSTFAB NA'
                              WHEN mbfa.bu = 'SDNA' THEN 'SHOEDAZZLE'
                              WHEN mbfa.bu = 'FKNA' THEN 'FABKIDS'
                              WHEN mbfa.bu = 'JFEU' THEN 'JUSTFAB EU'
             END)                                                                                            AS business_unit
                       , mbfa.month
                       , FIRST_VALUE(mbfa.membership_credits_redeemed)
                                     OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC) AS credit_redeemed_fc
                       , FIRST_VALUE(ABS(mbfa.refund_credit_redeemed))
                                     OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC) AS refund_credit_redeemed_fc
                       , (case
                            when credit_redeemed_fc = 0 then 0
                            else refund_credit_redeemed_fc * 1.0 / credit_redeemed_fc end)                             AS refund_credit_redeemed_rate
         FROM lake.fpa.monthly_budget_forecast_actual mbfa
         WHERE mbfa.bu IN ('JFNA', 'JFEU', 'SDNA', 'FKNA')
           AND mbfa.version = 'Forecast'
     ) AS fc ON fc.business_unit = (CASE
                                        WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
                                        WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
                                        WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
                                        WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
         END)
         AND fc.month = DATE_TRUNC(MONTH, a.date)
WHERE a.report_mapping IN ('JF-TREV-NA', 'JF-TREV-EU', 'SD-TREV-NA', 'FK-TREV-NA')
  AND a.currency_type = 'USD'
  AND a.date_object = 'placed'
  AND a.date >= DATEADD(MONTH, -1, $start_date)
  AND a.date <= $end_date;

----Target
--ACQUISITION
CREATE OR REPLACE TEMPORARY TABLE _acquisition_target AS
SELECT fc.business_unit
     , fc.month
     , dd.full_date                                                   AS date
     , fc.media_spend * 1.0 / DAYOFMONTH(LAST_DAY(month_date))        AS media_spend
     , fc.cpl
     , fc.m1_lead_to_vip
     , fc.vip_cpa
     , fc.leads * 1.0 / DAYOFMONTH(LAST_DAY(month_date))              AS leads
     , fc.m1_vips * 1.0 / DAYOFMONTH(LAST_DAY(month_date))            AS m1_vips
     , fc.aged_vip * 1.0 / DAYOFMONTH(LAST_DAY(month_date))           AS aged_vip
     , fc.total_vips_on_date * 1.0 / DAYOFMONTH(LAST_DAY(month_date)) AS total_vips_on_date
FROM (
         SELECT DISTINCT (CASE
                              WHEN mbfa.bu = 'JFNA' THEN 'JUSTFAB NA'
                              WHEN mbfa.bu = 'SDNA' THEN 'SHOEDAZZLE'
                              WHEN mbfa.bu = 'FKNA' THEN 'FABKIDS'
                              WHEN mbfa.bu = 'JFEU' THEN 'JUSTFAB EU'
             END)                                                                                                         AS business_unit
                       , mbfa.month
                       , FIRST_VALUE(mbfa.media_spend)
                                     OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC)              AS media_spend
                       , FIRST_VALUE(mbfa.cpl)
                                     OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC)              AS cpl
                       , FIRST_VALUE(mbfa.m1_lead_to_vip)
                                     OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC)              AS m1_lead_to_vip
                       , FIRST_VALUE(mbfa.vip_cpa)
                                     OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC)              AS vip_cpa
                       , FIRST_VALUE(mbfa.leads)
                                     OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC)              AS leads
                       , FIRST_VALUE(mbfa.m1_vips)
                                     OVER (PARTITION BY mbfa.bu, mbfa.version,mbfa.month ORDER BY mbfa.version_date DESC) AS m1_vips
                       , FIRST_VALUE(mbfa.total_new_vips - mbfa.m1_vips)
                                     OVER (PARTITION BY mbfa.bu, mbfa.version,mbfa.month ORDER BY mbfa.version_date DESC) AS aged_vip
                       , FIRST_VALUE(mbfa.total_new_vips)
                                     OVER (PARTITION BY mbfa.bu, mbfa.version,mbfa.month ORDER BY mbfa.version_date DESC) AS total_vips_on_date
         FROM lake.fpa.monthly_budget_forecast_actual mbfa
         WHERE mbfa.bu IN ('JFNA', 'JFEU', 'SDNA', 'FKNA')
           AND mbfa.version = 'Forecast'
           AND mbfa.month >= DATEADD(MONTH, -1, $start_date)
           AND mbfa.month <= $start_date
     ) fc
         JOIN edw_prod.data_model_jfb.dim_date dd
              ON dd.month_date = fc.month;

CREATE OR REPLACE TEMPORARY TABLE _cancels_target AS
SELECT a.business_unit
     , a.month
     , a.date
     , a.total_vips_on_date
     , a.cancels
     , LAG(a.cum_cancels, 1) OVER (PARTITION BY a.business_unit, a.month ORDER BY a.date) AS last_cum_cancel
     , LAG(a.cum_vip_on_date, 1)
           OVER (PARTITION BY a.business_unit, a.month ORDER BY a.date)                   AS last_cum_vip_on_date
     , a.bop_vip + COALESCE(last_cum_vip_on_date, 0) - COALESCE(last_cum_cancel, 0)       AS bop_vip
     , a.bop_vip + a.cum_vip_on_date - a.cum_cancels                                      AS eop_vip
     , a.cancels / (a.bop_vip + COALESCE(last_cum_vip_on_date, 0) - COALESCE(last_cum_cancel, 0) +
                    a.total_vips_on_date)                                                 AS attrition_rate
FROM (
         SELECT fc.business_unit
              , fc.month
              , dd.full_date                                                             AS date
              , fc.total_vips_on_date * 1.0 / DAYOFMONTH(LAST_DAY(month_date))           AS total_vips_on_date
              , fc.cancels * 1.0 / DAYOFMONTH(LAST_DAY(month_date))                      AS cancels
              , fc.bop_vip
              , SUM(fc.cancels * 1.0 / DAYOFMONTH(LAST_DAY(month_date)))
                    OVER (PARTITION BY fc.business_unit, fc.month ORDER BY dd.full_date) AS cum_cancels
              , SUM(fc.total_vips_on_date * 1.0 / DAYOFMONTH(LAST_DAY(month_date)))
                    OVER (PARTITION BY fc.business_unit, fc.month ORDER BY dd.full_date) AS cum_vip_on_date
         FROM (
                  SELECT DISTINCT (CASE
                                       WHEN mbfa.bu = 'JFNA' THEN 'JUSTFAB NA'
                                       WHEN mbfa.bu = 'SDNA' THEN 'SHOEDAZZLE'
                                       WHEN mbfa.bu = 'FKNA' THEN 'FABKIDS'
                                       WHEN mbfa.bu = 'JFEU' THEN 'JUSTFAB EU'
                      END)                                                                                            AS business_unit
                                , mbfa.month
                                , FIRST_VALUE(mbfa.total_new_vips)
                                              OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC) AS total_vips_on_date
                                , FIRST_VALUE(mbfa.cancels)
                                              OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC) AS cancels
                                , FIRST_VALUE(mbfa.bom_vips)
                                              OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC) AS bop_vip
                  FROM lake.fpa.monthly_budget_forecast_actual mbfa
                  WHERE mbfa.bu IN ('JFNA', 'JFEU', 'SDNA', 'FKNA')
                    AND mbfa.version = 'Forecast'
                    AND mbfa.month = $start_date
              ) fc
                  JOIN edw_prod.data_model_jfb.dim_date dd
                       ON dd.month_date = fc.month
                           AND dd.month_date = $start_date
     ) a;


CREATE OR REPLACE TEMPORARY TABLE _activating_order_target AS
SELECT UPPER(CASE
                 WHEN UPPER(a.business_unit) = 'JUSTFAB' THEN a.business_unit || ' ' || a.region
                 ELSE a.business_unit END)                                    AS business_unit
     , a.date
     , a.orders                                                               AS activating_orders
     , a.upt                                                                  AS activating_upt
     , a.aur                                                                  AS activating_aur
     , a.auc                                                                  AS activating_auc
     , a.units                                                                AS activating_units
     , a.aov_with_shipping                                                    AS activating_aov_with_shipping
     , a.gaap_revenue                                                         AS activating_gaap_gross_revenue
     , (CASE
            WHEN COALESCE(a.gaap_gross_margin_percent, 0) = 0
                THEN 0
            ELSE a.gaap_gross_margin * 1.0 / a.gaap_gross_margin_percent END) AS activating_gaap_net_revenue
     , a.units * a.auc                                                        AS activating_total_cogs
     , a.gaap_gross_margin                                                    AS activating_product_gross_margin
     , a.gaap_gross_margin_percent                                            AS activating_product_gross_margin_percent
     , a.retail_sales
FROM lake_view.sharepoint.gfb_daily_sale_standup_activating_tsos a
WHERE LOWER(a.source) = 'locked forecast'
  AND DATE_TRUNC(MONTH, a.date) >= DATEADD(MONTH, -1, $start_date);


CREATE OR REPLACE TEMPORARY TABLE _repeat_order_target AS
SELECT UPPER(CASE
                 WHEN UPPER(a.business_unit) = 'JUSTFAB' THEN a.business_unit || ' ' || a.region
                 ELSE a.business_unit END)                                    AS business_unit
     , a.date
     , a.orders                                                               AS repeat_orders
     , a.upt                                                                  AS repeat_upt
     , a.aur                                                                  AS repeat_aur
     , a.auc                                                                  AS repeat_auc
     , a.units                                                                AS repeat_units
     , a.aov_with_shipping                                                    AS repeat_aov_with_shipping
     , a.gaap_revenue                                                         AS repeat_gaap_gross_revenue
     , (CASE
            WHEN COALESCE(a.gaap_gross_margin_percent, 0) = 0
                THEN 0
            ELSE a.gaap_gross_margin * 1.0 / a.gaap_gross_margin_percent END) AS repeat_gaap_net_revenue
     , a.units * a.auc                                                        AS repeat_total_cogs
     , a.gaap_gross_margin                                                    AS repeat_product_gross_margin
     , a.gaap_gross_margin_percent                                            AS repeat_product_gross_margin_percent
     , a.retail_sales
FROM lake_view.sharepoint.gfb_daily_sale_standup_repeat_tsos a
WHERE LOWER(a.source) = 'locked forecast'
  AND DATE_TRUNC(MONTH, a.date) >= DATEADD(MONTH, -1, $start_date);


CREATE OR REPLACE TEMPORARY TABLE _activating_order_outlook AS
SELECT UPPER(CASE
                 WHEN UPPER(a.business_unit) = 'JUSTFAB' THEN a.business_unit || ' ' || a.region
                 ELSE a.business_unit END)                              AS business_unit
     , a.date
     , a.orders                                                         AS activating_orders
     , a.upt                                                            AS activating_upt
     , a.aur                                                            AS activating_aur
     , a.auc                                                            AS activating_auc
     , a.units                                                          AS activating_units
     , a.aov_with_shipping                                              AS activating_aov_with_shipping
     , a.gaap_revenue                                                   AS activating_gaap_gross_revenue
     , (CASE
            WHEN a.gaap_gross_margin_percent = 0 THEN 0
            ELSE a.gaap_gross_margin / a.gaap_gross_margin_percent END) AS activating_gaap_net_revenue
     , a.units * a.auc                                                  AS activating_total_cogs
     , a.gaap_gross_margin                                              AS activating_product_gross_margin
     , a.gaap_gross_margin_percent                                      AS activating_product_gross_margin_percent
FROM lake_view.sharepoint.gfb_daily_sale_standup_activating_tsos a
WHERE LOWER(a.source) = 'forecast/actuals'
  AND DATE_TRUNC(MONTH, a.date) = $start_date;


CREATE OR REPLACE TEMPORARY TABLE _repeat_order_outlook AS
SELECT UPPER(CASE
                 WHEN UPPER(a.business_unit) = 'JUSTFAB' THEN a.business_unit || ' ' || a.region
                 ELSE a.business_unit END)               AS business_unit
     , a.date
     , a.orders                                          AS repeat_orders
     , a.upt                                             AS repeat_upt
     , a.aur                                             AS repeat_aur
     , a.auc                                             AS repeat_auc
     , a.units                                           AS repeat_units
     , a.aov_with_shipping                               AS repeat_aov_with_shipping
     , a.gaap_revenue                                    AS repeat_gaap_gross_revenue
     , a.gaap_gross_margin / a.gaap_gross_margin_percent AS repeat_gaap_net_revenue
     , a.units * a.auc                                   AS repeat_total_cogs
     , a.gaap_gross_margin                               AS repeat_product_gross_margin
     , a.gaap_gross_margin_percent                       AS repeat_product_gross_margin_percent
FROM lake_view.sharepoint.gfb_daily_sale_standup_repeat_tsos a
WHERE LOWER(a.source) = 'forecast/actuals'
  AND DATE_TRUNC(MONTH, a.date) = $start_date;


CREATE OR REPLACE TEMPORARY TABLE _credit_billing_lm AS
SELECT DISTINCT f.business_unit
              , f.date
              , COALESCE(a.credit_billings,
                         LAG(a.credit_billings) OVER (PARTITION BY f.business_unit ORDER BY f.date))              AS credit_billings
              , COALESCE(a.credit_redemptions, LAG(a.credit_redemptions)
                                                   OVER (PARTITION BY f.business_unit ORDER BY f.date))           AS credit_redemptions
              , COALESCE(a.credit_cancels, LAG(a.credit_cancels)
                                               OVER (PARTITION BY f.business_unit ORDER BY f.date))               AS credit_cancels
              , COALESCE(a.net_unredeemed_credit, LAG(a.net_unredeemed_credit)
                                                      OVER (PARTITION BY f.business_unit ORDER BY f.date))        AS net_unredeemed_credit
              , COALESCE(a.billing_count, LAG(a.billing_count)
                                              OVER (PARTITION BY f.business_unit ORDER BY f.date))                AS billing_count
              --percent
              , COALESCE(a.credit_billing_percent_lm, LAG(a.credit_billing_percent_lm)
                                                          OVER (PARTITION BY f.business_unit ORDER BY f.date))    AS credit_billing_percent_lm
              , COALESCE(a.credit_redemption_percent_lm, LAG(a.credit_redemption_percent_lm)
                                                             OVER (PARTITION BY f.business_unit ORDER BY f.date)) AS credit_redemption_percent_lm
              , COALESCE(a.credit_cancel_percent_lm, LAG(a.credit_cancel_percent_lm)
                                                         OVER (PARTITION BY f.business_unit ORDER BY f.date))     AS credit_cancel_percent_lm
FROM (
         SELECT DISTINCT bu.business_unit
                       , dd.full_date AS date
         FROM (
                  SELECT DISTINCT (CASE
                                       WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
                                       WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
                                       WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
                                       WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
                      END) AS business_unit
                  FROM edw_prod.reporting.daily_cash_final_output a
                  WHERE a.report_mapping IN ('JF-TREV-NA', 'JF-TREV-EU', 'SD-TREV-NA', 'FK-TREV-NA')
                    AND a.currency_type = 'USD'
                    AND a.date_object = 'placed'
                    AND a.date >= DATEADD(MONTH, -1, $start_date)
                    AND a.date <= $end_date
              ) bu
                  JOIN edw_prod.data_model_jfb.dim_date dd
                       ON dd.month_date >= DATEADD(MONTH, -1, $start_date)
                           AND dd.month_date <= $start_date
     ) f
         LEFT JOIN
     (
         SELECT (CASE
                     WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
                     WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
                     WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
                     WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
             END)                                                                                  AS business_unit
              , DATEADD(MONTH, 1, a.date)                                                          AS date
              --membership credit
              , a.billed_credit_cash_transaction_amount                                            AS credit_billings
              , a.billed_cash_credit_redeemed_amount                                               AS credit_redemptions
              , a.billed_credit_cash_refund_chargeback_amount                                      AS credit_cancels
              , a.billed_credit_cash_transaction_amount - a.billed_cash_credit_redeemed_amount -
                a.billed_credit_cash_refund_chargeback_amount                                      AS net_unredeemed_credit
              , a.billed_credit_cash_transaction_count                                             AS billing_count
              --total of month
              , SUM(a.billed_credit_cash_transaction_amount)
                    OVER (PARTITION BY business_unit,DATE_TRUNC(MONTH, DATEADD(MONTH, 1, a.date))) AS total_credit_billings
              , SUM(a.billed_cash_credit_redeemed_amount)
                    OVER (PARTITION BY business_unit,DATE_TRUNC(MONTH, DATEADD(MONTH, 1, a.date))) AS total_credit_redemptions
              , SUM(a.billed_credit_cash_refund_chargeback_amount)
                    OVER (PARTITION BY business_unit,DATE_TRUNC(MONTH, DATEADD(MONTH, 1, a.date))) AS total_credit_cancels
              --percent
              , IFF(total_credit_billings = 0, 0, credit_billings * 1.0 /
                                                  total_credit_billings)                           AS credit_billing_percent_lm
              , IFF(total_credit_redemptions = 0, 0, credit_redemptions * 1.0 /
                                                     total_credit_redemptions)                     AS credit_redemption_percent_lm
              , IFF(total_credit_cancels = 0, 0, credit_cancels * 1.0 /
                                                 total_credit_cancels)                             AS credit_cancel_percent_lm
         FROM edw_prod.reporting.daily_cash_final_output a
         WHERE a.report_mapping IN ('JF-TREV-NA', 'JF-TREV-EU', 'SD-TREV-NA', 'FK-TREV-NA')
           AND a.currency_type = 'USD'
           AND a.date_object = 'placed'
           AND a.date >= DATEADD(MONTH, -2, $start_date)
           AND a.date <= DATEADD(DAY, -1, $start_date)
           AND DAYOFMONTH(a.date) <= DAYOFMONTH(LAST_DAY($end_date))
     ) a ON a.business_unit = f.business_unit
         AND a.date = f.date;


CREATE OR REPLACE TEMPORARY TABLE _credit_billing_target AS
SELECT fc.business_unit
     , cbl.date
     , fc.credit_billing_fc * cbl.credit_billing_percent_lm        AS credit_billing
     , fc.credit_redeemed_fc * cbl.credit_redemption_percent_lm    AS credit_redemption
     , fc.credit_cancel_fc * cbl.credit_cancel_percent_lm          AS credit_cancels
     , fc.credit_billing_fc * cbl.credit_billing_percent_lm
    - fc.credit_redeemed_fc * cbl.credit_redemption_percent_lm
    - fc.credit_cancel_fc * cbl.credit_cancel_percent_lm           AS net_unredeemed_credit
     , credit_redemption * fc.refund_credit_redeemed_rate          AS refund_credit_redeemed
     , fc.credit_billing_count_fc / DAYOFMONTH(LAST_DAY(cbl.date)) AS credit_billing_count
FROM (
         SELECT DISTINCT (CASE
                              WHEN mbfa.bu = 'JFNA' THEN 'JUSTFAB NA'
                              WHEN mbfa.bu = 'SDNA' THEN 'SHOEDAZZLE'
                              WHEN mbfa.bu = 'FKNA' THEN 'FABKIDS'
                              WHEN mbfa.bu = 'JFEU' THEN 'JUSTFAB EU'
             END)                                                                                            AS business_unit
                       , mbfa.month
                       , FIRST_VALUE(mbfa.membership_credits_charged)
                                     OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC) AS credit_billing_fc
                       , FIRST_VALUE(mbfa.membership_credits_redeemed)
                                     OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC) AS credit_redeemed_fc
                       , FIRST_VALUE(mbfa.membership_credits_refunded_plus_chargebacks)
                                     OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC) AS credit_cancel_fc
                       , FIRST_VALUE(ABS(mbfa.refund_credit_redeemed))
                                     OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC) AS refund_credit_redeemed_fc
                       , refund_credit_redeemed_fc * 1.0 / credit_redeemed_fc                                AS refund_credit_redeemed_rate
                       , FIRST_VALUE(mbfa.membership_credits_charged_count)
                                     OVER (PARTITION BY mbfa.bu, mbfa.month ORDER BY mbfa.version_date DESC) AS credit_billing_count_fc
         FROM lake.fpa.monthly_budget_forecast_actual mbfa
         WHERE mbfa.bu IN ('JFNA', 'JFEU', 'SDNA', 'FKNA')
           AND mbfa.version = 'Forecast'
           AND mbfa.month >= DATEADD(MONTH, -1, $start_date)
     ) fc
         LEFT JOIN _credit_billing_lm cbl
                   ON cbl.business_unit = fc.business_unit
                       AND DATE_TRUNC(MONTH, cbl.date) = fc.month;


CREATE OR REPLACE TEMPORARY TABLE _daily_report_topline_target AS
SELECT rot.business_unit
     , rot.date
     , aot.activating_gaap_net_revenue
     , rot.repeat_gaap_net_revenue
     , cbt.net_unredeemed_credit
     , cbt.refund_credit_redeemed
     , aot.activating_gaap_net_revenue
    + rot.repeat_gaap_net_revenue
    + cbt.net_unredeemed_credit
    + cbt.refund_credit_redeemed                         AS net_cash_revenue
     , aot.activating_total_cogs
     , rot.repeat_total_cogs
     , aot.activating_total_cogs + rot.repeat_total_cogs AS total_cogs
     , net_cash_revenue - total_cogs                     AS cash_gm
     , cash_gm * 1.0 / net_cash_revenue                  AS cash_gm_percent
FROM _repeat_order_target rot
         LEFT JOIN _activating_order_target aot
                   ON aot.business_unit = rot.business_unit
                       AND aot.date = rot.date
         LEFT JOIN _credit_billing_target cbt
                   ON cbt.business_unit = rot.business_unit
                       AND cbt.date = rot.date;


CREATE OR REPLACE TEMPORARY TABLE _base_frame AS
SELECT DISTINCT (CASE
                     WHEN st.store_brand_abbr IN ('JF') THEN UPPER(st.store_name_region)
                     ELSE UPPER(store_brand)
    END)                     AS business_unit
              , dd.full_date AS date
FROM edw_prod.data_model_jfb.dim_store st
         JOIN edw_prod.data_model_jfb.dim_date dd
              ON dd.full_date >= DATEADD(MONTH, -1, $start_date)
                  AND dd.full_date <= LAST_DAY($end_date)
WHERE st.store_brand_abbr IN ('JF', 'SD', 'FK')
  AND st.store_full_name NOT LIKE '%(DM)%'
  AND st.store_full_name NOT LIKE '%Wholesale%'
  AND st.store_full_name NOT LIKE '%Heels.com%'
  AND st.store_full_name NOT LIKE '%Retail%'
  AND st.store_full_name NOT LIKE '%Sample%'
  AND st.store_full_name NOT LIKE '%SWAG%'
  AND st.store_full_name NOT LIKE '%PS%';


CREATE OR REPLACE TEMPORARY TABLE _outlook_gsheet AS
SELECT (CASE
            WHEN a.business_unit = 'JFNA' THEN 'JUSTFAB NA'
            WHEN a.business_unit = 'JFEU' THEN 'JUSTFAB EU'
            WHEN a.business_unit = 'SDNA' THEN 'SHOEDAZZLE'
            WHEN a.business_unit = 'FKNA' THEN 'FABKIDS'
    END) AS business_unit
     , a.outlook_month
     --media spend is only for rest of days in the month
     , (CASE
            WHEN (DAYOFMONTH(LAST_DAY(a.outlook_month)) - DAYOFMONTH($end_date)) = 0
                THEN 0
            ELSE (a.media_spend - ac.media_spend) / (DAYOFMONTH(LAST_DAY(a.outlook_month)) - DAYOFMONTH($end_date))
    END) AS media_spend
     , a.vip_cpa
     , a.credit_billings
     , a.new_vips
FROM lake_view.sharepoint.jfb_outlook_input a
         LEFT JOIN
     (
         SELECT aa.business_unit
              , DATE_TRUNC(MONTH, aa.date) AS month
              , SUM(aa.media_spend)        AS media_spend
         FROM _acquisition_actual aa
         GROUP BY aa.business_unit
                , DATE_TRUNC(MONTH, aa.date)
     ) ac ON ac.business_unit = (CASE
                                     WHEN a.business_unit = 'JFNA' THEN 'JUSTFAB NA'
                                     WHEN a.business_unit = 'JFEU' THEN 'JUSTFAB EU'
                                     WHEN a.business_unit = 'SDNA' THEN 'SHOEDAZZLE'
                                     WHEN a.business_unit = 'FKNA' THEN 'FABKIDS'
         END)
         AND ac.month = a.outlook_month
WHERE a.date = (SELECT MAX(b.date) FROM lake_view.sharepoint.jfb_outlook_input b);


CREATE OR REPLACE TEMPORARY TABLE _credit_billing_outlook AS
SELECT a.business_unit
     , a.month
     , (CASE
            WHEN (DAYOFMONTH(LAST_DAY(a.month)) - DAYOFMONTH($end_date)) = 0
                THEN ac.credit_billings / DAYOFMONTH(LAST_DAY(a.month))
            ELSE (a.credit_billing - ac.credit_billings) / (DAYOFMONTH(LAST_DAY(a.month)) - DAYOFMONTH($end_date))
    END) AS credit_billing
     , (CASE
            WHEN (DAYOFMONTH(LAST_DAY(a.month)) - DAYOFMONTH($end_date)) = 0
                THEN ac.credit_redemptions / DAYOFMONTH(LAST_DAY(a.month))
            ELSE (a.credit_redeemed - ac.credit_redemptions) / (DAYOFMONTH(LAST_DAY(a.month)) - DAYOFMONTH($end_date))
    END) AS credit_redemption
     , (CASE
            WHEN (DAYOFMONTH(LAST_DAY(a.month)) - DAYOFMONTH($end_date)) = 0
                THEN ac.credit_cancels / DAYOFMONTH(LAST_DAY(a.month))
            ELSE (a.credit_refund - ac.credit_cancels) / (DAYOFMONTH(LAST_DAY(a.month)) - DAYOFMONTH($end_date))
    END) AS credit_cancels
     , (CASE
            WHEN (DAYOFMONTH(LAST_DAY(a.month)) - DAYOFMONTH($end_date)) = 0
                THEN ac.net_unredeemed_credit / DAYOFMONTH(LAST_DAY(a.month))
            ELSE (a.net_unredeemed_credit_billing - ac.net_unredeemed_credit) /
                 (DAYOFMONTH(LAST_DAY(a.month)) - DAYOFMONTH($end_date))
    END) AS net_unredeemed_credit
     , (CASE
            WHEN (DAYOFMONTH(LAST_DAY(a.month)) - DAYOFMONTH($end_date)) = 0
                THEN ac.total_net_cash_revenue / DAYOFMONTH(LAST_DAY(a.month))
            ELSE (a.cash_net_revenue - ac.total_net_cash_revenue) /
                 (DAYOFMONTH(LAST_DAY(a.month)) - DAYOFMONTH($end_date))
    END) AS cash_net_revenue
     , (CASE
            WHEN (DAYOFMONTH(LAST_DAY(a.month)) - DAYOFMONTH($end_date)) = 0
                THEN ac.billing_count / DAYOFMONTH(LAST_DAY(a.month))
            ELSE (a.credit_billing_count - ac.billing_count) / (DAYOFMONTH(LAST_DAY(a.month)) - DAYOFMONTH($end_date))
    END) AS credit_billing_count
FROM reporting_prod.gfb.gfb055_01_outlook_metrics a
         LEFT JOIN
     (
         SELECT aa.business_unit
              , DATE_TRUNC(MONTH, aa.date)     AS month

              , SUM(aa.credit_billings)        AS credit_billings
              , SUM(aa.credit_redemptions)     AS credit_redemptions
              , SUM(aa.credit_cancels)         AS credit_cancels
              , SUM(aa.net_unredeemed_credit)  AS net_unredeemed_credit
              , SUM(aa.total_net_cash_revenue) AS total_net_cash_revenue
              , SUM(aa.billing_count)          AS billing_count
         FROM _daily_cash_actual aa
         GROUP BY aa.business_unit
                , DATE_TRUNC(MONTH, aa.date)
     ) ac ON ac.business_unit = a.business_unit
         AND ac.month = a.month
WHERE a.date IN (SELECT MAX(date) FROM reporting_prod.gfb.gfb055_01_outlook_metrics);


CREATE OR REPLACE TEMPORARY TABLE _daily_report_topline_outlook AS
SELECT rot.business_unit
     , rot.date
     , aot.activating_gaap_net_revenue
     , rot.repeat_gaap_net_revenue
     , cbt.net_unredeemed_credit
--     ,cbt.refund_credit_redeemed
     , cbt.cash_net_revenue                              AS net_cash_revenue
     , aot.activating_total_cogs
     , rot.repeat_total_cogs
     , aot.activating_total_cogs + rot.repeat_total_cogs AS total_cogs
     , net_cash_revenue - total_cogs                     AS cash_gm
     , cash_gm * 1.0 / net_cash_revenue                  AS cash_gm_percent
     , rot.repeat_gaap_gross_revenue
     , aot.activating_gaap_gross_revenue
FROM _repeat_order_outlook rot
         LEFT JOIN _activating_order_outlook aot
                   ON aot.business_unit = rot.business_unit
                       AND aot.date = rot.date
         LEFT JOIN _credit_billing_outlook cbt
                   ON cbt.business_unit = rot.business_unit
                       AND cbt.month = DATE_TRUNC(MONTH, rot.date);


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.outlook AS
SELECT bf.business_unit
     , bf.date
     --Aquisition
     , (CASE
            WHEN bf.date <= $end_date THEN aa.media_spend
            ELSE og.media_spend END)                      AS media_spend
     , (CASE
            WHEN bf.date <= $end_date THEN aa.cpl
            ELSE atg.cpl END)                             AS cpl
     , (CASE
            WHEN bf.date <= $end_date THEN aa.m1_lead_to_vip
            ELSE atg.m1_lead_to_vip END)                  AS m1_lead_to_vip
     , (CASE
            WHEN bf.date <= $end_date THEN aa.total_vip_cpa
            ELSE og.vip_cpa END)                          AS total_vip_cpa
     , (CASE
            WHEN bf.date <= $end_date THEN aa.primary_leads
            ELSE atg.leads END)                           AS leads
     , (CASE
            WHEN bf.date <= $end_date THEN aa.vips_from_leads_m1
            ELSE atg.m1_vips END)                         AS m1_vips
     , (CASE
            WHEN bf.date <= $end_date THEN aa.aged_vip
            ELSE atg.aged_vip END)                        AS aged_vip
     , (CASE
            WHEN bf.date <= $end_date THEN aa.total_vips_on_date
            ELSE atg.total_vips_on_date END)              AS total_vips_on_date
     --Cancellation
     , (CASE
            WHEN bf.date <= $end_date THEN aa.total_cancels
            ELSE ct.cancels END)                          AS cancels
     , (CASE
            WHEN bf.date <= $end_date THEN aa.bop_vips
            ELSE ct.bop_vip END)                          AS bop_vips
     , (CASE
            WHEN bf.date <= $end_date THEN aa.eop_vips
            ELSE ct.eop_vip END)                          AS eop_vips
     , (CASE
            WHEN bf.date <= $end_date THEN aa.attrition_rate
            ELSE ct.attrition_rate END)                   AS attrition_rate
     --Activating Orders
     , (CASE
            WHEN bf.date <= $end_date THEN dca.activating_product_orders
            ELSE aot.activating_orders END)               AS activating_orders
     , (CASE
            WHEN bf.date <= $end_date THEN dca.activating_upt
            ELSE aot.activating_upt END)                  AS activating_upt
     , (CASE
            WHEN bf.date <= $end_date THEN dca.activating_aur
            ELSE aot.activating_aur END)                  AS activating_aur
     , (CASE
            WHEN bf.date <= $end_date THEN dca.activating_auc
            ELSE aot.activating_auc END)                  AS activating_auc
     , (CASE
            WHEN bf.date <= $end_date THEN dca.activating_product_units
            ELSE aot.activating_units END)                AS activating_units
     , (CASE
            WHEN bf.date <= $end_date THEN dca.activating_aov_incl_shipping
            ELSE aot.activating_aov_with_shipping END)    AS activating_aov_incl_shipping
     , (CASE
            WHEN bf.date <= $end_date THEN dca.activating_gaap_revenue
            ELSE aot.activating_gaap_gross_revenue END)   AS activating_gaap_gross_revenue
     , (CASE
            WHEN bf.date <= $end_date THEN dca.activating_gaap_net_revenue
            ELSE aot.activating_gaap_net_revenue END)     AS activating_gaap_net_revenue
     , (CASE
            WHEN bf.date <= $end_date THEN dca.activaitng_total_cogs
            ELSE aot.activating_total_cogs END)           AS activating_total_cogs
     , (CASE
            WHEN bf.date <= $end_date THEN dca.activating_product_gross_margin
            ELSE aot.activating_product_gross_margin END) AS activating_gaap_gross_margin
     , (CASE
            WHEN bf.date <= $end_date THEN dca.activating_product_gross_revenue_excl_shipping
            ELSE aot.activating_gaap_gross_revenue END)   AS activating_product_gross_revenue_excl_shipping
     --Repeat Orders
     , (CASE
            WHEN bf.date <= $end_date THEN dca.repeat_product_orders
            ELSE rot.repeat_orders END)                   AS repeat_orders
     , (CASE
            WHEN bf.date <= $end_date THEN dca.repeat_upt
            ELSE rot.repeat_upt END)                      AS repeat_upt
     , (CASE
            WHEN bf.date <= $end_date THEN dca.repeat_aur
            ELSE rot.repeat_aur END)                      AS repeat_aur
     , (CASE
            WHEN bf.date <= $end_date THEN dca.repeat_auc
            ELSE rot.repeat_auc END)                      AS repeat_auc
     , (CASE
            WHEN bf.date <= $end_date THEN dca.repeat_product_units
            ELSE rot.repeat_units END)                    AS repeat_units
     , (CASE
            WHEN bf.date <= $end_date THEN dca.repeat_aov_incl_shipping
            ELSE rot.repeat_aov_with_shipping END)        AS repeat_aov_incl_shipping
     , (CASE
            WHEN bf.date <= $end_date THEN dca.repeat_gaap_revenue
            ELSE rot.repeat_gaap_gross_revenue END)       AS repeat_gaap_gross_revenue
     , (CASE
            WHEN bf.date <= $end_date THEN dca.repeat_gaap_net_revenue
            ELSE rot.repeat_gaap_net_revenue END)         AS repeat_gaap_net_revenue
     , (CASE
            WHEN bf.date <= $end_date THEN dca.repeat_total_cogs
            ELSE rot.repeat_total_cogs END)               AS repeat_total_cogs
     , (CASE
            WHEN bf.date <= $end_date THEN dca.repeat_product_gross_margin
            ELSE rot.repeat_product_gross_margin END)     AS repeat_gaap_gross_margin
     , (CASE
            WHEN bf.date <= $end_date THEN dca.nonactivating_product_gross_revenue_excl_shipping
            ELSE rot.repeat_gaap_gross_revenue END)       AS repeat_product_gross_revenue_excl_shipping
     --Membership Credit
     , (CASE
            WHEN bf.date <= $end_date THEN dca.credit_billings
            ELSE cbt.credit_billing END)                  AS credit_billings
     , (CASE
            WHEN bf.date <= $end_date THEN dca.credit_redemptions
            ELSE cbt.credit_redemption END)               AS credit_redemptions
     , (CASE
            WHEN bf.date <= $end_date THEN dca.credit_cancels
            ELSE cbt.credit_cancels END)                  AS credit_cancels
     , (CASE
            WHEN bf.date <= $end_date THEN dca.net_unredeemed_credit
            ELSE cbt.net_unredeemed_credit END)           AS net_unredeemed_credit
     , (CASE
            WHEN bf.date <= $end_date THEN dca.billing_count
            ELSE cbt.credit_billing_count END)            AS billing_count
     --Additional Toplines
     , (CASE
            WHEN bf.date <= $end_date THEN dca.total_net_cash_revenue
            ELSE tt.net_cash_revenue END)                 AS net_cash_revenue
     , (CASE
            WHEN bf.date <= $end_date THEN dca.total_cogs
            ELSE tt.total_cogs END)                       AS total_cogs
     , (CASE
            WHEN bf.date <= $end_date THEN dca.cash_gm
            ELSE tt.cash_gm END)                          AS cash_gm
     , (CASE
            WHEN bf.date <= $end_date THEN dca.cash_gm_percent
            ELSE tt.cash_gm_percent END)                  AS cash_gm_percent
     , og.new_vips                                        AS new_vips_outlook
FROM _base_frame bf
         LEFT JOIN _acquisition_actual aa
                   ON aa.business_unit = bf.business_unit
                       AND aa.date = bf.date
         LEFT JOIN _acquisition_target atg
                   ON atg.business_unit = bf.business_unit
                       AND atg.date = bf.date
    -- left join _cancels_actual cc
--     on cc.business_unit = bf.business_unit
--     and cc.DATE = bf.date
         LEFT JOIN _cancels_target ct
                   ON ct.business_unit = bf.business_unit
                       AND ct.date = bf.date
         LEFT JOIN _daily_cash_actual dca
                   ON dca.business_unit = bf.business_unit
                       AND dca.date = bf.date
         LEFT JOIN _activating_order_outlook aot
                   ON aot.business_unit = bf.business_unit
                       AND aot.date = bf.date
         LEFT JOIN _repeat_order_outlook rot
                   ON rot.business_unit = bf.business_unit
                       AND rot.date = bf.date
         LEFT JOIN _credit_billing_outlook cbt
                   ON cbt.business_unit = bf.business_unit
                       AND cbt.month = DATE_TRUNC(MONTH, bf.date)
         LEFT JOIN _daily_report_topline_outlook tt
                   ON tt.business_unit = bf.business_unit
                       AND tt.date = bf.date
         LEFT JOIN _outlook_gsheet og
                   ON og.business_unit = bf.business_unit
                       AND og.outlook_month = DATE_TRUNC(MONTH, bf.date);


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb059_finance_daily_sales AS
--Yesterday
SELECT o.business_unit
     , 'Yesterday'                                                          AS report_date_range
     , 'Actual'                                                             AS report_type
     , $end_date                                                            AS actualized_through_date
     --Topline
     , o.net_cash_revenue
     , o.cash_gm
     , o.cash_gm * 1.0 / o.net_cash_revenue                                 AS gross_margin_percent
     --Acquisition
     , o.bop_vips
     , o.m1_vips
     , o.aged_vip
     , o.total_vips_on_date                                                 AS new_vips
     , o.cancels
     , o.eop_vips
     , o.attrition_rate
     , o.media_spend
     , o.cpl
     , o.m1_lead_to_vip
     , o.media_spend * 1.0 / o.m1_vips                                      AS m1_paid_cac
     , o.media_spend * 1.0 / o.total_vips_on_date                           AS total_paid_cac
     --Activating
     , o.activating_gaap_net_revenue
     , o.activating_aov_incl_shipping
     , o.activating_gaap_gross_revenue                                      AS activating_gross_revenue
     , o.activating_aur
     , o.activating_upt
     , o.activating_auc
     , o.activating_orders
     , o.activating_units
     , o.activating_gaap_gross_margin
     , o.activating_gaap_gross_margin * 1.0 / o.activating_gaap_net_revenue AS activating_gaap_gross_margin_percent
     --Repeat
     , o.repeat_gaap_net_revenue
     , o.repeat_aov_incl_shipping
     , o.repeat_gaap_gross_revenue                                          AS repeat_gross_revenue
     , o.repeat_aur
     , o.repeat_upt
     , o.repeat_auc
     , o.repeat_orders
     , o.repeat_units
     , o.repeat_gaap_gross_margin
     , o.repeat_gaap_gross_margin * 1.0 / o.repeat_gaap_net_revenue         AS repeat_gaap_gross_margin_percent
     , o.credit_redemptions
     , o.repeat_gaap_net_revenue - o.credit_redemptions                     AS vip_cash
     , o.credit_redemptions * 1.0 / o.repeat_gaap_net_revenue               AS redemption_rate
     , vip_cash * 1.0 / o.repeat_orders                                     AS vip_cash_per_order
     --Membership Credit
     , o.credit_billings
     , o.credit_cancels
     , o.net_unredeemed_credit
     , o.billing_count
     , o.billing_count * 1.0 / o.bop_vips                                   AS credit_billing_rate
     , (CASE
            WHEN o.credit_billings = 0 THEN 0
            ELSE o.credit_cancels * 1.0 / o.credit_billings END)            AS refunds_billing_rate
FROM reporting_prod.gfb.outlook o
WHERE o.date = $end_date
UNION
SELECT bf.business_unit
     , 'Yesterday'                                                  AS report_date_range
     , 'Target'                                                     AS report_type
     , $end_date                                                    AS actualized_through_date
     --Topline
     , tt.net_cash_revenue
     , tt.cash_gm
     , tt.cash_gm * 1.0 / tt.net_cash_revenue                       AS gross_margin_percent
     --Acquisition
     , ct.bop_vip
     , atg.m1_vips
     , atg.aged_vip
     , atg.total_vips_on_date                                       AS new_vips
     , ct.cancels
     , ct.eop_vip
     , ct.attrition_rate
     , atg.media_spend
     , atg.cpl
     , atg.m1_lead_to_vip
     , atg.media_spend * 1.0 / atg.m1_vips                          AS m1_paid_cac
     , atg.media_spend * 1.0 / atg.total_vips_on_date               AS total_paid_cac
     --Activating
     , aot.activating_gaap_net_revenue
     , aot.activating_aov_with_shipping
     , aot.activating_gaap_gross_revenue                            AS activating_gross_revenue
     , aot.retail_sales * 1.0 / aot.activating_units                AS activating_aur
     , aot.activating_upt
     , aot.activating_auc
     , aot.activating_orders
     , aot.activating_units
     , aot.activating_product_gross_margin
     , aot.activating_product_gross_margin * 1.0 /
       aot.activating_gaap_gross_revenue                            AS activating_gaap_gross_margin_percent
     --Repeat
     , rot.repeat_gaap_net_revenue
     , rot.repeat_aov_with_shipping
     , rot.repeat_gaap_gross_revenue                                AS repeat_gross_revenue
     , rot.retail_sales * 1.0 / rot.repeat_units                    AS repeat_aur
     , rot.repeat_upt
     , rot.repeat_auc
     , rot.repeat_orders
     , rot.repeat_units
     , rot.repeat_product_gross_margin
     , rot.repeat_product_gross_margin_percent                      AS repeat_gaap_gross_margin_percent
     , cbt.credit_redemption
     , rot.repeat_gaap_net_revenue - cbt.credit_redemption          AS vip_cash
     , cbt.credit_redemption * 1.0 / rot.repeat_gaap_net_revenue    AS redemption_rate
     , vip_cash * 1.0 / rot.repeat_orders                           AS vip_cash_per_order
     --Membership Credit
     , cbt.credit_billing
     , cbt.credit_cancels
     , cbt.net_unredeemed_credit
     , cbt.credit_billing_count
     , cbt.credit_billing_count * 1.0 / ct.bop_vip                  AS credit_billing_rate
     , (CASE
            WHEN cbt.credit_billing = 0 THEN 0
            ELSE cbt.credit_cancels * 1.0 / cbt.credit_billing END) AS refunds_billing_rate
FROM _base_frame bf
         LEFT JOIN _daily_report_topline_target tt
                   ON tt.business_unit = bf.business_unit
                       AND tt.date = bf.date
         LEFT JOIN _acquisition_target atg
                   ON atg.business_unit = bf.business_unit
                       AND atg.date = bf.date
         LEFT JOIN _cancels_target ct
                   ON ct.business_unit = bf.business_unit
                       AND ct.date = bf.date
         LEFT JOIN _activating_order_target aot
                   ON aot.business_unit = bf.business_unit
                       AND aot.date = bf.date
         LEFT JOIN _repeat_order_target rot
                   ON rot.business_unit = bf.business_unit
                       AND rot.date = bf.date
         LEFT JOIN _credit_billing_target cbt
                   ON cbt.business_unit = bf.business_unit
                       AND cbt.date = bf.date
WHERE bf.date = $end_date
--Month To Date
UNION
SELECT o.business_unit
     , 'Month To Date'                                                                       AS report_date_range
     , 'Actual'                                                                              AS report_type
     , $end_date                                                                             AS actualized_through_date
     --Topline
     , SUM(o.net_cash_revenue)                                                               AS net_cash_revenue
     , SUM(o.cash_gm)                                                                        AS cash_gm
     , SUM(o.cash_gm) * 1.0 / SUM(o.net_cash_revenue)                                        AS gross_margin_percent
     --Acquisition
     , SUM(CASE
               WHEN o.date = $start_date THEN o.bop_vips
               ELSE 0 END)                                                                   AS bop_vips
     , SUM(o.m1_vips)                                                                        AS m1_vips
     , SUM(o.aged_vip)                                                                       AS aged_vip
     , SUM(o.total_vips_on_date)                                                             AS new_vips
     , SUM(o.cancels)                                                                        AS cancels
     , SUM(CASE
               WHEN o.date = $end_date THEN o.eop_vips
               ELSE 0 END)                                                                   AS eop_vips
     , SUM(o.cancels) * 1.0 / (SUM(CASE
                                       WHEN o.date = $start_date THEN o.bop_vips
                                       ELSE 0 END) + new_vips)                               AS attrition_rate
     , SUM(o.media_spend)                                                                    AS media_spend
     , SUM(o.media_spend) * 1.0 / SUM(o.leads)                                               AS cpl
     , AVG(o.m1_lead_to_vip)                                                                 AS m1_lead_to_vip
     , SUM(o.media_spend) * 1.0 / SUM(o.m1_vips)                                             AS m1_paid_cac
     , SUM(o.media_spend) * 1.0 / SUM(o.total_vips_on_date)                                  AS total_paid_cac
     --Activating
     , SUM(o.activating_gaap_net_revenue)                                                    AS activating_gaap_net_revenue
     , AVG(o.activating_aov_incl_shipping)                                                   AS activating_aov_incl_shipping
     , SUM(o.activating_gaap_gross_revenue)                                                  AS activating_gross_revenue
     , SUM(o.activating_product_gross_revenue_excl_shipping) * 1.0 / SUM(o.activating_units) AS activating_aur
     , SUM(o.activating_units) * 1.0 / SUM(o.activating_orders)                              AS activating_upt
     , SUM(o.activating_total_cogs) * 1.0 / SUM(o.activating_units)                          AS activating_auc
     , SUM(o.activating_orders)                                                              AS activating_orders
     , SUM(o.activating_units)                                                               AS activating_units
     , SUM(o.activating_gaap_gross_margin)                                                   AS activating_gaap_gross_margin
     , SUM(o.activating_gaap_gross_margin) * 1.0 /
       SUM(o.activating_gaap_gross_revenue)                                                  AS activating_gaap_gross_margin_percent
     --Repeat
     , SUM(o.repeat_gaap_net_revenue)                                                        AS repeat_gaap_net_revenue
     , AVG(o.repeat_aov_incl_shipping)                                                       AS repeat_aov_incl_shipping
     , SUM(o.repeat_gaap_gross_revenue)                                                      AS repeaet_gross_revenue
     , SUM(o.repeat_product_gross_revenue_excl_shipping) * 1.0 / SUM(o.repeat_units)         AS repeat_aur
     , SUM(o.repeat_units) * 1.0 / SUM(o.repeat_orders)                                      AS repeat_upt
     , SUM(o.repeat_total_cogs) * 1.0 / SUM(o.repeat_units)                                  AS repeat_auc
     , SUM(o.repeat_orders)                                                                  AS repeat_orders
     , SUM(o.repeat_units)                                                                   AS repeat_units
     , SUM(o.repeat_gaap_gross_margin)                                                       AS repeat_gaap_gross_margin
     , SUM(o.repeat_gaap_gross_margin) * 1.0 /
       SUM(o.repeat_gaap_gross_revenue)                                                      AS repeat_gaap_gross_margin_percent
     , SUM(o.credit_redemptions)                                                             AS credit_redemptions
     , SUM(o.repeat_gaap_net_revenue - o.credit_redemptions)                                 AS vip_cash
     , SUM(o.credit_redemptions) * 1.0 / SUM(o.repeat_gaap_net_revenue)                      AS redemption_rate
     , vip_cash * 1.0 / SUM(o.repeat_orders)                                                 AS vip_cash_per_order
     --Membership Credit
     , SUM(o.credit_billings)                                                                AS credit_billings
     , SUM(o.credit_cancels)                                                                 AS credit_cancels
     , SUM(o.net_unredeemed_credit)                                                          AS net_unredeemed_credit
     , SUM(o.billing_count)                                                                  AS billing_count
     , SUM(o.billing_count) * 1.0 / SUM(o.bop_vips)                                          AS credit_billing_rate
     , (CASE
            WHEN SUM(o.credit_billings) = 0 THEN 0
            ELSE SUM(o.credit_cancels) * 1.0 / SUM(o.credit_billings) END)                   AS refunds_billing_rate
FROM reporting_prod.gfb.outlook o
WHERE o.date <= $end_date
  AND o.date >= $start_date
GROUP BY o.business_unit
UNION
SELECT bf.business_unit
     , 'Month To Date'                                                        AS report_date_range
     , 'Target'                                                               AS report_type
     , $end_date                                                              AS actualized_through_date
     --Topline
     , SUM(tt.net_cash_revenue)                                               AS net_cash_revenue
     , SUM(tt.cash_gm)                                                        AS cash_gm
     , SUM(tt.cash_gm) * 1.0 / SUM(tt.net_cash_revenue)                       AS gross_margin_percent
     --Acquisition
     , SUM(CASE
               WHEN bf.date = $start_date THEN ct.bop_vip
               ELSE 0 END)                                                    AS bop_vip
     , SUM(atg.m1_vips)                                                       AS m1_vips
     , SUM(atg.aged_vip)                                                      AS aged_vip
     , SUM(atg.total_vips_on_date)                                            AS new_vips
     , SUM(ct.cancels)                                                        AS cancels
     , SUM(CASE
               WHEN bf.date = $end_date THEN ct.eop_vip
               ELSE 0 END)                                                    AS eop_vip
     , SUM(ct.cancels) * 1.0 / (SUM(CASE
                                        WHEN bf.date = $start_date THEN ct.bop_vip
                                        ELSE 0 END) + new_vips)               AS attrition_rate
     , SUM(atg.media_spend)                                                   AS media_spend
     , SUM(atg.media_spend) * 1.0 / SUM(atg.leads)                            AS cpl
     , AVG(atg.m1_lead_to_vip)                                                AS m1_lead_to_vip
     , SUM(atg.media_spend) * 1.0 / SUM(atg.m1_vips)                          AS m1_paid_cac
     , SUM(atg.media_spend) * 1.0 / SUM(atg.total_vips_on_date)               AS total_paid_cac
     --Activating
     , SUM(aot.activating_gaap_net_revenue)                                   AS activating_gaap_net_revenue
     , AVG(aot.activating_aov_with_shipping)                                  AS activating_aov_with_shipping
     , SUM(aot.activating_gaap_gross_revenue)                                 AS activating_gross_revenue
     , SUM(aot.retail_sales) * 1.0 / SUM(aot.activating_units)                AS activating_aur
     , SUM(aot.activating_units) * 1.0 / SUM(aot.activating_orders)           AS activating_upt
     , SUM(aot.activating_total_cogs) * 1.0 / SUM(aot.activating_units)       AS activating_auc
     , SUM(aot.activating_orders)                                             AS activating_orders
     , SUM(aot.activating_units)                                              AS activating_units
     , SUM(aot.activating_product_gross_margin)                               AS activating_gaap_gross_margin
     , SUM(aot.activating_product_gross_margin) * 1.0 /
       SUM(aot.activating_gaap_gross_revenue)                                 AS activating_gaap_gross_margin_percent
     --Repeat
     , SUM(rot.repeat_gaap_net_revenue)                                       AS repeat_gaap_net_revenue
     , AVG(rot.repeat_aov_with_shipping)                                      AS repeat_aov_with_shipping
     , SUM(rot.repeat_gaap_gross_revenue)                                     AS repeat_gross_revenue
     , SUM(rot.retail_sales) * 1.0 / SUM(rot.repeat_units)                    AS repeat_aur
     , SUM(rot.repeat_units) * 1.0 / SUM(rot.repeat_orders)                   AS repeat_upt
     , SUM(rot.repeat_total_cogs) * 1.0 / SUM(rot.repeat_units)               AS repeat_auc
     , SUM(rot.repeat_orders)                                                 AS repeat_orders
     , SUM(rot.repeat_units)                                                  AS repeat_units
     , SUM(rot.repeat_product_gross_margin)                                   AS repeat_gaap_gross_margin
     , SUM(rot.repeat_product_gross_margin) * 1.0 /
       SUM(rot.repeat_gaap_gross_revenue)                                     AS repeat_gaap_gross_margin_percent
     , SUM(cbt.credit_redemption)                                             AS credit_redemption
     , SUM(rot.repeat_gaap_net_revenue - cbt.credit_redemption)               AS vip_cash
     , SUM(cbt.credit_redemption) * 1.0 / SUM(rot.repeat_gaap_net_revenue)    AS redemption_rate
     , vip_cash * 1.0 / SUM(rot.repeat_orders)                                AS vip_cash_per_order
     --Membership Credit
     , SUM(cbt.credit_billing)                                                AS credit_billing
     , SUM(cbt.credit_cancels)                                                AS credit_cancels
     , SUM(cbt.net_unredeemed_credit)                                         AS net_unredeemed_credit
     , SUM(cbt.credit_billing_count)                                          AS credit_billing_count
     , SUM(cbt.credit_billing_count) * 1.0 / SUM(ct.bop_vip)                  AS credit_billing_rate
     , (CASE
            WHEN SUM(cbt.credit_billing) = 0 THEN 0
            ELSE SUM(cbt.credit_cancels) * 1.0 / SUM(cbt.credit_billing) END) AS refunds_billing_rate
FROM _base_frame bf
         LEFT JOIN _daily_report_topline_target tt
                   ON tt.business_unit = bf.business_unit
                       AND tt.date = bf.date
         LEFT JOIN _acquisition_target atg
                   ON atg.business_unit = bf.business_unit
                       AND atg.date = bf.date
         LEFT JOIN _cancels_target ct
                   ON ct.business_unit = bf.business_unit
                       AND ct.date = bf.date
         LEFT JOIN _activating_order_target aot
                   ON aot.business_unit = bf.business_unit
                       AND aot.date = bf.date
         LEFT JOIN _repeat_order_target rot
                   ON rot.business_unit = bf.business_unit
                       AND rot.date = bf.date
         LEFT JOIN _credit_billing_target cbt
                   ON cbt.business_unit = bf.business_unit
                       AND cbt.date = bf.date
WHERE bf.date <= $end_date
  AND bf.date >= $start_date
GROUP BY bf.business_unit
--Full Month
UNION
SELECT o.business_unit
     , 'Full Month'                                                                          AS report_date_range
     , 'Outlook'                                                                             AS report_type
     , $end_date                                                                             AS actualized_through_date
     --Topline
     , SUM(o.net_cash_revenue)                                                               AS net_cash_revenue
     , SUM(o.cash_gm)                                                                        AS cash_gm
     , SUM(o.cash_gm) * 1.0 / SUM(o.net_cash_revenue)                                        AS gross_margin_percent
     --Acquisition
     , SUM(CASE
               WHEN o.date = $start_date THEN o.bop_vips
               ELSE 0 END)                                                                   AS bop_vips
     , SUM(IFF(o.business_unit IN ('JUSTFAB NA', 'SHOEDAZZLE'), o.new_vips_outlook / DAYOFMONTH(LAST_DAY($end_date)),
               o.total_vips_on_date)) * .70                                                  AS m1_vips
     , SUM(IFF(o.business_unit IN ('JUSTFAB NA', 'SHOEDAZZLE'), o.new_vips_outlook / DAYOFMONTH(LAST_DAY($end_date)),
               o.total_vips_on_date)) * .30                                                  AS aged_vip
     , SUM(IFF(o.business_unit IN ('JUSTFAB NA', 'SHOEDAZZLE'), o.new_vips_outlook / DAYOFMONTH(LAST_DAY($end_date)),
               o.total_vips_on_date))                                                        AS new_vips
     , SUM(o.cancels)                                                                        AS cancels
     , SUM(CASE
               WHEN o.date = $end_date THEN o.eop_vips
               ELSE 0 END)                                                                   AS eop_vips
     , SUM(o.cancels) * 1.0 / (SUM(CASE
                                       WHEN o.date = $start_date THEN o.bop_vips
                                       ELSE 0 END) + new_vips)                               AS attrition_rate
     , SUM(o.media_spend)                                                                    AS media_spend
     , SUM(o.media_spend) * 1.0 / SUM(o.leads)                                               AS cpl
     , AVG(o.m1_lead_to_vip)                                                                 AS m1_lead_to_vip
     , SUM(o.media_spend) * 1.0 /
       (SUM(IFF(o.business_unit IN ('JUSTFAB NA', 'SHOEDAZZLE'), o.new_vips_outlook / DAYOFMONTH(LAST_DAY($end_date)),
                o.total_vips_on_date)) * .70)                                                AS m1_paid_cac
     , SUM(o.media_spend) * 1.0 /
       SUM(IFF(o.business_unit IN ('JUSTFAB NA', 'SHOEDAZZLE'), o.new_vips_outlook / DAYOFMONTH(LAST_DAY($end_date)),
               o.total_vips_on_date))                                                        AS total_paid_cac
     --Activating
     , SUM(o.activating_gaap_net_revenue)                                                    AS activating_gaap_net_revenue
     , AVG(o.activating_aov_incl_shipping)                                                   AS activating_aov_incl_shipping
     , SUM(o.activating_gaap_gross_revenue)                                                  AS activating_gross_revenue
     , SUM(o.activating_product_gross_revenue_excl_shipping) * 1.0 / SUM(o.activating_units) AS activating_aur
     , SUM(o.activating_units) * 1.0 / SUM(o.activating_orders)                              AS activating_upt
     , SUM(o.activating_total_cogs) * 1.0 / SUM(o.activating_units)                          AS activating_auc
     , SUM(o.activating_orders)                                                              AS activating_orders
     , SUM(o.activating_units)                                                               AS activating_units
     , SUM(o.activating_gaap_gross_margin)                                                   AS activating_gaap_gross_margin
     , SUM(o.activating_gaap_gross_margin) * 1.0 /
       SUM(o.activating_gaap_net_revenue)                                                    AS activating_gaap_gross_margin_percent
     --Repeat
     , SUM(o.repeat_gaap_net_revenue)                                                        AS repeat_gaap_net_revenue
     , AVG(o.repeat_aov_incl_shipping)                                                       AS repeat_aov_incl_shipping
     , SUM(o.repeat_gaap_gross_revenue)                                                      AS repeat_gross_revenue
     , SUM(o.repeat_product_gross_revenue_excl_shipping) * 1.0 / SUM(o.repeat_units)         AS repeat_aur
     , SUM(o.repeat_units) * 1.0 / SUM(o.repeat_orders)                                      AS repeat_upt
     , SUM(o.repeat_total_cogs) * 1.0 / SUM(o.repeat_units)                                  AS repeat_auc
     , SUM(o.repeat_orders)                                                                  AS repeat_orders
     , SUM(o.repeat_units)                                                                   AS repeat_units
     , SUM(o.repeat_gaap_gross_margin)                                                       AS repeat_gaap_gross_margin
     , SUM(o.repeat_gaap_gross_margin) * 1.0 /
       SUM(o.repeat_gaap_net_revenue)                                                        AS repeat_gaap_gross_margin_percent
     , SUM(o.credit_redemptions)                                                             AS credit_redemptions
     , SUM(o.repeat_gaap_net_revenue - o.credit_redemptions)                                 AS vip_cash
     , SUM(o.credit_redemptions) * 1.0 / SUM(o.repeat_gaap_net_revenue)                      AS redemption_rate
     , vip_cash * 1.0 / SUM(o.repeat_orders)                                                 AS vip_cash_per_order
     --Membership Credit
     , SUM(o.credit_billings)                                                                AS credit_billings
     , SUM(o.credit_cancels)                                                                 AS credit_cancels
     , SUM(o.net_unredeemed_credit)                                                          AS net_unredeemed_credit
     , SUM(o.billing_count)                                                                  AS billing_count
     , SUM(o.billing_count) * 1.0 / SUM(o.bop_vips)                                          AS credit_billing_rate
     , (CASE
            WHEN SUM(o.credit_billings) = 0 THEN 0
            ELSE SUM(o.credit_cancels) * 1.0 / SUM(o.credit_billings) END)                   AS refunds_billing_rate
FROM reporting_prod.gfb.outlook o
WHERE o.date <= LAST_DAY($end_date)
  AND o.date >= $start_date
GROUP BY o.business_unit
UNION
SELECT bf.business_unit
     , 'Full Month'                                                           AS report_date_range
     , 'Target'                                                               AS report_type
     , $end_date                                                              AS actualized_through_date
     --Topline
     , SUM(tt.net_cash_revenue)                                               AS net_cash_revenue
     , SUM(tt.cash_gm)                                                        AS cash_gm
     , SUM(tt.cash_gm) * 1.0 / SUM(tt.net_cash_revenue)                       AS gross_margin_percent
     --Acquisition
     , SUM(CASE
               WHEN bf.date = $start_date THEN ct.bop_vip
               ELSE 0 END)                                                    AS bop_vip
     , SUM(atg.m1_vips)                                                       AS m1_vips
     , SUM(atg.aged_vip)                                                      AS aged_vip
     , SUM(atg.total_vips_on_date)                                            AS new_vips
     , SUM(ct.cancels)                                                        AS cancels
     , SUM(CASE
               WHEN bf.date = $end_date THEN ct.eop_vip
               ELSE 0 END)                                                    AS eop_vip
     , SUM(ct.cancels) * 1.0 / (SUM(CASE
                                        WHEN bf.date = $start_date THEN ct.bop_vip
                                        ELSE 0 END) + new_vips)               AS attrition_rate
     , SUM(atg.media_spend)                                                   AS media_spend
     , SUM(atg.media_spend) * 1.0 / SUM(atg.leads)                            AS cpl
     , AVG(atg.m1_lead_to_vip)                                                AS m1_lead_to_vip
     , SUM(atg.media_spend) * 1.0 / SUM(atg.m1_vips)                          AS m1_paid_cac
     , SUM(atg.media_spend) * 1.0 / SUM(atg.total_vips_on_date)               AS total_paid_cac
     --Activating
     , SUM(aot.activating_gaap_net_revenue)                                   AS activating_gaap_net_revenue
     , AVG(aot.activating_aov_with_shipping)                                  AS activating_aov_with_shipping
     , SUM(aot.activating_gaap_gross_revenue)                                 AS activating_gross_revenue
     , SUM(aot.retail_sales) * 1.0 / SUM(aot.activating_units)                AS activating_aur
     , SUM(aot.activating_units) * 1.0 / SUM(aot.activating_orders)           AS activating_upt
     , SUM(aot.activating_total_cogs) * 1.0 / SUM(aot.activating_units)       AS activating_auc
     , SUM(aot.activating_orders)                                             AS activating_orders
     , SUM(aot.activating_units)                                              AS activating_units
     , SUM(aot.activating_product_gross_margin)                               AS activating_gaap_gross_margin
     , SUM(aot.activating_product_gross_margin) * 1.0 /
       SUM(aot.activating_gaap_gross_revenue)                                 AS activating_gaap_gross_margin_percent
     --Repeat
     , SUM(rot.repeat_gaap_net_revenue)                                       AS repeat_gaap_net_revenue
     , AVG(rot.repeat_aov_with_shipping)                                      AS repeat_aov_with_shipping
     , SUM(rot.repeat_gaap_gross_revenue)                                     AS repeat_gross_revenue
     , SUM(rot.retail_sales) * 1.0 / SUM(rot.repeat_units)                    AS repeat_aur
     , SUM(rot.repeat_units) * 1.0 / SUM(rot.repeat_orders)                   AS repeat_upt
     , SUM(rot.repeat_total_cogs) * 1.0 / SUM(rot.repeat_units)               AS repeat_auc
     , SUM(rot.repeat_orders)                                                 AS repeat_orders
     , SUM(rot.repeat_units)                                                  AS repeat_units
     , SUM(rot.repeat_product_gross_margin)                                   AS repeat_gaap_gross_margin
     , SUM(rot.repeat_product_gross_margin) * 1.0 /
       SUM(rot.repeat_gaap_gross_revenue)                                     AS repeat_gaap_gross_margin_percent
     , SUM(cbt.credit_redemption)                                             AS credit_redemption
     , SUM(rot.repeat_gaap_net_revenue - cbt.credit_redemption)               AS vip_cash
     , SUM(cbt.credit_redemption) * 1.0 / SUM(rot.repeat_gaap_net_revenue)    AS redemption_rate
     , vip_cash * 1.0 / SUM(rot.repeat_orders)                                AS vip_cash_per_order
     --Membership Credit
     , SUM(cbt.credit_billing)                                                AS credit_billing
     , SUM(cbt.credit_cancels)                                                AS credit_cancels
     , SUM(cbt.net_unredeemed_credit)                                         AS net_unredeemed_credit
     , SUM(cbt.credit_billing_count)                                          AS credit_billing_count
     , SUM(cbt.credit_billing_count) * 1.0 / SUM(ct.bop_vip)                  AS credit_billing_rate
     , (CASE
            WHEN SUM(cbt.credit_billing) = 0 THEN 0
            ELSE SUM(cbt.credit_cancels) * 1.0 / SUM(cbt.credit_billing) END) AS refunds_billing_rate
FROM _base_frame bf
         LEFT JOIN _daily_report_topline_target tt
                   ON tt.business_unit = bf.business_unit
                       AND tt.date = bf.date
         LEFT JOIN _acquisition_target atg
                   ON atg.business_unit = bf.business_unit
                       AND atg.date = bf.date
         LEFT JOIN _cancels_target ct
                   ON ct.business_unit = bf.business_unit
                       AND ct.date = bf.date
         LEFT JOIN _activating_order_target aot
                   ON aot.business_unit = bf.business_unit
                       AND aot.date = bf.date
         LEFT JOIN _repeat_order_target rot
                   ON rot.business_unit = bf.business_unit
                       AND rot.date = bf.date
         LEFT JOIN _credit_billing_target cbt
                   ON cbt.business_unit = bf.business_unit
                       AND cbt.date = bf.date
WHERE bf.date <= LAST_DAY($end_date)
  AND bf.date >= $start_date
GROUP BY bf.business_unit
UNION
SELECT DISTINCT (CASE
                     WHEN a.bu = 'JFNA' THEN 'JUSTFAB NA'
                     WHEN a.bu = 'SDNA' THEN 'SHOEDAZZLE'
                     WHEN a.bu = 'FKNA' THEN 'FABKIDS'
                     WHEN a.bu = 'JFEU' THEN 'JUSTFAB EU'
    END)                                                                                          AS business_unit
              , 'Full Month'                                                                      AS report_date_range
              , 'Budget'                                                                          AS report_type
              , $end_date                                                                         AS actualized_through_date
              , a.net_cash_revenue_total                                                          AS net_cash_revenue
              , a.cash_gross_margin                                                               AS cash_gm
              , cash_gm * 1.0 / net_cash_revenue                                                  AS gross_margin_percent
              , a.bom_vips                                                                        AS bop_vip
              , a.m1_vips
              , a.total_new_vips - a.m1_vips                                                      AS aged_vips
              , a.total_new_vips                                                                  AS new_vips
              , a.cancels
              , bop_vip + new_vips - a.cancels                                                    AS eop_vip
              , a.cancels * 1.0 / (bop_vip + new_vips)                                            AS attrition_rate
              , a.media_spend
              , a.cpl
              , a.m1_lead_to_vip
              , a.media_spend * 1.0 / a.m1_vips                                                   AS m1_paid_cac
              , a.media_spend * 1.0 / a.total_new_vips                                            AS total_paid_cac
              , a.activating_gaap_net_revenue
              , a.activating_aov_incl_shipping_rev                                                AS activating_aov_with_shipping
              , a.activating_gaap_gross_revenue                                                   AS activating_gross_revenue
              , a.activating_gaap_net_revenue * 1.0 / a.activating_units                          AS activating_aur
              , a.activating_units * 1.0 / a.activating_order_count                               AS activating_upt
              , (a.activating_gaap_net_revenue - a.activating_gross_margin_$) * 1.0 /
                a.activating_units                                                                AS activating_auc
              , a.activating_order_count                                                          AS activating_orders
              , a.activating_units                                                                AS activating_units
              , a.activating_gross_margin_$                                                       AS activating_gaap_gross_margin
              , a.activating_gross_margin_percent                                                 AS activating_gaap_gross_margin_percent
              , a.repeat_gaap_net_revenue
              , a.repeat_aov_incl_shipping_rev                                                    AS repeat_aov_with_shipping
              , a.repeat_gaap_gross_revenue                                                       AS repeat_gross_revenue
              , a.repeat_gaap_net_revenue * 1.0 / a.repeat_units                                  AS repeat_aur
              , a.repeat_units * 1.0 / a.repeat_order_count                                       AS repeat_upt
              , (a.repeat_gaap_net_revenue - a.repeat_gaap_gross_margin_$) * 1.0 / a.repeat_units AS repeat_auc
              , a.repeat_order_count                                                              AS repeat_orders
              , a.repeat_units                                                                    AS repeat_units
              , a.repeat_gaap_gross_margin_$                                                      AS repeat_gaap_gross_margin
              , a.repeat_gaap_gross_margin_percent                                                AS repeat_gaap_gross_margin_percent
              , a.membership_credits_redeemed                                                     AS credit_redemption
              , a.repeat_gaap_net_revenue - a.membership_credits_redeemed                         AS vip_cash
              , a.membership_credits_redeemed * 1.0 / a.repeat_gaap_net_revenue                   AS redemption_rate
              , vip_cash * 1.0 / a.repeat_order_count                                             AS vip_cash_per_order
              , a.membership_credits_charged                                                      AS credit_billing
              , a.membership_credits_charged * a.credit_canceled_percent                          AS credit_cancels
              , a.net_unredeemed_credit_billings                                                  AS net_unredeemed_credit
              , a.membership_credits_charged_count                                                AS credit_billing_count
              , a.membership_credits_charged_count * 1.0 / a.bom_vips                             AS credit_billing_rate
              , credit_cancels * 1.0 / credit_billing                                             AS refunds_billing_rate
FROM lake.fpa.monthly_budget_forecast_actual a
WHERE a.bu IN ('JFNA', 'JFEU', 'SDNA', 'FKNA')
  AND a.version = 'Budget'
  AND a.month = $start_date
UNION
SELECT o.business_unit
     , 'Last Week'                                                                           AS report_date_range
     , 'Actual'                                                                              AS report_type
     , $end_date                                                                             AS actualized_through_date
     --Topline
     , SUM(o.net_cash_revenue)                                                               AS net_cash_revenue
     , SUM(o.cash_gm)                                                                        AS cash_gm
     , SUM(o.cash_gm) * 1.0 / SUM(o.net_cash_revenue)                                        AS gross_margin_percent
     --Acquisition
     , SUM(CASE
               WHEN o.date = DATE_TRUNC(WEEK, DATEADD(DAY, -1, DATE_TRUNC(WEEK, CURRENT_DATE()))) THEN o.bop_vips
               ELSE 0 END)                                                                   AS bop_vips
     , SUM(o.m1_vips)                                                                        AS m1_vips
     , SUM(o.aged_vip)                                                                       AS aged_vip
     , SUM(o.total_vips_on_date)                                                             AS new_vips
     , SUM(o.cancels)                                                                        AS cancels
     , SUM(CASE
               WHEN o.date = $end_date THEN o.eop_vips
               ELSE 0 END)                                                                   AS eop_vips
     , SUM(o.cancels) * 1.0 / (SUM(CASE
                                       WHEN o.date =
                                            DATE_TRUNC(WEEK, DATEADD(DAY, -1, DATE_TRUNC(WEEK, CURRENT_DATE())))
                                           THEN o.bop_vips
                                       ELSE 0 END) + new_vips)                               AS attrition_rate
     , SUM(o.media_spend)                                                                    AS media_spend
     , SUM(o.media_spend) * 1.0 / SUM(o.leads)                                               AS cpl
     , AVG(o.m1_lead_to_vip)                                                                 AS m1_lead_to_vip
     , SUM(o.media_spend) * 1.0 / SUM(o.m1_vips)                                             AS m1_paid_cac
     , SUM(o.media_spend) * 1.0 / SUM(o.total_vips_on_date)                                  AS total_paid_cac
     --Activating
     , SUM(o.activating_gaap_net_revenue)                                                    AS activating_gaap_net_revenue
     , AVG(o.activating_aov_incl_shipping)                                                   AS activating_aov_incl_shipping
     , SUM(o.activating_gaap_gross_revenue)                                                  AS activating_gross_revenue
     , SUM(o.activating_product_gross_revenue_excl_shipping) * 1.0 / SUM(o.activating_units) AS activating_aur
     , SUM(o.activating_units) * 1.0 / SUM(o.activating_orders)                              AS activating_upt
     , SUM(o.activating_total_cogs) * 1.0 / SUM(o.activating_units)                          AS activating_auc
     , SUM(o.activating_orders)                                                              AS activating_orders
     , SUM(o.activating_units)                                                               AS activating_units
     , SUM(o.activating_gaap_gross_margin)                                                   AS activating_gaap_gross_margin
     , SUM(o.activating_gaap_gross_margin) * 1.0 /
       SUM(o.activating_gaap_gross_revenue)                                                  AS activating_gaap_gross_margin_percent
     --Repeat
     , SUM(o.repeat_gaap_net_revenue)                                                        AS repeat_gaap_net_revenue
     , AVG(o.repeat_aov_incl_shipping)                                                       AS repeat_aov_incl_shipping
     , SUM(o.repeat_gaap_gross_revenue)                                                      AS repeat_gross_revenue
     , SUM(o.repeat_product_gross_revenue_excl_shipping) * 1.0 / SUM(o.repeat_units)         AS repeat_aur
     , SUM(o.repeat_units) * 1.0 / SUM(o.repeat_orders)                                      AS repeat_upt
     , SUM(o.repeat_total_cogs) * 1.0 / SUM(o.repeat_units)                                  AS repeat_auc
     , SUM(o.repeat_orders)                                                                  AS repeat_orders
     , SUM(o.repeat_units)                                                                   AS repeat_units
     , SUM(o.repeat_gaap_gross_margin)                                                       AS repeat_gaap_gross_margin
     , SUM(o.repeat_gaap_gross_margin) * 1.0 /
       SUM(o.repeat_gaap_gross_revenue)                                                      AS repeat_gaap_gross_margin_percent
     , SUM(o.credit_redemptions)                                                             AS credit_redemptions
     , SUM(o.repeat_gaap_net_revenue - o.credit_redemptions)                                 AS vip_cash
     , SUM(o.credit_redemptions) * 1.0 / SUM(o.repeat_gaap_net_revenue)                      AS redemption_rate
     , vip_cash * 1.0 / SUM(o.repeat_orders)                                                 AS vip_cash_per_order
     --Membership Credit
     , SUM(o.credit_billings)                                                                AS credit_billings
     , SUM(o.credit_cancels)                                                                 AS credit_cancels
     , SUM(o.net_unredeemed_credit)                                                          AS net_unredeemed_credit
     , SUM(o.billing_count)                                                                  AS billing_count
     , SUM(o.billing_count) * 1.0 / SUM(o.bop_vips)                                          AS credit_billing_rate
     , (CASE
            WHEN SUM(o.credit_billings) = 0 THEN 0
            ELSE SUM(o.credit_cancels) * 1.0 / SUM(o.credit_billings) END)                   AS refunds_billing_rate
FROM reporting_prod.gfb.outlook o
WHERE o.date <= DATEADD(DAY, -1, DATE_TRUNC(WEEK, CURRENT_DATE()))
  AND o.date >= DATE_TRUNC(WEEK, DATEADD(DAY, -1, DATE_TRUNC(WEEK, CURRENT_DATE())))
GROUP BY o.business_unit
UNION
SELECT bf.business_unit
     , 'Last Week'                                                            AS report_date_range
     , 'Target'                                                               AS report_type
     , $end_date                                                              AS actualized_through_date
     --Topline
     , SUM(tt.net_cash_revenue)                                               AS net_cash_revenue
     , SUM(tt.cash_gm)                                                        AS cash_gm
     , SUM(tt.cash_gm) * 1.0 / SUM(tt.net_cash_revenue)                       AS gross_margin_percent
     --Acquisition
     , SUM(CASE
               WHEN bf.date = DATE_TRUNC(WEEK, DATEADD(DAY, -1, DATE_TRUNC(WEEK, CURRENT_DATE()))) THEN ct.bop_vip
               ELSE 0 END)                                                    AS bop_vip
     , SUM(atg.m1_vips)                                                       AS m1_vips
     , SUM(atg.aged_vip)                                                      AS aged_vip
     , SUM(atg.total_vips_on_date)                                            AS new_vips
     , SUM(ct.cancels)                                                        AS cancels
     , SUM(CASE
               WHEN bf.date = $end_date THEN ct.eop_vip
               ELSE 0 END)                                                    AS eop_vip
     , SUM(ct.cancels) * 1.0 / (SUM(CASE
                                        WHEN bf.date =
                                             DATE_TRUNC(WEEK, DATEADD(DAY, -1, DATE_TRUNC(WEEK, CURRENT_DATE())))
                                            THEN ct.bop_vip
                                        ELSE 0 END) + new_vips)               AS attrition_rate
     , SUM(atg.media_spend)                                                   AS media_spend
     , SUM(atg.media_spend) * 1.0 / SUM(atg.leads)                            AS cpl
     , AVG(atg.m1_lead_to_vip)                                                AS m1_lead_to_vip
     , SUM(atg.media_spend) * 1.0 / SUM(atg.m1_vips)                          AS m1_paid_cac
     , SUM(atg.media_spend) * 1.0 / SUM(atg.total_vips_on_date)               AS total_paid_cac
     --Activating
     , SUM(aot.activating_gaap_net_revenue)                                   AS activating_gaap_net_revenue
     , AVG(aot.activating_aov_with_shipping)                                  AS activating_aov_with_shipping
     , SUM(aot.activating_gaap_gross_revenue)                                 AS activating_gross_revenue
     , SUM(aot.retail_sales) * 1.0 / SUM(aot.activating_units)                AS activating_aur
     , SUM(aot.activating_units) * 1.0 / SUM(aot.activating_orders)           AS activating_upt
     , SUM(aot.activating_total_cogs) * 1.0 / SUM(aot.activating_units)       AS activating_auc
     , SUM(aot.activating_orders)                                             AS activating_orders
     , SUM(aot.activating_units)                                              AS activating_units
     , SUM(aot.activating_product_gross_margin)                               AS activating_gaap_gross_margin
     , SUM(aot.activating_product_gross_margin) * 1.0 /
       SUM(aot.activating_gaap_gross_revenue)                                 AS activating_gaap_gross_margin_percent
     --Repeat
     , SUM(rot.repeat_gaap_net_revenue)                                       AS repeat_gaap_net_revenue
     , AVG(rot.repeat_aov_with_shipping)                                      AS repeat_aov_with_shipping
     , SUM(rot.repeat_gaap_gross_revenue)                                     AS repeat_gross_revenue
     , SUM(rot.retail_sales) * 1.0 / SUM(rot.repeat_units)                    AS repeat_aur
     , SUM(rot.repeat_units) * 1.0 / SUM(rot.repeat_orders)                   AS repeat_upt
     , SUM(rot.repeat_total_cogs) * 1.0 / SUM(rot.repeat_units)               AS repeat_auc
     , SUM(rot.repeat_orders)                                                 AS repeat_orders
     , SUM(rot.repeat_units)                                                  AS repeat_units
     , SUM(rot.repeat_product_gross_margin)                                   AS repeat_gaap_gross_margin
     , SUM(rot.repeat_product_gross_margin) * 1.0 /
       SUM(rot.repeat_gaap_gross_revenue)                                     AS repeat_gaap_gross_margin_percent
     , SUM(cbt.credit_redemption)                                             AS credit_redemption
     , SUM(rot.repeat_gaap_net_revenue - cbt.credit_redemption)               AS vip_cash
     , SUM(cbt.credit_redemption) * 1.0 / SUM(rot.repeat_gaap_net_revenue)    AS redemption_rate
     , vip_cash * 1.0 / SUM(rot.repeat_orders)                                AS vip_cash_per_order
     --Membership Credit
     , SUM(cbt.credit_billing)                                                AS credit_billing
     , SUM(cbt.credit_cancels)                                                AS credit_cancels
     , SUM(cbt.net_unredeemed_credit)                                         AS net_unredeemed_credit
     , SUM(cbt.credit_billing_count)                                          AS credit_billing_count
     , SUM(cbt.credit_billing_count) * 1.0 / SUM(ct.bop_vip)                  AS credit_billing_rate
     , (CASE
            WHEN SUM(cbt.credit_billing) = 0 THEN 0
            ELSE SUM(cbt.credit_cancels) * 1.0 / SUM(cbt.credit_billing) END) AS refunds_billing_rate
FROM _base_frame bf
         LEFT JOIN _daily_report_topline_target tt
                   ON tt.business_unit = bf.business_unit
                       AND tt.date = bf.date
         LEFT JOIN _acquisition_target atg
                   ON atg.business_unit = bf.business_unit
                       AND atg.date = bf.date
         LEFT JOIN _cancels_target ct
                   ON ct.business_unit = bf.business_unit
                       AND ct.date = bf.date
         LEFT JOIN _activating_order_target aot
                   ON aot.business_unit = bf.business_unit
                       AND aot.date = bf.date
         LEFT JOIN _repeat_order_target rot
                   ON rot.business_unit = bf.business_unit
                       AND rot.date = bf.date
         LEFT JOIN _credit_billing_target cbt
                   ON cbt.business_unit = bf.business_unit
                       AND cbt.date = bf.date
WHERE bf.date <= DATEADD(DAY, -1, DATE_TRUNC(WEEK, CURRENT_DATE()))
  AND bf.date >= DATE_TRUNC(WEEK, DATEADD(DAY, -1, DATE_TRUNC(WEEK, CURRENT_DATE())))
GROUP BY bf.business_unit;
