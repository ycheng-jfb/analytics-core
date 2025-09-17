SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, CURRENT_DATE()));
SET end_date = DATE_TRUNC(MONTH, CURRENT_DATE());

CREATE OR REPLACE TEMPORARY TABLE _last_month_base AS
SELECT DISTINCT (CASE
                     WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
                     WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
                     WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
                     WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
    END)                                                               AS business_unit
              , DATE_TRUNC(MONTH, a.date)                              AS month
              , LAST_DAY(a.date)                                       AS last_day_of_month
              , IFF(RIGHT(CURRENT_DATE(), 2) > RIGHT(LAST_DAY(a.date), 2), LAST_DAY(a.date),
                    CONCAT(LEFT(a.date, 8), RIGHT(CURRENT_DATE(), 2))) AS current_day
              , DATEDIFF(DAY, CONCAT(LEFT(a.date, 8), RIGHT(current_day, 2))::DATE,
                         LAST_DAY(a.date))                             AS number_of_days
FROM edw_prod.reporting.daily_cash_final_output a
WHERE a.report_mapping IN ('JF-TREV-NA', 'JF-TREV-EU', 'SD-TREV-NA', 'FK-TREV-NA')
  AND a.currency_type = 'USD'
  AND a.date_object = 'placed'
  AND a.date >= $start_date
  AND a.date <= $end_date;


CREATE OR REPLACE TEMPORARY TABLE _daily_cash AS
SELECT CASE
           WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
           WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
           WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
           WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
    END                                                                                           AS business_unit
     , DATE_TRUNC(MONTH, a.date)                                                                  AS month
     , SUM(a.cash_net_revenue)                                                                    AS cash_net_revenue
     , SUM(a.cash_gross_revenue)                                                                  AS total_cash_collected
     , SUM(a.cash_gross_profit)                                                                   AS cash_gross_margin
     , SUM(a.billed_credit_cash_transaction_amount)                                               AS credit_billings
     , SUM(a.billed_cash_credit_redeemed_amount)                                                  AS credit_redeemed
     , SUM(a.billed_credit_cash_refund_chargeback_amount)                                         AS credit_refunds
     , credit_billings - credit_redeemed - credit_refunds                                         AS net_credit_billings
     , SUM(a.activating_product_gross_revenue)                                                    AS activating_product_revenue
     , SUM(a.activating_product_order_count)                                                      AS activating_product_orders
     , SUM(a.activating_unit_count)                                                               AS activating_product_units
     , SUM(a.nonactivating_product_gross_revenue)                                                 AS repeat_product_revenue
     , SUM(a.nonactivating_product_order_count)                                                   AS repeat_product_orders
     , SUM(a.nonactivating_unit_count)                                                            AS repeat_product_units
     , activating_product_units + repeat_product_units                                            AS total_units_shipped
     , activating_product_orders + repeat_product_orders                                          AS total_orders_shipped
     , IFF(SUM(a.activating_unit_count) = 0, 0,
           SUM(a.activating_product_gross_revenue - a.activating_shipping_revenue) * 1.0 /
           SUM(a.activating_unit_count))                                                          AS activating_aur
     , IFF((SUM(a.activating_unit_count + a.activating_product_order_reship_unit_count +
                a.activating_product_order_exchange_unit_count)) = 0, 0,
           SUM(a.activating_product_order_landed_product_cost_amount +
               a.activating_product_order_exchange_product_cost_amount +
               a.activating_product_order_reship_product_cost_amount)
               / SUM(a.activating_unit_count + a.activating_product_order_reship_unit_count +
                     a.activating_product_order_exchange_unit_count))                             AS activating_auc
     , SUM(activating_product_gross_profit)                                                       AS activating_product_margin
     , IFF(SUM(a.nonactivating_unit_count) = 0, 0,
           SUM(a.nonactivating_product_gross_revenue - a.nonactivating_shipping_revenue) * 1.0 /
           SUM(a.nonactivating_unit_count))                                                       AS repeat_aur
     , IFF((SUM(a.nonactivating_unit_count + a.nonactivating_product_order_reship_unit_count +
                a.nonactivating_product_order_exchange_unit_count)) = 0, 0,
           SUM(a.nonactivating_product_order_landed_product_cost_amount +
               a.nonactivating_product_order_exchange_product_cost_amount +
               a.nonactivating_product_order_reship_product_cost_amount)
               / SUM(a.nonactivating_unit_count + a.nonactivating_product_order_reship_unit_count +
                     a.nonactivating_product_order_exchange_unit_count))                          AS repeat_auc
     , SUM(nonactivating_product_gross_profit)                                                    AS repeat_product_margin
     , SUM(a.activating_product_net_revenue)                                                      AS activating_product_net_revenue
     , SUM(a.nonactivating_product_net_revenue)                                                   AS repeat_product_net_revenue
     , IFF(SUM(activating_product_net_revenue) = 0, 0, SUM(activating_product_gross_profit) /
                                                       SUM(activating_product_net_revenue))       AS activating_product_margin_percent
     , IFF(SUM(nonactivating_product_net_revenue) = 0, 0, SUM(nonactivating_product_gross_profit) /
                                                          SUM(nonactivating_product_net_revenue)) AS repeat_product_margin_percent
     , IFF(activating_product_orders = 0, 0, activating_product_margin /
                                             activating_product_orders)                           AS activating_gross_margin_per_order
     , SUM(product_order_cash_refund_amount + billing_cash_refund_amount)                         AS product_order_billing_cash_refund
     , SUM(product_order_cash_chargeback_amount + billing_cash_chargeback_amount)                 AS product_order_billing_chargeback
     , SUM(product_order_noncash_credit_refund_amount +
           product_order_cash_credit_refund_amount)                                               AS product_order_credit_refund_amount
     , SUM(CASE
               WHEN a.date >= lm.current_day AND a.date <= lm.last_day_of_month
                   THEN product_order_cash_refund_amount + billing_cash_refund_amount
               ELSE 0 END)                                                                        AS previous_product_order_billing_cash_refund
     , SUM(CASE
               WHEN a.date >= lm.current_day AND a.date <= lm.last_day_of_month
                   THEN product_order_cash_chargeback_amount + billing_cash_chargeback_amount
               ELSE 0 END)                                                                        AS previous_product_order_billing_chargeback
     , SUM(CASE
               WHEN a.date >= lm.current_day AND a.date <= lm.last_day_of_month
                   THEN product_order_noncash_credit_refund_amount + product_order_cash_credit_refund_amount
               ELSE 0 END)                                                                        AS previous_product_order_credit_refund_amount
     , MIN(lm.number_of_days)                                                                     AS number_of_days
FROM edw_prod.reporting.daily_cash_final_output a
         LEFT JOIN _last_month_base lm
                   ON lm.business_unit = CASE
                                             WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
                                             WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
                                             WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
                                             WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
                       END
                       AND lm.month = DATE_TRUNC(MONTH, a.date)
WHERE a.report_mapping IN ('JF-TREV-NA', 'JF-TREV-EU', 'SD-TREV-NA', 'FK-TREV-NA')
  AND a.currency_type = 'USD'
  AND a.date_object = 'placed'
  AND DATE_TRUNC(MONTH, a.date) >= $start_date
  AND DATE_TRUNC(MONTH, a.date) <= $end_date
GROUP BY (CASE
              WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
              WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
              WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
              WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
    END)
       , DATE_TRUNC(MONTH, a.date);


CREATE OR REPLACE TEMPORARY TABLE _media_spend AS
SELECT (CASE
            WHEN a.store_name = 'JustFab NA' THEN 'JUSTFAB NA'
            WHEN a.store_name = 'JustFab EU' THEN 'JUSTFAB EU'
            WHEN a.store_name = 'ShoeDazzle NA' THEN 'SHOEDAZZLE'
            WHEN a.store_name = 'FabKids NA' THEN 'FABKIDS'
    END)                                    AS business_unit
     , DATE_TRUNC(MONTH, a.date)            AS month
     , SUM(a.media_spend)                   AS media_spend
     , SUM(CASE
               WHEN a.date = DATE_TRUNC(MONTH, a.date)
                   THEN a.bop_vips
               ELSE 0 END)                  AS bop_vips
     , SUM(a.cancels_on_date)               AS total_cancels
     , SUM(a.total_vips_on_date)            AS new_vips
     , SUM(a.primary_leads)                 AS new_leads
     , AVG(a.total_vips_on_date_cac)        AS total_vips_cac
     , SUM(a.activating_acquisition_margin) AS activating_acquisition_margin
     , SUM(a.vips_from_leads_m1)            AS m1_vips_from_leads
     , SUM(CASE
               WHEN a.date = DATE_TRUNC(MONTH, a.date)
                   THEN a.bop_vips
               ELSE 0 END)
           + SUM(a.total_vips_on_date)
    - SUM(a.cancels_on_date)                AS eop_vips
FROM reporting_media_prod.attribution.total_cac_output a
WHERE a.store_name IN ('JustFab NA', 'JustFab EU', 'ShoeDazzle NA', 'FabKids NA')
  AND a.source = 'Monthly'
  AND a.version = 'JFB GLOBAL - USD'
  AND a.currency = 'USD'
  AND DATE_TRUNC(MONTH, a.date) >= $start_date
  AND DATE_TRUNC(MONTH, a.date) <= $end_date
GROUP BY (CASE
              WHEN a.store_name = 'JustFab NA' THEN 'JUSTFAB NA'
              WHEN a.store_name = 'JustFab EU' THEN 'JUSTFAB EU'
              WHEN a.store_name = 'ShoeDazzle NA' THEN 'SHOEDAZZLE'
              WHEN a.store_name = 'FabKids NA' THEN 'FABKIDS'
    END)
       , DATE_TRUNC(MONTH, a.date)
UNION
SELECT (CASE
            WHEN a.store_name = 'JustFab NA' THEN 'JUSTFAB NA'
            WHEN a.store_name = 'JustFab EU' THEN 'JUSTFAB EU'
            WHEN a.store_name = 'ShoeDazzle NA' THEN 'SHOEDAZZLE'
            WHEN a.store_name = 'FabKids NA' THEN 'FABKIDS'
    END)                                    AS business_unit
     , DATE_TRUNC(MONTH, a.date)            AS month
     , SUM(a.media_spend)                   AS media_spend
     , SUM(CASE
               WHEN a.date = DATE_TRUNC(MONTH, a.date)
                   THEN a.bop_vips
               ELSE 0 END)                  AS bop_vips
     , SUM(a.cancels_on_date)               AS total_cancels
     , SUM(a.total_vips_on_date)            AS new_vips
     , SUM(a.primary_leads)                 AS new_leads
     , AVG(a.total_vips_on_date_cac)        AS total_vips_cac
     , SUM(a.activating_acquisition_margin) AS activating_acquisition_margin
     , SUM(a.vips_from_leads_m1)            AS m1_vips_from_leads
     , SUM(CASE
               WHEN a.date = DATE_TRUNC(MONTH, a.date)
                   THEN a.bop_vips
               ELSE 0 END)
           + SUM(a.total_vips_on_date)
    - SUM(a.cancels_on_date)                AS eop_vips
FROM reporting_media_prod.attribution.total_cac_output a
WHERE a.store_name IN ('JustFab NA', 'JustFab EU', 'ShoeDazzle NA', 'FabKids NA')
  AND a.source = 'DailyTY'
  AND a.version = 'JFB GLOBAL - USD'
  AND a.currency = 'USD'
  AND DATE_TRUNC(MONTH, a.date) >= $start_date
  AND DATE_TRUNC(MONTH, a.date) <= $end_date
GROUP BY (CASE
              WHEN a.store_name = 'JustFab NA' THEN 'JUSTFAB NA'
              WHEN a.store_name = 'JustFab EU' THEN 'JUSTFAB EU'
              WHEN a.store_name = 'ShoeDazzle NA' THEN 'SHOEDAZZLE'
              WHEN a.store_name = 'FabKids NA' THEN 'FABKIDS'
    END)
       , DATE_TRUNC(MONTH, a.date);

CREATE OR REPLACE TEMPORARY TABLE _max_version AS
SELECT DISTINCT a.bu
              , a.month
              , MAX(a.version_date) AS version_date
FROM lake.fpa.monthly_budget_forecast_actual a
WHERE a.bu IN ('JFNA', 'JFEU', 'SDNA', 'FKNA')
  AND a.version = 'Budget'
  AND a.month >= $start_date
  AND a.month <= DATEADD('month', 1, $end_date)
GROUP BY a.bu
       , a.month;

CREATE OR REPLACE TEMPORARY TABLE _budget AS
SELECT (CASE
            WHEN a.bu = 'JFNA' THEN 'JUSTFAB NA'
            WHEN a.bu = 'SDNA' THEN 'SHOEDAZZLE'
            WHEN a.bu = 'FKNA' THEN 'FABKIDS'
            WHEN a.bu = 'JFEU' THEN 'JUSTFAB EU'
    END)                                                          AS business_unit
     , a.month
     , a.version_date
     , SUM(a.cash_net_revenue)                                    AS cash_net_revenue_bt
     , SUM(a.repeat_gaap_net_revenue + a.activating_gaap_net_revenue + a.membership_credits_charged -
           a.membership_credits_redeemed)                         AS total_cash_collected_bt
     , SUM(a.cash_gross_margin)                                   AS cash_gross_margin_bt
     , SUM(a.membership_credits_charged)                          AS credit_billings_bt
     , SUM(a.membership_credits_redeemed)                         AS credit_redeemed_bt
     , SUM(a.membership_credits_refunded_plus_chargebacks)        AS credit_refunds_bt
     , SUM(a.net_unredeemed_credit_billings)                      AS net_credit_billings_bt
     , SUM(a.activating_product_gross_revenue)                    AS activating_product_revenue_bt
     , SUM(a.activating_order_count)                              AS activating_product_orders_bt
     , SUM(a.activating_units)                                    AS activating_product_units_bt
     , SUM(a.repeat_product_gross_revenue)                        AS repeat_product_revenue_bt
     , SUM(a.repeat_order_count)                                  AS repeat_product_orders_bt
     , SUM(a.repeat_units)                                        AS repeat_product_units_bt
     , activating_product_units_bt + repeat_product_units_bt      AS total_units_shipped_bt
     , activating_product_orders_bt + repeat_product_orders_bt    AS total_orders_shipped_bt
     , SUM(a.media_spend)                                         AS media_spend_bt
     , SUM(a.bom_vips)                                            AS bop_vips_bt
     , SUM(a.cancels)                                             AS total_cancels_bt
     , SUM(a.total_new_vips)                                      AS new_vips_bt
     , SUM(a.leads)                                               AS new_leads_bt
     , SUM(a.m1_vips)                                             AS m1_vips_from_leads_bt
     , SUM(a.ending_vips)                                         AS eom_vips_bt
     , SUM(a.activating_gross_margin_$)                           AS activating_product_margin
     , SUM(a.repeat_product_gross_revenue)
    *
       AVG(a.repeat_gaap_gross_margin_percent)                    AS repeat_product_margin
     , SUM(a.activating_gaap_net_revenue)                         AS activating_product_net_revenue
     , SUM(a.repeat_gaap_net_revenue)                             AS repeat_product_net_revenue
     , activating_product_margin / activating_product_net_revenue AS activating_product_margin_percent
     , AVG(a.repeat_gaap_gross_margin_percent)                    AS repeat_product_margin_percent
     , activating_product_margin / activating_product_orders_bt   AS activating_gross_margin_per_order
FROM lake.fpa.monthly_budget_forecast_actual a
         JOIN _max_version mv
              ON a.month = mv.month
                  AND a.bu = mv.bu
                  AND a.version_date = mv.version_date
WHERE a.bu IN ('JFNA', 'JFEU', 'SDNA', 'FKNA')
  AND a.version = 'Budget'
  AND a.month >= $start_date
  AND a.month <= DATEADD('month', 1, $end_date)
GROUP BY (CASE
              WHEN a.bu = 'JFNA' THEN 'JUSTFAB NA'
              WHEN a.bu = 'SDNA' THEN 'SHOEDAZZLE'
              WHEN a.bu = 'FKNA' THEN 'FABKIDS'
              WHEN a.bu = 'JFEU' THEN 'JUSTFAB EU'
    END)
       , a.month
       , a.version_date;


CREATE OR REPLACE TEMPORARY TABLE _forecast AS
SELECT (CASE
            WHEN a.bu = 'JFNA' THEN 'JUSTFAB NA'
            WHEN a.bu = 'SDNA' THEN 'SHOEDAZZLE'
            WHEN a.bu = 'FKNA' THEN 'FABKIDS'
            WHEN a.bu = 'JFEU' THEN 'JUSTFAB EU'
    END)                                                        AS business_unit
     , a.month
     , a.version_date
     , SUM(a.cash_net_revenue)                                  AS cash_net_revenue_fc
     , SUM(a.repeat_gaap_net_revenue + a.activating_gaap_net_revenue + a.membership_credits_charged -
           a.membership_credits_redeemed)                       AS total_cash_collected_fc
     , SUM(a.cash_gross_margin)                                 AS cash_gross_margin_fc
     , SUM(a.membership_credits_charged)                        AS credit_billings_fc
     , SUM(a.membership_credits_redeemed)                       AS credit_redeemed_fc
     , SUM(a.membership_credits_refunded_plus_chargebacks)      AS credit_refunds_fc
     , SUM(a.net_unredeemed_credit_billings)                    AS net_credit_billings_fc
     , SUM(a.activating_product_gross_revenue)                  AS activating_product_revenue_fc
     , SUM(a.activating_order_count)                            AS activating_product_orders_fc
     , SUM(a.activating_units)                                  AS activating_product_units_fc
     , SUM(a.repeat_product_gross_revenue)                      AS repeat_product_revenue_fc
     , SUM(a.repeat_order_count)                                AS repeat_product_orders_fc
     , SUM(a.repeat_units)                                      AS repeat_product_units_fc
     , activating_product_units_fc + repeat_product_units_fc    AS total_units_shipped_fc
     , activating_product_orders_fc + repeat_product_orders_fc  AS total_orders_shipped_fc
     , SUM(a.media_spend)                                       AS media_spend_fc
     , SUM(a.bom_vips)                                          AS bop_vips_fc
     , SUM(a.cancels)                                           AS total_cancels_fc
     , SUM(a.total_new_vips)                                    AS new_vips_fc
     , SUM(a.leads)                                             AS new_leads_fc
     , SUM(a.m1_vips)                                           AS m1_vips_from_leads_fc
     , SUM(a.ending_vips)                                       AS eom_vips_fc
     , SUM(a.activating_gross_margin_$)                         AS activating_product_margin
     , SUM(a.repeat_gaap_gross_margin_$)                        AS repeat_product_margin
     , SUM(a.activating_gaap_net_revenue)                       AS activating_product_net_revenue
     , SUM(a.repeat_gaap_net_revenue)                           AS repeat_product_net_revenue
     , SUM(a.activating_gross_margin_$) /
       SUM(a.activating_gaap_net_revenue)                       AS activating_product_margin_percent
     , AVG(repeat_gaap_gross_margin_percent)                    AS repeat_product_margin_percent
     , activating_product_margin / activating_product_orders_fc AS activating_gross_margin_per_order
FROM lake.fpa.monthly_budget_forecast_actual a
         LEFT JOIN
     (
         SELECT DISTINCT (CASE
                              WHEN a.business_unit = 'JFNA' THEN 'JUSTFAB NA'
                              WHEN a.business_unit LIKE 'SD%' THEN 'SHOEDAZZLE'
                              WHEN a.business_unit LIKE 'FK%' THEN 'FABKIDS'
                              WHEN a.business_unit = 'JFEU' THEN 'JUSTFAB EU'
             END) AS business_unit
                       , a.month
                       , a.version_date
         FROM lake_view.sharepoint.jfb_adhoc_forecast_input_tsos a
         WHERE a.business_unit IN ('JFNA', 'JFEU', 'SDNA', 'FKNA', 'SD', 'FK')
           AND a.month >= $start_date
           AND a.month <= $end_date
     ) b ON b.business_unit = (CASE
                                   WHEN a.bu = 'JFNA' THEN 'JUSTFAB NA'
                                   WHEN a.bu LIKE 'SD%' THEN 'SHOEDAZZLE'
                                   WHEN a.bu LIKE 'FK%' THEN 'FABKIDS'
                                   WHEN a.bu = 'JFEU' THEN 'JUSTFAB EU'
         END)
         AND b.month = a.month
         AND b.version_date = a.version_date
WHERE a.bu IN ('JFNA', 'JFEU', 'SDNA', 'FKNA')
  AND a.version = 'Forecast'
  AND a.month >= $start_date
  AND a.month <= $end_date
  AND b.business_unit IS NULL
GROUP BY (CASE
              WHEN a.bu = 'JFNA' THEN 'JUSTFAB NA'
              WHEN a.bu = 'SDNA' THEN 'SHOEDAZZLE'
              WHEN a.bu = 'FKNA' THEN 'FABKIDS'
              WHEN a.bu = 'JFEU' THEN 'JUSTFAB EU'
    END)
       , a.month
       , a.version_date
UNION
SELECT (CASE
            WHEN a.business_unit = 'JFNA' THEN 'JUSTFAB NA'
            WHEN a.business_unit LIKE 'SD%' THEN 'SHOEDAZZLE'
            WHEN a.business_unit LIKE 'FK%' THEN 'FABKIDS'
            WHEN a.business_unit = 'JFEU' THEN 'JUSTFAB EU'
    END)                                                                           AS business_unit
     , a.month
     , a.version_date
     , a.cash_net_rev                                                              AS cash_net_revenue_fc
     , a.activating_product_revenue + a.repeat_product_revenue + a.credit_billings AS total_cash_collected_fc
     , a.cash_gross_margin                                                         AS cash_gross_margin_fc
     , a.credit_billings                                                           AS credit_billings_fc
     , a.less_redeemed                                                             AS credit_redeemed_fc
     , ABS(a.less_refunds)                                                         AS credit_refunds_fc
     , a.net_credit_billings                                                       AS net_credit_billings_fc
     , a.activating_product_revenue                                                AS activating_product_revenue_fc
     , a.activating_orders_shipped                                                 AS activating_product_orders_fc
     , a.activating_upt * a.activating_orders_shipped                              AS activating_product_units_fc
     , a.repeat_product_revenue                                                    AS repeat_product_revenue_fc
     , a.repeat_orders_shipped                                                     AS repeat_product_orders_fc
     , a.repeat_upt * repeat_orders_shipped                                        AS repeat_product_units_fc
     , activating_product_units_fc + repeat_product_units_fc                       AS total_units_shipped_fc
     , activating_product_orders_fc + repeat_product_orders_fc                     AS total_orders_shipped_fc
     , a.media_spend                                                               AS media_spend_fc
     , a.bop_vips                                                                  AS bop_vips_fc
     , a.cancels                                                                   AS total_cancels_fc
     , a.new_vips                                                                  AS new_vips_fc
     , a.media_spend / a.cpl                                                       AS new_leads_fc
     , (a.media_spend / a.cpl) * m1_lead_to_vip                                    AS m1_vips_from_leads_fc
     , a.eom_vip_count                                                             AS eom_vips_fc
     , a.activating_gross_profit                                                   AS activating_product_margin
     , a.repeat_product_revenue * a.repeat_product_margin_percent                  AS repeat_product_margin
     , (CASE
            WHEN a.activating_product_margin_percent = 0 THEN 0
            ELSE a.activating_gross_profit / a.activating_product_margin_percent
    END)                                                                           AS activating_product_net_revenue
     , 0                                                                           AS repeat_product_net_revenue
     , a.activating_product_margin_percent                                         AS activating_product_margin_percent
     , a.repeat_product_margin_percent                                             AS repeat_product_margin_percent
     , activating_product_margin / activating_product_orders_fc                    AS activating_gross_margin_per_order
FROM lake_view.sharepoint.jfb_adhoc_forecast_input_tsos a
WHERE a.business_unit IN ('JFNA', 'JFEU', 'SDNA', 'FKNA', 'SD', 'FK');

CREATE OR REPLACE TEMPORARY TABLE _actual AS
SELECT dc.business_unit
     , dc.month
     --actual
     , dc.cash_net_revenue
     , dc.total_cash_collected
     , dc.cash_gross_margin
     , dc.credit_billings
     , dc.credit_redeemed
     , dc.credit_refunds
     , dc.net_credit_billings
     , dc.activating_product_revenue
     , dc.activating_product_orders
     , dc.activating_product_units
     , dc.repeat_product_revenue
     , dc.repeat_product_orders
     , dc.repeat_product_units
     , dc.total_units_shipped
     , dc.total_orders_shipped
     , ms.media_spend
     , ms.bop_vips
     , ms.total_cancels
     , ms.new_vips
     , ms.new_leads
     , ms.m1_vips_from_leads
     , ms.eop_vips
     , dc.activating_product_margin
     , dc.repeat_product_margin
     , dc.activating_product_margin_percent
     , dc.repeat_product_margin_percent
     , dc.activating_gross_margin_per_order
     , ms.total_vips_cac
     , ms.activating_acquisition_margin
     , dc.product_order_billing_cash_refund
     , dc.product_order_billing_chargeback
     , dc.product_order_credit_refund_amount
FROM _daily_cash dc
         LEFT JOIN _media_spend ms
                   ON ms.business_unit = dc.business_unit
                       AND ms.month = dc.month
         LEFT JOIN _daily_cash dc1
                   ON dc1.business_unit = dc.business_unit
                       AND DATEADD(MONTH, 1, dc1.month) = dc.month
         LEFT JOIN _daily_cash dc2
                   ON dc2.business_unit = dc.business_unit
                       AND DATEADD(YEAR, 1, dc2.month) = dc.month;


CREATE OR REPLACE TEMPORARY TABLE _forecast_cal AS
SELECT om.business_unit
     , om.month
     , om.last_month
     , om.ly_month
     , lm.bop_vips      AS lm_bop_vip
     , lm.total_cancels AS lm_cancels
     , ly.bop_vips      AS ly_bop_vip
     , ly.total_cancels AS ly_cancels
     , cm.bop_vips * (
            lm_cancels / lm_bop_vip
        + ly_cancels / ly_bop_vip
    ) / 2               AS cancels_fc
FROM (
         SELECT DISTINCT a.business_unit
                       , a.month
                       , DATEADD(MONTH, -1, a.month) AS last_month
                       , DATEADD(YEAR, -1, a.month)  AS ly_month
         FROM reporting_prod.gfb.gfb055_01_outlook_metrics a
         WHERE a.month IS NOT NULL
     ) om
         LEFT JOIN _actual lm
                   ON om.business_unit = lm.business_unit
                       AND om.last_month = lm.month
         LEFT JOIN _actual ly
                   ON om.business_unit = ly.business_unit
                       AND om.ly_month = ly.month
         LEFT JOIN _actual cm
                   ON cm.business_unit = om.business_unit
                       AND cm.month = om.month;


CREATE OR REPLACE TEMPORARY TABLE _outlook AS
SELECT a.business_unit
     , a.month
     , dc1.number_of_days + dc2.number_of_days                               AS number_of_days
     , dc.number_of_days + 1                                                 AS current_month_days_left
     , a.date                                                                AS version_date
     , a.cash_net_revenue
     , a.cash_gross_revenue                                                  AS total_cash_collected
     , a.cash_gross_margin_amount                                            AS cash_gross_margin
     , a.credit_billing                                                      AS credit_billings
     , a.credit_redeemed
     , a.credit_refund                                                       AS credit_refunds
     , a.net_unredeemed_credit_billing                                       AS net_credit_billings
     , a.activating_product_revenue
     , a.activating_order_count                                              AS activating_product_orders
     , a.activating_shipped_units                                            AS activating_product_units
     , a.repeat_product_revenue
     , a.repeat_order_count                                                  AS repeat_product_orders
     , a.repeat_shipped_units                                                AS repeat_product_units
     , a.total_units_shipped
     , a.total_orders_shipped
     , a.media_spend
     , a.bop_vips
     , a.cancels                                                             AS total_cancels
     , a.new_vip                                                             AS new_vips
     , a.leads                                                               AS new_leads
     , a.m1_vip                                                              AS m1_vips_from_leads
     , a.eom_vip_count                                                       AS eop_vips
     , a.activating_gross_margin_percent                                     AS activating_product_margin_percent
     , a.repeat_gross_margin_percent                                         AS repeat_product_margin_percent
     , a.activating_product_margin                                           AS activating_product_margin
     , a.repeat_product_margin                                               AS repeat_product_margin
     , (a.activating_cash_net_revenue * a.activating_gross_margin_percent) /
       a.activating_order_count                                              AS activating_gross_margin_per_order
     , a.vip_cash_percent
     , a.total_vip_on_date_cac
     , a.activating_acquisition_margin
     , dc.product_order_credit_refund_amount +
       ((dc1.previous_product_order_credit_refund_amount + dc2.previous_product_order_credit_refund_amount) /
        (dc1.number_of_days + dc2.number_of_days)) * (dc.number_of_days + 1) AS product_order_credit_refund_amount
     , dc.product_order_billing_chargeback +
       ((dc1.previous_product_order_billing_chargeback + dc2.previous_product_order_billing_chargeback) /
        (dc1.number_of_days + dc2.number_of_days)) * (dc.number_of_days + 1) AS product_order_billing_chargeback
     , dc.product_order_billing_cash_refund +
       ((dc1.previous_product_order_billing_cash_refund + dc2.previous_product_order_billing_cash_refund) /
        (dc1.number_of_days + dc2.number_of_days)) * (dc.number_of_days + 1) AS product_order_billing_cash_refund
FROM reporting_prod.gfb.gfb055_01_outlook_metrics a
         LEFT JOIN _forecast_cal fc
                   ON fc.business_unit = a.business_unit
                       AND fc.month = a.month
         LEFT JOIN _daily_cash dc
                   ON dc.business_unit = a.business_unit
                       AND dc.month = a.month
         LEFT JOIN _daily_cash dc1
                   ON dc1.business_unit = a.business_unit
                       AND DATEADD(MONTH, 1, dc1.month) = a.month
         LEFT JOIN _daily_cash dc2
                   ON dc2.business_unit = a.business_unit
                       AND DATEADD(YEAR, 1, dc2.month) = a.month;


CREATE OR REPLACE TEMPORARY TABLE _store_month_version AS
SELECT DISTINCT fb.month
              , fb.version_date                                                   AS forecast_budget_version_date
              , COALESCE(CAST(o.version_date AS VARCHAR(100)), 'No Outlook Data') AS outlook_version_date
FROM (
         SELECT DISTINCT f.business_unit
                       , f.month
                       , f.version_date
         FROM _forecast f
         UNION
         SELECT DISTINCT b.business_unit
                       , b.month
                       , b.version_date
         FROM _budget b
     ) AS fb
         LEFT JOIN
     (
         SELECT DISTINCT o.business_unit
                       , o.month
                       , o.version_date
         FROM _outlook o
         WHERE o.month IS NOT NULL
           AND o.version_date IS NOT NULL
     ) o ON o.business_unit = fb.business_unit
         AND o.month = fb.month
UNION
SELECT DISTINCT fb.month
              , fb.version_date                                                   AS forecast_budget_version_date
              , COALESCE(CAST(po.version_date AS VARCHAR(100)), 'No Outlook Data') AS outlook_version_date
FROM (
         SELECT DISTINCT f.business_unit
                       , f.month
                       , f.version_date
         FROM _forecast f
         UNION
         SELECT DISTINCT b.business_unit
                       , b.month
                       , b.version_date
         FROM _budget b
     ) AS fb
         LEFT JOIN
     (
         SELECT DISTINCT o.business_unit
                       , o.month
                       , 'No Outlook Data' AS version_date
         FROM _outlook o
         WHERE o.month IS NOT NULL
           AND o.version_date IS NOT NULL
     ) po ON po.business_unit = fb.business_unit
         AND po.month = fb.month
;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb055_month_end_kpi AS
SELECT a.business_unit
     , a.month
     , smv.forecast_budget_version_date
     , smv.outlook_version_date
     , 'Actual'                                                                       AS report_type
     , a.cash_net_revenue
     , a.total_cash_collected
     , a.cash_gross_margin
     , a.credit_billings
     , a.credit_redeemed
     , a.credit_refunds
     , a.net_credit_billings
     , a.activating_product_revenue
     , a.activating_product_orders
     , a.activating_product_units
     , a.repeat_product_revenue
     , a.repeat_product_orders
     , a.repeat_product_units
     , a.total_units_shipped
     , a.total_orders_shipped
     , a.media_spend
     , a.bop_vips
     , a.total_cancels
     , a.new_vips
     , a.new_leads
     , a.m1_vips_from_leads
     , a.eop_vips
     , a.activating_product_margin_percent
     , a.repeat_product_margin_percent
     , a.repeat_product_margin
     , a.activating_product_margin
     , IFF(a.bop_vips = 0, 0, a.repeat_product_orders / a.bop_vips)                   AS merch_purchase_rate
     , IFF((a.new_vips) = 0, 0,
           a.media_spend / a.new_vips - a.activating_gross_margin_per_order)          AS cpaam
     , IFF(a.repeat_product_revenue = 0, 0,
           (a.repeat_product_revenue - a.credit_redeemed) / a.repeat_product_revenue) AS vip_cash_percent
     , a.total_vips_cac
     , IFF(a.new_vips = 0, 0, a.activating_acquisition_margin / a.new_vips)           AS activating_acquisition_margin
     , a.product_order_billing_chargeback +
       a.product_order_credit_refund_amount                                           AS product_order_refunds
FROM _actual a
         LEFT JOIN _store_month_version smv
                   ON smv.month = a.month
UNION
SELECT b.business_unit
     , b.month
     , smv.forecast_budget_version_date
     , smv.outlook_version_date
     , 'Budget'                                                                           AS report_type
     , b.cash_net_revenue_bt
     , b.total_cash_collected_bt
     , b.cash_gross_margin_bt
     , b.credit_billings_bt
     , b.credit_redeemed_bt
     , b.credit_refunds_bt
     , b.net_credit_billings_bt
     , b.activating_product_revenue_bt
     , b.activating_product_orders_bt
     , b.activating_product_units_bt
     , b.repeat_product_revenue_bt
     , b.repeat_product_orders_bt
     , b.repeat_product_units_bt
     , b.total_units_shipped_bt
     , b.total_orders_shipped_bt
     , b.media_spend_bt
     , b.bop_vips_bt
     , b.total_cancels_bt
     , b.new_vips_bt
     , b.new_leads_bt
     , b.m1_vips_from_leads_bt
     , b.eom_vips_bt
     , b.activating_product_margin_percent
     , b.repeat_product_margin_percent
     , b.repeat_product_margin
     , b.activating_product_margin
     , b.repeat_product_orders_bt / b.bop_vips_bt                                         AS merch_purchase_rate
     , b.media_spend_bt / b.new_vips_bt - b.activating_gross_margin_per_order             AS cpaam
     , (b.repeat_product_revenue_bt - b.credit_redeemed_bt) / b.repeat_product_revenue_bt AS vip_cash_percent
     , b.media_spend_bt / b.new_vips_bt                                                   AS total_vips_cac
     , b.activating_gross_margin_per_order                                                AS activating_acquisition_margin
     , 0                                                                                  AS product_order_refunds
FROM _budget b
         JOIN _store_month_version smv
              ON smv.month = b.month
UNION
SELECT f.business_unit
     , f.month
     , smv.forecast_budget_version_date
     , smv.outlook_version_date
     , 'Forecast'                                                                         AS report_type
     , f.cash_net_revenue_fc
     , f.total_cash_collected_fc
     , f.cash_gross_margin_fc
     , f.credit_billings_fc
     , f.credit_redeemed_fc
     , f.credit_refunds_fc
     , f.net_credit_billings_fc
     , f.activating_product_revenue_fc
     , f.activating_product_orders_fc
     , f.activating_product_units_fc
     , f.repeat_product_revenue_fc
     , f.repeat_product_orders_fc
     , f.repeat_product_units_fc
     , f.total_units_shipped_fc
     , f.total_orders_shipped_fc
     , f.media_spend_fc
     , f.bop_vips_fc
     , f.total_cancels_fc
     , f.new_vips_fc
     , f.new_leads_fc
     , f.m1_vips_from_leads_fc
     , f.eom_vips_fc
     , f.activating_product_margin_percent
     , f.repeat_product_margin_percent
     , f.repeat_product_margin
     , f.activating_product_margin
     , f.repeat_product_orders_fc / f.bop_vips_fc                                         AS merch_purchase_rate
     , f.media_spend_fc / f.new_vips_fc - f.activating_gross_margin_per_order             AS cpaam
     , (f.repeat_product_revenue_fc - f.credit_redeemed_fc) / f.repeat_product_revenue_fc AS vip_cash_percent
     , f.media_spend_fc / f.new_vips_fc                                                   AS total_vips_cac
     , f.activating_gross_margin_per_order                                                AS activating_acquisition_margin
     , 0                                                                                  AS product_order_refunds
FROM _forecast f
         JOIN _store_month_version smv
              ON smv.month = f.month
                  AND smv.forecast_budget_version_date = f.version_date
UNION
SELECT a.business_unit
     , DATEADD(YEAR, 1, a.month)                                                      AS month
     , smv.forecast_budget_version_date
     , smv.outlook_version_date
     , 'Prior Year'                                                                   AS report_type
     , a.cash_net_revenue
     , a.total_cash_collected
     , a.cash_gross_margin
     , a.credit_billings
     , a.credit_redeemed
     , a.credit_refunds
     , a.net_credit_billings
     , a.activating_product_revenue
     , a.activating_product_orders
     , a.activating_product_units
     , a.repeat_product_revenue
     , a.repeat_product_orders
     , a.repeat_product_units
     , a.total_units_shipped
     , a.total_orders_shipped
     , a.media_spend
     , a.bop_vips
     , a.total_cancels
     , a.new_vips
     , a.new_leads
     , a.m1_vips_from_leads
     , a.eop_vips
     , a.activating_product_margin_percent
     , a.repeat_product_margin_percent
     , a.repeat_product_margin
     , a.activating_product_margin
     , IFF(a.bop_vips = 0, 0, a.repeat_product_orders / a.bop_vips)                   AS merch_purchase_rate
     , IFF((a.new_vips) = 0, 0,
           a.media_spend / a.new_vips - a.activating_gross_margin_per_order)          AS cpaam
     , IFF(a.repeat_product_revenue = 0, 0,
           (a.repeat_product_revenue - a.credit_redeemed) / a.repeat_product_revenue) AS vip_cash_percent
     , IFF(a.new_vips = 0, 0, a.media_spend / a.new_vips)                             AS total_vips_cac
     , a.activating_gross_margin_per_order                                            AS activating_acquisition_margin
     , a.product_order_billing_chargeback +
       a.product_order_credit_refund_amount                                           AS product_order_refunds
FROM _actual a
         JOIN _store_month_version smv
              ON smv.month = DATEADD(YEAR, 1, a.month)
WHERE DATEADD(YEAR, 1, a.month) <= $end_date
UNION
SELECT a.business_unit
     , a.month
     , smv.forecast_budget_version_date
     , smv.outlook_version_date
     , 'Outlook'                                                             AS report_type
     , a.cash_net_revenue
     , a.total_cash_collected
     , a.cash_gross_margin
     , a.credit_billings
     , a.credit_redeemed
     , a.credit_refunds
     , a.credit_billings - a.credit_redeemed - a.credit_refunds              AS net_credit_billings
     , a.activating_product_revenue
     , a.activating_product_orders
     , a.activating_product_units
     , a.repeat_product_revenue
     , a.repeat_product_orders
     , a.repeat_product_units
     , a.total_units_shipped
     , a.total_orders_shipped
     , a.media_spend
     , a.bop_vips
     , a.total_cancels
     , a.new_vips
     , a.new_leads
     , a.m1_vips_from_leads
     , a.eop_vips
     , a.activating_product_margin_percent
     , a.repeat_product_margin_percent
     , a.repeat_product_margin
     , a.activating_product_margin
     , IFF(a.bop_vips = 0, 0, a.repeat_product_orders / a.bop_vips)          AS merch_purchase_rate
     , IFF((a.new_vips) = 0, 0,
           a.media_spend / a.new_vips - a.activating_gross_margin_per_order) AS cpaam
     , a.vip_cash_percent
     , a.total_vip_on_date_cac                                               AS total_vips_cac
     , IFF(a.new_vips = 0, 0, a.activating_acquisition_margin / a.new_vips)  AS activating_acquisition_margin
     , a.product_order_billing_chargeback +
       a.product_order_credit_refund_amount                                  AS product_order_refunds
FROM _outlook a
         JOIN _store_month_version smv
              ON smv.month = a.month
                  AND smv.outlook_version_date = CAST(a.version_date AS VARCHAR(100));


