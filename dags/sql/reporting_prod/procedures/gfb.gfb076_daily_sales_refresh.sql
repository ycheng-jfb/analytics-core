SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, CURRENT_DATE()));
SET end_date = DATE_TRUNC(MONTH, CURRENT_DATE());


CREATE OR REPLACE TEMPORARY TABLE _daily_cash AS
SELECT (CASE
            WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
            WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
            WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
            WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
    END)                                                                                          AS business_unit
     , a.date
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
     , IFF(SUM(a.activating_unit_count + a.activating_product_order_reship_unit_count +
               a.activating_product_order_exchange_unit_count) = 0, 0,
           SUM(a.activating_product_order_landed_product_cost_amount +
               a.activating_product_order_exchange_product_cost_amount +
               a.activating_product_order_reship_product_cost_amount)
               / SUM(a.activating_unit_count + a.activating_product_order_reship_unit_count +
                     a.activating_product_order_exchange_unit_count))                             AS activating_auc
     , SUM(activating_product_gross_profit)                                                       AS activating_product_margin
     , IFF(SUM(a.nonactivating_unit_count) = 0, 0,
           SUM(a.nonactivating_product_gross_revenue - a.nonactivating_shipping_revenue) * 1.0 /
           SUM(a.nonactivating_unit_count))                                                       AS repeat_aur
     , IFF(SUM(a.nonactivating_unit_count + a.nonactivating_product_order_reship_unit_count +
               a.nonactivating_product_order_exchange_unit_count) = 0, 0,
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
FROM edw_prod.reporting.daily_cash_final_output a
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
       , DATE_TRUNC(MONTH, a.date)
       , a.date;

CREATE OR REPLACE TEMPORARY TABLE _media_spend AS
SELECT (CASE
            WHEN cac.store_brand ILIKE 'JUSTFAB' AND cac.store_region = 'NA' THEN 'JUSTFAB NA'
            WHEN cac.store_brand ILIKE 'JUSTFAB' AND cac.store_region = 'NA' THEN 'JUSTFAB NA'
            ELSE UPPER(cac.store_brand)
    END)                                                 AS business_unit
     , DATE_TRUNC(MONTH, cac.date)                       AS month
     , cac.date
     , SUM(cac.total_spend)                              AS media_spend
     , SUM(cac.bop_vips)                                 AS bop_vips
     , SUM(cac.total_cancels_on_date)                    AS total_cancels
     , SUM(cac.total_vips_on_date)                       AS new_vips
     , SUM(cac.primary_leads)                            AS new_leads
     , DIV0(media_spend, new_vips)                       AS total_vips_cac
     , SUM(cac.activating_vip_product_margin_pre_return) AS activating_acquisition_margin
     , SUM(cac.vips_from_leads_m1)                       AS m1_vips_from_leads
     , SUM(CASE
               WHEN cac.date = DATE_TRUNC(MONTH, cac.date)
                   THEN cac.bop_vips
               ELSE 0 END)
           + SUM(cac.total_vips_on_date)
    - SUM(cac.total_cancels_on_date)                     AS eop_vips
FROM reporting_media_prod.attribution.acquisition_unattributed_total_cac cac
         JOIN edw_prod.data_model.dim_store ds
              ON ds.store_id = cac.store_id
WHERE cac.store_brand ILIKE ANY ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS')
  AND cac.currency = 'USD'
  AND DATE_TRUNC(MONTH, cac.date) >= $start_date
  AND DATE_TRUNC(MONTH, cac.date) <= $end_date
GROUP BY (CASE
              WHEN cac.store_brand ILIKE 'JUSTFAB' AND cac.store_region = 'NA' THEN 'JUSTFAB NA'
              WHEN cac.store_brand ILIKE 'JUSTFAB' AND cac.store_region = 'NA' THEN 'JUSTFAB NA'
              ELSE UPPER(cac.store_brand)
    END),
         DATE_TRUNC(MONTH, cac.date),
         cac.date;

CREATE OR REPLACE TEMPORARY TABLE _forecast AS
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
     , 0                                                                           AS credit_redeemed_percent
FROM lake_view.sharepoint.jfb_adhoc_forecast_input a
WHERE a.business_unit IN ('JFNA', 'JFEU', 'SDNA', 'FKNA', 'SD', 'FK');


CREATE OR REPLACE TEMPORARY TABLE _actual AS
SELECT dc.business_unit
     , dc.month
     , dc.date
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
FROM _daily_cash dc
         LEFT JOIN _media_spend ms
                   ON ms.business_unit = dc.business_unit
                       AND ms.month = dc.month
                       AND dc.date = ms.date;



CREATE OR REPLACE TEMPORARY TABLE _forecast_cal AS
SELECT om.business_unit
     , om.month
     , om.last_month
     , om.ly_month
     , lm.bop_vips      AS lm_bop_vip
     , lm.total_cancels AS lm_cancels
     , ly.bop_vips      AS ly_bop_vip
     , ly.total_cancels AS ly_cancels
     , IFF(lm_bop_vip = 0 OR ly_bop_vip = 0, 0, cm.bop_vips * (
            lm_cancels / lm_bop_vip
        + ly_cancels / ly_bop_vip
    ) / 2)              AS cancels_fc
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
WHERE a.bu IN ('JFNA', 'JFEU', 'SDNA', 'FKNA')
  AND a.version = 'Budget'
  AND a.version_date = DATE_TRUNC('month', a.version_date)
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

CREATE OR REPLACE TEMPORARY TABLE _outlook AS
SELECT a.business_unit
     , a.month
     , a.date                            AS version_date
     , a.cash_net_revenue
     , a.cash_gross_revenue              AS total_cash_collected
     , a.cash_gross_margin_amount        AS cash_gross_margin
     , a.credit_billing                  AS credit_billings
     , a.credit_redeemed
     , a.credit_refund                   AS credit_refunds
     , a.net_unredeemed_credit_billing   AS net_credit_billings
     , a.activating_product_revenue
     , a.activating_order_count          AS activating_product_orders
     , a.activating_shipped_units        AS activating_product_units
     , a.repeat_product_revenue
     , a.repeat_order_count              AS repeat_product_orders
     , a.repeat_shipped_units            AS repeat_product_units
     , a.total_units_shipped
     , a.total_orders_shipped
     , a.media_spend
     , a.bop_vips
     , a.cancels                         AS total_cancels
     , a.new_vip                         AS new_vips
     , a.leads                           AS new_leads
     , a.m1_vip                          AS m1_vips_from_leads
     , a.eom_vip_count                   AS eop_vips
     , a.activating_gross_margin_percent AS activating_product_margin_percent
     , a.repeat_gross_margin_percent     AS repeat_product_margin_percent
     , a.activating_product_margin       AS activating_product_margin
     , a.repeat_product_margin           AS repeat_product_margin
     , (a.activating_cash_net_revenue * a.activating_gross_margin_percent) /
       a.activating_order_count          AS activating_gross_margin_per_order
     , a.vip_cash_percent
     , a.total_vip_on_date_cac
     , a.activating_acquisition_margin
     , a.credit_redeemed_percent
FROM reporting_prod.gfb.gfb055_01_outlook_metrics a
         LEFT JOIN _forecast_cal fc
                   ON fc.business_unit = a.business_unit
                       AND fc.month = a.month;


CREATE OR REPLACE TEMPORARY TABLE _forecast_store_month_version AS
SELECT f.business_unit
     , f.month
     , MAX(f.version_date) AS version_date
FROM _forecast f
GROUP BY f.business_unit
       , f.month;


CREATE OR REPLACE TEMPORARY TABLE _outlook_store_month_version AS
SELECT o.business_unit
     , o.month
     , MAX(o.version_date) AS version_date
FROM _outlook o
GROUP BY o.business_unit
       , o.month;


CREATE OR REPLACE TEMPORARY TABLE _actual_final AS
SELECT DISTINCT a.business_unit
              , a.month
              , a.date
              , 'Actual'                                                                              AS report_type
              , a.cash_net_revenue
              , SUM(a.cash_net_revenue)
                    OVER (PARTITION BY a.business_unit, DATE_TRUNC('month', a.date))                  AS monthly_net_cash_revenue
              , IFF(a.cash_net_revenue = 0, 0,
                    (a.cash_net_revenue / monthly_net_cash_revenue))                                  AS net_cash_revenue_distribution
              , a.total_cash_collected
              , a.cash_gross_margin
              , SUM(a.cash_gross_margin)
                    OVER (PARTITION BY a.business_unit, DATE_TRUNC('month', a.date))                  AS monthly_cash_gm
              , IFF(a.cash_gross_margin = 0, 0, a.cash_gross_margin / monthly_cash_gm)                AS cash_gm_distribution
              , a.credit_billings
              , SUM(a.credit_billings)
                    OVER (PARTITION BY a.business_unit, DATE_TRUNC('month', a.date))                  AS monthly_credit_billings_amount
              , IFF(a.credit_billings = 0, 0, a.credit_billings /
                                              monthly_credit_billings_amount)                         AS credit_billing_amount_distribution
              , a.credit_redeemed
              , a.credit_refunds
              , SUM(a.credit_refunds)
                    OVER (PARTITION BY a.business_unit, DATE_TRUNC('month', a.date))                  AS monthly_credit_refunds
              , IFF(a.credit_refunds = 0, 0, a.credit_refunds /
                                             monthly_credit_refunds)                                  AS credit_refund_distribution
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
              , SUM(a.media_spend)
                    OVER (PARTITION BY a.business_unit, DATE_TRUNC('month', a.date))                  AS monthly_media_spend
              , IFF(IFNULL(a.media_spend, 0) = 0, 0, a.media_spend /
                                                     monthly_media_spend)                             AS media_spend_distribution
              , a.bop_vips
              , a.total_cancels
              , SUM(a.total_cancels) OVER (PARTITION BY a.business_unit, DATE_TRUNC('month', a.date)) AS monthly_cancels
              , IFF(a.total_cancels = 0, 0, a.total_cancels / monthly_cancels)                        AS cancel_distribution
              , a.new_vips
              , SUM(a.new_vips)
                    OVER (PARTITION BY a.business_unit, DATE_TRUNC('month', a.date))                  AS monthly_new_vips
              , IFF(a.new_vips = 0, 0, a.new_vips / monthly_new_vips)                                 AS new_vip_distribution
              , a.new_leads
              , SUM(a.new_leads) OVER (PARTITION BY a.business_unit, DATE_TRUNC('month', a.date))     AS monthly_leads
              , IFF(a.new_leads = 0, 0, a.new_leads / monthly_leads)                                  AS leads_distribution
              , a.m1_vips_from_leads
              , a.eop_vips
              , a.activating_product_margin_percent
              , a.repeat_product_margin_percent
              , a.repeat_product_margin
              , a.activating_product_margin
              , IFF(a.bop_vips = 0, 0, a.repeat_product_orders / a.bop_vips)                          AS merch_purchase_rate
              , IFF((a.new_vips - a.activating_gross_margin_per_order) = 0, 0,
                    a.media_spend / a.new_vips - a.activating_gross_margin_per_order)                 AS cpaam
              , IFF(a.repeat_product_revenue = 0, 0,
                    (a.repeat_product_revenue - a.credit_redeemed) /
                    a.repeat_product_revenue)                                                         AS vip_cash_percent
              , a.total_vips_cac
              , IFF(a.new_vips = 0, 0, a.activating_acquisition_margin / a.new_vips)                  AS activating_acquisition_margin
              , a.cash_gross_margin - a.media_spend                                                   AS contribution_after_media
FROM _actual a;


CREATE OR REPLACE TEMPORARY TABLE _forecast_final AS
SELECT DISTINCT f.business_unit
              , f.month
              , 'Forecast'                                                                  AS report_type
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
              , f.repeat_product_orders_fc / f.bop_vips_fc                                  AS merch_purchase_rate
              , IFF((f.new_vips_fc - f.activating_gross_margin_per_order) = 0, 0,
                    f.media_spend_fc / f.new_vips_fc - f.activating_gross_margin_per_order) AS cpaam
              , IFF(f.repeat_product_revenue_fc = 0, 0,
                    (f.repeat_product_revenue_fc - f.credit_redeemed_fc) /
                    f.repeat_product_revenue_fc)                                            AS vip_cash_percent
              , IFF(f.new_vips_fc = 0, 0, f.media_spend_fc / f.new_vips_fc)                 AS total_vips_cac
              , f.activating_gross_margin_per_order                                         AS activating_acquisition_margin
              , f.credit_redeemed_percent
FROM _forecast f
         JOIN _forecast_store_month_version smv
              ON smv.business_unit = f.business_unit
                  AND smv.month = f.month
                  AND smv.version_date = f.version_date;


CREATE OR REPLACE TEMPORARY TABLE _outlook_final AS
SELECT DISTINCT a.business_unit
              , a.month
              , 'Outlook'                                                             AS report_type
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
              , IFF(a.bop_vips = 0, 0, a.repeat_product_orders / a.bop_vips)          AS merch_purchase_rate
              , IFF((a.new_vips - a.activating_gross_margin_per_order) = 0, 0,
                    a.media_spend / a.new_vips - a.activating_gross_margin_per_order) AS cpaam
              , a.vip_cash_percent
              , a.total_vip_on_date_cac                                               AS total_vips_cac
              , IFF(a.new_vips = 0, 0, a.activating_acquisition_margin / a.new_vips)  AS activating_acquisition_margin
              , a.activating_gross_margin_per_order
              , a.credit_redeemed_percent
FROM _outlook a
         JOIN _outlook_store_month_version smv
              ON smv.business_unit = a.business_unit
                  AND smv.month = a.month
                  AND smv.version_date = CAST(a.version_date AS VARCHAR(100));

CREATE OR REPLACE TEMPORARY TABLE _combine_forecast_outlook AS
SELECT COALESCE(oo.business_unit, ff.business_unit)                         AS business_unit
     , COALESCE(oo.month, ff.month)                                         AS month
     , LAST_DAY(COALESCE(oo.month, ff.month))                               AS last_day_of_month
     , COALESCE(oo.cash_net_revenue, ff.cash_net_revenue_fc)                AS cash_net_revenue
     , COALESCE(oo.credit_redeemed, ff.credit_redeemed_fc)                  AS credit_redeemed
     , COALESCE(oo.net_credit_billings, ff.net_credit_billings_fc)          AS net_credit_billings
     , COALESCE(oo.cash_gross_margin, ff.cash_gross_margin_fc)              AS cash_gross_margin
     , COALESCE(oo.credit_billings, ff.credit_billings_fc)                  AS billed_credit_cash_amount
     , COALESCE(oo.credit_refunds, ff.credit_refunds_fc)                    AS credit_refunds
     , COALESCE(oo.media_spend, ff.media_spend_fc)                          AS media_spends
     , COALESCE(oo.m1_vips_from_leads, ff.m1_vips_from_leads_fc)            AS m1_vips_from_leads
     , COALESCE(oo.new_vips, ff.new_vips_fc)                                AS new_vips
     , COALESCE(oo.total_cancels, ff.total_cancels_fc)                      AS total_cancels
     , COALESCE(oo.new_leads, ff.new_leads_fc)                              AS leads
     , COALESCE(oo.activating_product_revenue,
                ff.activating_product_revenue_fc)                           AS activating_product_revenue
     , COALESCE(oo.activating_product_orders,
                ff.activating_product_orders_fc)                            AS activating_product_orders
     , COALESCE(oo.activating_product_units,
                ff.activating_product_units_fc)                             AS activating_product_units
     , COALESCE(oo.activating_product_margin, ff.activating_product_margin) AS activating_product_margin
     , COALESCE(oo.repeat_product_revenue, ff.repeat_product_revenue_fc)    AS repeat_product_revenue
     , COALESCE(oo.repeat_product_orders, ff.repeat_product_orders_fc)      AS repeat_product_orders
     , COALESCE(oo.repeat_product_units, ff.repeat_product_units_fc)        AS repeat_product_units
     , COALESCE(oo.repeat_product_margin, ff.repeat_product_margin)         AS repeat_product_margin
     , COALESCE((oo.repeat_product_revenue - oo.credit_redeemed),
                (ff.repeat_product_revenue_fc - ff.credit_redeemed_fc))     AS vip_cash
     , COALESCE(oo.vip_cash_percent, ff.vip_cash_percent)                   AS vip_cash_percent
     , COALESCE(oo.credit_redeemed_percent, ff.credit_redeemed_percent)     AS credit_redeemed_percent
     , COALESCE((oo.merch_purchase_rate),
                (ff.merch_purchase_rate))                                   AS merch_purchase_rate
     , COALESCE(oo.cpaam, ff.cpaam)                                         AS cpaam
     , COALESCE(oo.total_vips_cac, ff.total_vips_cac)                       AS total_vips_cac
     , COALESCE(oo.bop_vips, ff.bop_vips_fc)                                AS bop_vips
     , COALESCE(oo.eop_vips, ff.eom_vips_fc)                                AS eop_vips
FROM _forecast_final ff
         FULL JOIN _outlook_final oo
                   ON ff.business_unit = oo.business_unit
                       AND ff.month = oo.month;


CREATE OR REPLACE TEMPORARY TABLE _actual_distribution AS
SELECT COALESCE(af.business_unit, pm.business_unit, py.business_unit)                 AS business_unit
     , af.date
     , DATE_TRUNC('month', af.date)                                                   AS month_date
     , CASE
           WHEN af.date = '2024-02-29' AND pm.date IN ('2024-01-30', '2024-01-29') THEN 'Remove'
           ELSE 'keep' END                                                            AS leap_year_flag
     , af.cash_net_revenue
     , IFF(py.net_cash_revenue_distribution = 0, pm.net_cash_revenue_distribution, (pm.net_cash_revenue_distribution +
                                                                                    py.net_cash_revenue_distribution) /
                                                                                   2) AS cash_net_revenue_distribution
     , IFF(py.cash_gm_distribution = 0, pm.cash_gm_distribution, (pm.cash_gm_distribution + py.cash_gm_distribution) /
                                                                 2)                   AS cash_gm_distribution
     , IFF(py.credit_billing_amount_distribution = 0, pm.credit_billing_amount_distribution,
           (py.credit_billing_amount_distribution + pm.credit_billing_amount_distribution) /
           2)                                                                         AS billings_distribution
     , IFF(py.credit_refund_distribution = 0, pm.credit_refund_distribution,
           (pm.credit_refund_distribution + py.credit_refund_distribution) /
           2)                                                                         AS credit_refund_distribution
     , IFF(IFNULL(py.media_spend_distribution, 0) = 0, pm.media_spend_distribution,
           (pm.media_spend_distribution + py.media_spend_distribution) /
           2)                                                                         AS media_spend_distribution
     , py.media_spend_distribution                                                    AS previous_year
     , pm.media_spend_distribution                                                    AS previous_month
     , IFF(py.new_vip_distribution = 0, pm.new_vip_distribution, (pm.new_vip_distribution + py.new_vip_distribution) /
                                                                 2)                   AS new_vips_distribution
     , IFF(py.cancel_distribution = 0, pm.cancel_distribution, (pm.cancel_distribution + py.cancel_distribution) /
                                                               2)                     AS cancel_distribution
     , IFF(py.leads_distribution = 0, pm.leads_distribution, (py.leads_distribution + pm.leads_distribution) /
                                                             2)                       AS lead_distribution
FROM _actual_final af
         LEFT JOIN _actual_final pm
                   ON af.business_unit = pm.business_unit
                       AND af.date = DATEADD('month', 1, pm.date)
         LEFT JOIN _actual_final py
                   ON af.business_unit = py.business_unit
                       AND af.date = DATEADD('year', 1, py.date)
WHERE leap_year_flag = 'keep';


CREATE OR REPLACE TEMPORARY TABLE _outlook_forecast_final AS
SELECT DISTINCT COALESCE(ad.business_unit, cfo.business_unit)                   AS business_unit
              , 'Forecast'                                                      AS report_type
              , COALESCE(ad.month_date, cfo.month)                              AS month_date
              , DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS last_day
              , ad.date
              , cfo.cash_net_revenue
              , cfo.cash_net_revenue /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS cash_net_revenue_fc
              , cfo.cash_gross_margin
              , cfo.cash_gross_margin /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS cash_gm_fc
              , cfo.billed_credit_cash_amount
              , IFF(DATE_PART('day', ad.date) >= 6 AND DATE_PART('day', ad.date) <= 13,
                    cfo.billed_credit_cash_amount / 8, 0)                       AS billed_credit_cash_fc
              , cfo.credit_refunds
              , cfo.credit_refunds /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS credit_refund_fc
              , cfo.media_spends
              , cfo.media_spends /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS media_spends_fc
              , cfo.new_vips
              , cfo.new_vips /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS new_vips_fc
              , cfo.total_cancels
              , cfo.total_cancels /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS cancels_fc
              , cfo.leads
              , cfo.leads /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS leads_fc
              , cfo.credit_redeemed /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS credit_redeemed_fc
              , billed_credit_cash_fc - credit_refund_fc - credit_redeemed_fc   AS net_credit_billings_fc
              , cfo.activating_product_revenue /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS activating_product_revenue_fc
              , cfo.activating_product_units /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS activating_product_units_fc
              , cfo.activating_product_orders /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS activating_product_orders_fc
              , cfo.activating_product_margin /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS activating_product_margin_fc
              , cfo.activating_product_revenue / cfo.activating_product_orders  AS activating_aov_fc
              , cfo.activating_product_revenue / cfo.activating_product_units   AS activating_upt_fc
              , cfo.activating_product_margin / cfo.activating_product_revenue  AS activating_product_margin_pct_fc
              , cfo.repeat_product_revenue /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS repeat_product_revenue_fc
              , cfo.repeat_product_units /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS repeat_product_units_fc
              , cfo.repeat_product_orders /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS repeat_product_orders_fc
              , cfo.repeat_product_margin /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, cfo.month)))  AS repeat_product_margin_fc
              , cfo.repeat_product_revenue / cfo.repeat_product_orders          AS repeat_aov_fc
              , cfo.repeat_product_units / cfo.repeat_product_orders            AS repeat_upt_fc
              , cfo.repeat_product_margin / cfo.repeat_product_revenue          AS repeat_product_margin_pct_fc
              , cfo.credit_redeemed_percent
              , cfo.merch_purchase_rate
              , cfo.cpaam
              , cfo.total_vips_cac
              , cfo.bop_vips
              , cfo.eop_vips
              , cfo.m1_vips_from_leads / TO_NUMBER(RIGHT(last_day_of_month, 2)) AS m1_vips_from_leads_fc
              , cash_gm_fc - media_spends_fc                                    AS contribution_after_media_fc
              , media_spends_fc / leads_fc                                      AS cpl
              , cancels_fc / (new_vips_fc / cfo.bop_vips)                       AS attrition
FROM _actual_distribution ad
         LEFT JOIN _combine_forecast_outlook cfo
                   ON ad.business_unit = cfo.business_unit
                       AND ad.month_date = cfo.month
ORDER BY date DESC;

CREATE OR REPLACE TEMPORARY TABLE _budget_final AS
SELECT DISTINCT COALESCE(ad.business_unit, b.business_unit)                            AS business_unit
              , 'Budget'                                                               AS report_type
              , DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS last_day
              , COALESCE(ad.month_date, b.month)                                       AS month_date
              , ad.date
              , b.cash_net_revenue_bt                                                  AS cash_net_revenue
              , b.cash_net_revenue_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS cash_net_revenue_bt
              , b.cash_gross_margin_bt
              , b.cash_gross_margin_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS cash_gm_bt
              , b.credit_billings_bt
              , IFF(DATE_PART('day', ad.date) >= 6 AND DATE_PART('day', ad.date) <= 13, b.credit_billings_bt / 8,
                    0)                                                                 AS billed_credit_cash_bt
              , b.credit_refunds_bt
              , b.credit_refunds_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS credit_refund_bt
              , b.media_spend_bt
              , b.media_spend_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS media_spends_bt
              , b.new_vips_bt                                                          AS new_vips
              , b.new_vips_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS new_vips_bt
              , b.total_cancels_bt
              , b.total_cancels_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS cancels_bt
              , b.new_leads_bt
              , b.new_leads_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS leads_bt
              , b.credit_redeemed_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS credit_redeemed_bt
              , net_credit_billings_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS net_credit_billings_bt
              , b.activating_product_revenue_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS activating_product_revenue_bt
              , b.activating_product_units_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS activating_product_units_bt
              , b.activating_product_orders_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS activating_product_orders_bt
              , b.activating_product_margin /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS activating_product_margin_bt
              , b.activating_product_revenue_bt / b.activating_product_orders_bt       AS activating_aov_bt
              , b.activating_product_revenue_bt / b.activating_product_units_bt        AS activating_upt_bt
              , b.activating_product_margin / b.activating_product_revenue_bt          AS activating_product_margin_pct_bt
              , b.repeat_product_revenue_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS repeat_product_revenue_bt
              , b.repeat_product_units_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS repeat_product_units_bt
              , b.repeat_product_orders_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS repeat_product_orders_bt
              , b.repeat_product_margin /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS repeat_product_margin_bt
              , b.repeat_product_revenue_bt / b.repeat_product_orders_bt               AS repeat_aov_bt
              , b.repeat_product_units_bt / b.repeat_product_orders_bt                 AS repeat_upt_bt
              , b.repeat_product_margin / b.repeat_product_revenue_bt                  AS repeat_product_margin_pct_bt
              , 0                                                                      AS credit_redeemed_percent
              , b.repeat_product_orders_bt / b.bop_vips_bt                             AS merch_purchase_rate
              , b.media_spend_bt / b.new_vips_bt - b.activating_gross_margin_per_order AS cpaam
              , (b.repeat_product_revenue_bt - b.credit_redeemed_bt) /
                b.repeat_product_revenue_bt                                            AS vip_cash_percent
              , b.media_spend_bt /
                b.new_vips_bt                                                          AS total_vips_cac
              , b.bop_vips_bt
              , b.eom_vips_bt
              , b.m1_vips_from_leads_bt /
                DATE_PART('day', LAST_DAY(COALESCE(ad.month_date, b.month)))           AS m1_vips_from_leads_bt
              , b.cash_gross_margin_bt - b.media_spend_bt                              AS contribution_after_media_bt
              , b.media_spend_bt / b.new_leads_bt                                      AS cpl
              , b.total_cancels_bt / (b.new_vips_bt / b.bop_vips_bt)                   AS attrition
              , b.total_orders_shipped_bt
              , b.total_units_shipped_bt
FROM _actual_distribution ad
         LEFT JOIN _budget b
                   ON ad.business_unit = b.business_unit
                       AND ad.month_date = b.month;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb076_daily_sales_refresh AS
SELECT a.business_unit
     , a.month
     , a.date
     , 'Actual'                                                                       AS report_type
     , a.cash_net_revenue
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
     , IFF(a.new_vips - a.activating_gross_margin_per_order = 0, 0,
           a.media_spend / a.new_vips - a.activating_gross_margin_per_order)          AS cpaam
     , IFF(a.repeat_product_revenue = 0, 0,
           (a.repeat_product_revenue - a.credit_redeemed) / a.repeat_product_revenue) AS vip_cash_percent
     , a.total_vips_cac
     , IFF(a.new_vips = 0, 0, a.activating_acquisition_margin / a.new_vips)           AS activating_acquisition_margin
FROM _actual a
UNION
SELECT a.business_unit
     , DATEADD(YEAR, 1, a.month)                                                      AS month
     , DATEADD('year', 1, a.date)
     , 'Prior Year'                                                                   AS report_type
     , a.cash_net_revenue
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
     , IFF(a.new_vips - a.activating_gross_margin_per_order = 0, 0,
           a.media_spend / a.new_vips - a.activating_gross_margin_per_order)          AS cpaam
     , IFF(a.repeat_product_revenue = 0, 0,
           (a.repeat_product_revenue - a.credit_redeemed) / a.repeat_product_revenue) AS vip_cash_percent
     , IFF(a.new_vips = 0, 0, a.media_spend / a.new_vips)                             AS total_vips_cac
     , a.activating_gross_margin_per_order                                            AS activating_acquisition_margin
FROM _actual a
WHERE a.date >= DATEADD('year', -1, DATE_TRUNC('year', CURRENT_DATE()))
  AND a.date < DATE_TRUNC('year', CURRENT_DATE())
UNION
SELECT d.business_unit
     , d.month_date
     , d.date
     , 'Forecast'
     , d.cash_net_revenue_fc
     , d.cash_gm_fc
     , d.billed_credit_cash_fc
     , d.credit_redeemed_fc
     , d.credit_refund_fc
     , d.net_credit_billings_fc
     , d.activating_product_revenue_fc
     , d.activating_product_orders_fc
     , d.activating_product_units_fc
     , d.repeat_product_revenue_fc
     , d.repeat_product_orders_fc
     , d.repeat_product_units_fc
     , d.activating_product_units_fc + d.repeat_product_units_fc
     , d.activating_product_orders_fc + d.repeat_product_orders_fc
     , d.media_spends_fc
     , d.bop_vips
     , d.cancels_fc
     , d.new_vips_fc
     , d.leads_fc
     , d.m1_vips_from_leads_fc
     , d.eop_vips
     , d.activating_product_margin_pct_fc
     , d.repeat_product_margin_pct_fc
     , d.repeat_product_margin_fc
     , d.activating_product_margin_fc
     , d.merch_purchase_rate
     , d.cpaam
     , IFF(d.repeat_product_revenue_fc = 0, 0,
           (d.repeat_product_revenue_fc - d.credit_redeemed_fc) / d.repeat_product_revenue_fc)
     , d.total_vips_cac
     , IFF(d.activating_product_orders_fc = 0, 0, d.activating_product_margin_fc / d.activating_product_orders_fc)

FROM _outlook_forecast_final d
UNION
SELECT d.business_unit
     , d.month_date
     , d.date
     , 'Budget'
     , d.cash_net_revenue_bt
     , d.cash_gm_bt
     , d.billed_credit_cash_bt
     , d.credit_redeemed_bt
     , d.credit_refund_bt
     , d.net_credit_billings_bt
     , d.activating_product_revenue_bt
     , d.activating_product_orders_bt
     , d.activating_product_units_bt
     , d.repeat_product_revenue_bt
     , d.repeat_product_orders_bt
     , d.repeat_product_units_bt
     , d.activating_product_units_bt + d.repeat_product_units_bt
     , d.activating_product_orders_bt + d.repeat_product_orders_bt
     , d.media_spends_bt
     , d.bop_vips_bt
     , d.cancels_bt
     , d.new_vips_bt
     , d.leads_bt
     , d.m1_vips_from_leads_bt
     , d.eom_vips_bt
     , d.activating_product_margin_pct_bt
     , d.repeat_product_margin_pct_bt
     , d.repeat_product_margin_bt
     , d.activating_product_margin_bt
     , d.merch_purchase_rate
     , d.cpaam
     , IFF(d.repeat_product_revenue_bt = 0, 0,
           (d.repeat_product_revenue_bt - d.credit_redeemed_bt) / d.repeat_product_revenue_bt)
     , d.total_vips_cac
     , IFF(d.activating_product_orders_bt = 0, 0, d.activating_product_margin_bt / d.activating_product_orders_bt)
FROM _budget_final d;
