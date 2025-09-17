--SET current_month = DATE_TRUNC(MONTH, DATEADD(DAY, -1, CURRENT_DATE()));
--SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, CURRENT_DATE()));
--SET end_date = DATEADD(DAY, -1, CURRENT_DATE());

SET current_month = DATE_TRUNC(MONTH, DATEADD(DAY, -1, '2015-07-01'));
SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, '2015-07-01'));
SET end_date = DATEADD(DAY, -1, '2015-07-01');


CREATE OR REPLACE TEMPORARY TABLE _cpa AS
SELECT (CASE
            WHEN a.store_brand = 'JustFab' AND a.store_region = 'NA' THEN 'JUSTFAB NA'
            WHEN a.store_brand = 'JustFab' AND a.store_region = 'EU' THEN 'JUSTFAB EU'
            WHEN a.store_brand = 'ShoeDazzle' THEN 'SHOEDAZZLE'
            WHEN a.store_brand = 'FabKids' THEN 'FABKIDS'
    END)                                             AS business_unit
     , DATE_TRUNC(MONTH, a.date)                     AS month
     , SUM(total_spend) / SUM(primary_leads)         AS cpl
     , SUM(vips_from_leads_m1) / SUM(primary_leads)  AS m1_lead_to_vip_percent
     , SUM(activating_vip_product_margin_pre_return) AS activating_acquisition_margin
     , SUM(total_spend) / SUM(total_vips_on_date)    AS total_vip_on_date_cac
FROM reporting_media_prod.attribution.daily_acquisition_metrics_cac a
WHERE a.store_brand IN ('JustFab', 'ShoeDazzle', 'FabKids')
  AND a.currency = 'USD'
  and a.date <= $end_date
GROUP BY (CASE
              WHEN a.store_brand = 'JustFab' AND a.store_region = 'NA' THEN 'JUSTFAB NA'
              WHEN a.store_brand = 'JustFab' AND a.store_region = 'EU' THEN 'JUSTFAB EU'
              WHEN a.store_brand = 'ShoeDazzle' THEN 'SHOEDAZZLE'
              WHEN a.store_brand = 'FabKids' THEN 'FABKIDS'
    END)
       , DATE_TRUNC(MONTH, a.date);


CREATE OR REPLACE TEMPORARY TABLE _daily_cash_ly AS
SELECT (CASE
            WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
            WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
            WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
            WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
    END)                                          AS business_unit
     , DATE_TRUNC(MONTH, a.date)                  AS month_ly
     , DATEADD(YEAR, 1, month_ly)                 AS month_ty
     , SUM(a.activating_product_net_revenue)      AS activating_gaap_net_revenue
     , SUM(a.activating_product_gross_revenue)    AS activating_gaap_gross_revenue
     , SUM(a.nonactivating_product_net_revenue)   AS repeat_gaap_net_revenue
     , SUM(a.nonactivating_product_gross_revenue) AS repeat_gaap_gross_revenue
FROM edw_prod.reporting.daily_cash_final_output a
WHERE a.report_mapping IN ('JF-TREV-NA', 'JF-TREV-EU', 'SD-TREV-NA', 'FK-TREV-NA')
  AND a.currency_type = 'USD'
  AND a.date_object = 'placed'
  AND DATE_TRUNC(MONTH, a.date) <= DATEADD(YEAR, -1, $current_month)
GROUP BY (CASE
              WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
              WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
              WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
              WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
    END)
       , DATE_TRUNC(MONTH, a.date)
       , DATEADD(YEAR, 1, month_ly);


CREATE OR REPLACE TEMPORARY TABLE _max_month_date AS
SELECT (CASE
            WHEN a.store_brand = 'JustFab' AND a.store_region = 'NA' THEN 'JUSTFAB NA'
            WHEN a.store_brand = 'JustFab' AND a.store_region = 'EU' THEN 'JUSTFAB EU'
            WHEN a.store_brand = 'ShoeDazzle' THEN 'SHOEDAZZLE'
            WHEN a.store_brand = 'FabKids' THEN 'FABKIDS'
    END)                         AS business_unit
     , DATE_TRUNC(MONTH, a.date) AS month
     , MAX(a.date)               AS max_date
FROM reporting_media_prod.attribution.daily_acquisition_metrics_cac a
WHERE a.store_brand IN ('JustFab', 'ShoeDazzle', 'FabKids')
  AND a.currency = 'USD'
  AND DATE_TRUNC('month', a.date) <= $current_month
GROUP BY (CASE
              WHEN a.store_brand = 'JustFab' AND a.store_region = 'NA' THEN 'JUSTFAB NA'
              WHEN a.store_brand = 'JustFab' AND a.store_region = 'EU' THEN 'JUSTFAB EU'
              WHEN a.store_brand = 'ShoeDazzle' THEN 'SHOEDAZZLE'
              WHEN a.store_brand = 'FabKids' THEN 'FABKIDS'
    END)
       , DATE_TRUNC(MONTH, a.date);

CREATE OR REPLACE TEMPORARY TABLE _lead_to_vip AS
SELECT (CASE
            WHEN a.store_brand = 'JustFab' AND a.store_region = 'NA' THEN 'JUSTFAB NA'
            WHEN a.store_brand = 'JustFab' AND a.store_region = 'EU' THEN 'JUSTFAB EU'
            WHEN a.store_brand = 'ShoeDazzle' THEN 'SHOEDAZZLE'
            WHEN a.store_brand = 'FabKids' THEN 'FABKIDS'
    END)                                                             AS business_unit
     , DATE_TRUNC(MONTH, a.date)                                     AS month
     , SUM(total_cancels_on_date)                                    AS total_cancels
     , SUM(CASE WHEN a.date = mmd.max_date THEN bop_vips ELSE 0 END) AS bop_vips
FROM reporting_media_prod.attribution.daily_acquisition_metrics_cac a
         LEFT JOIN _max_month_date mmd
                   ON mmd.business_unit = (CASE
                                               WHEN a.store_brand = 'JustFab' AND a.store_region = 'NA'
                                                   THEN 'JUSTFAB NA'
                                               WHEN a.store_brand = 'JustFab' AND a.store_region = 'EU'
                                                   THEN 'JUSTFAB EU'
                                               WHEN a.store_brand = 'ShoeDazzle' THEN 'SHOEDAZZLE'
                                               WHEN a.store_brand = 'FabKids' THEN 'FABKIDS'
                       END)
                       AND mmd.month = DATE_TRUNC('month', a.date)
WHERE a.store_brand IN ('JustFab', 'ShoeDazzle', 'FabKids')
  AND a.currency = 'USD'
  AND DATE_TRUNC('month', a.date) <= $current_month
GROUP BY (CASE
              WHEN a.store_brand = 'JustFab' AND a.store_region = 'NA' THEN 'JUSTFAB NA'
              WHEN a.store_brand = 'JustFab' AND a.store_region = 'EU' THEN 'JUSTFAB EU'
              WHEN a.store_brand = 'ShoeDazzle' THEN 'SHOEDAZZLE'
              WHEN a.store_brand = 'FabKids' THEN 'FABKIDS'
    END)
       , DATE_TRUNC(MONTH, a.date);


CREATE OR REPLACE TEMPORARY TABLE _daily_cash_rr AS
SELECT DISTINCT (CASE
                     WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
                     WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
                     WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
                     WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
    END)                                                           AS business_unit
              , DATE_TRUNC(MONTH, a.date)                          AS month
              , (COALESCE(refund_cash_credit_redeemed_amount, 0)
    * (COALESCE(refund_cash_credit_redeemed_amount_month_tot_ly, 0) /
       NULLIFZERO(refund_cash_credit_redeemed_amount_mtd_ly)))
    + (COALESCE(refund_cash_credit_redeemed_amount_mtd, 0)
        * (COALESCE(refund_cash_credit_redeemed_amount_month_tot_ly, 0) /
           NULLIFZERO(refund_cash_credit_redeemed_amount_mtd_ly))) AS refund_credit_redeemed_amount
              , (COALESCE(refund_cash_credit_issued_amount, 0)
    * (COALESCE(refund_cash_credit_issued_amount_month_tot_ly, 0) /
       NULLIFZERO(refund_cash_credit_issued_amount_mtd_ly)))
    + (COALESCE(refund_cash_credit_issued_amount_mtd, 0)
        * (COALESCE(refund_cash_credit_issued_amount_month_tot_ly, 0) /
           NULLIFZERO(refund_cash_credit_issued_amount_mtd_ly)))   AS refund_as_credit_amount
FROM edw_prod.reporting.daily_cash_final_output a
         LEFT JOIN (SELECT report_mapping,
                           MAX(date) OVER (PARTITION BY DATE_TRUNC('month', date),report_mapping) AS last_day_of_month
                    FROM edw_prod.reporting.daily_cash_final_output
                    WHERE report_mapping IN ('JF-TREV-NA', 'JF-TREV-EU', 'SD-TREV-NA', 'FK-TREV-NA')
                      AND currency_type = 'USD'
                      AND date_object = 'placed'
                      and date <= $end_date) mp
                   ON a.report_mapping = mp.report_mapping
                       AND DATE_TRUNC(MONTH, a.date) = DATE_TRUNC(MONTH, mp.last_day_of_month)
WHERE a.report_mapping IN ('JF-TREV-NA', 'JF-TREV-EU', 'SD-TREV-NA', 'FK-TREV-NA')
  AND a.currency_type = 'USD'
  AND a.date_object = 'placed'
  AND a.date = mp.last_day_of_month;


CREATE OR REPLACE TEMPORARY TABLE _net_bop_vip AS
SELECT (CASE
            WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
            WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
            WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
            WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
    END)                                                                             AS business_unit
     , DATE_TRUNC(MONTH, a.date)                                                     AS month
     , SUM(CASE
               WHEN a.date = DATE_TRUNC(MONTH, a.date)
                   THEN a.bop_vips END)                                              AS bop_vip
     , SUM(IFNULL(a.skip_count, 0))                                                  AS skips
     , SUM(CASE
               WHEN DAYOFMONTH(a.date) < 6
                   THEN a.merch_purchase_count
               ELSE 0 END)                                                           AS merch_purchase_count_first_five_days
     , merch_purchase_count_first_five_days * 1.0 / SUM(CASE
                                                            WHEN a.date = DATE_TRUNC(MONTH, a.date)
                                                                THEN a.bop_vips END) AS merch_purchase_rate_first_five_days
     , skips * 1.0 / bop_vip                                                         AS skip_rate
     , bop_vip * merch_purchase_rate_first_five_days                                 AS first_5_days_purchased_vip
     , bop_vip - skips - first_5_days_purchased_vip                                  AS net_bop_vip
FROM edw_prod.reporting.daily_cash_final_output a
WHERE a.report_mapping IN ('JF-TREV-NA', 'JF-TREV-EU', 'SD-TREV-NA', 'FK-TREV-NA')
  AND a.currency_type = 'USD'
  AND a.date_object = 'placed'
  AND a.date >= DATEADD(MONTH, -2, $current_month)
GROUP BY (CASE
              WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
              WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
              WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
              WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
    END)
       , DATE_TRUNC(MONTH, a.date)
HAVING bop_vip > 0;


CREATE OR REPLACE TEMPORARY TABLE _credit_cancel_actual AS
SELECT main.business_unit
     , main.day_of_month
     , SUM(CASE
               WHEN DATE_TRUNC(MONTH, actual.date) = DATEADD(MONTH, -2, $current_month)
                   THEN actual.membership_credit_refunds_chargebacks
               ELSE 0 END) AS membership_credit_refunds_chargebacks_pp
     , AVG(CASE
               WHEN DATE_TRUNC(MONTH, actual.date) <= DATEADD(MONTH, -1, $current_month) AND
                    DATE_TRUNC(MONTH, actual.date) >= DATEADD(MONTH, -6, $current_month)
                   THEN actual.membership_credit_refunds_chargebacks
               ELSE 0 END) AS membership_credit_refunds_chargebacks_p
     , SUM(CASE
               WHEN DATE_TRUNC(MONTH, actual.date) = $current_month
                   THEN actual.membership_credit_refunds_chargebacks
               ELSE 0 END) AS membership_credit_refunds_chargebacks
FROM (
         SELECT DISTINCT (CASE
                              WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
                              WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
                              WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
                              WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
             END)                        AS business_unit
                       , dd.day_of_month AS day_of_month
         FROM edw_prod.reporting.daily_cash_final_output a
                  JOIN edw_prod.data_model_jfb.dim_date dd
                       ON dd.month_date = $current_month
         WHERE a.report_mapping IN ('JF-TREV-NA', 'JF-TREV-EU', 'SD-TREV-NA', 'FK-TREV-NA')
           AND a.currency_type = 'USD'
           AND a.date_object = 'placed'
           AND a.date >= $current_month
     ) main
         LEFT JOIN
     (
         SELECT (CASE
                     WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
                     WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
                     WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
                     WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
             END)                                             AS business_unit
              , a.date
              , DAYOFMONTH(a.date)                            AS month_day
              , a.billed_credit_cash_refund_chargeback_amount AS membership_credit_refunds_chargebacks
         FROM edw_prod.reporting.daily_cash_final_output a
         WHERE a.report_mapping IN ('JF-TREV-NA', 'JF-TREV-EU', 'SD-TREV-NA', 'FK-TREV-NA')
           AND a.currency_type = 'USD'
           AND a.date_object = 'placed'
           AND a.date >= DATEADD(MONTH, -6, $current_month)
     ) AS actual ON actual.business_unit = main.business_unit
         AND actual.month_day = main.day_of_month
GROUP BY main.business_unit
       , main.day_of_month;


CREATE OR REPLACE TEMPORARY TABLE _current_month_credit_cancel AS
SELECT cca.business_unit
     , SUM(CASE
               WHEN cca.day_of_month >= DAYOFMONTH(CURRENT_DATE())
                   THEN (cca.membership_credit_refunds_chargebacks_p) *
                        yoy.net_bop_vip_yoy
               ELSE cca.membership_credit_refunds_chargebacks END) AS membership_credit_refunds_chargebacks
     , SUM(CASE
               WHEN cca.day_of_month >= DAYOFMONTH(CURRENT_DATE())
                   THEN (cca.membership_credit_refunds_chargebacks_p) *
                        yoy.net_bop_vip_yoy
               ELSE cca.membership_credit_refunds_chargebacks END) * 1.0 /
       49.95                                                       AS membership_credit_refunds_chargebacks_count
FROM _credit_cancel_actual cca
         JOIN
     (
         SELECT nbv.business_unit
              , SUM(CASE
                        WHEN nbv.month = $current_month
                            THEN nbv.net_bop_vip
                        ELSE 0 END)                                                                  AS current_month_net_bop_vip
              , SUM(CASE
                        WHEN nbv.month = DATEADD(MONTH, -1, $current_month)
                            THEN nbv.net_bop_vip
                        ELSE 0 END)                                                                  AS p_month_net_bop_vip
              , SUM(CASE
                        WHEN nbv.month = DATEADD(MONTH, -2, $current_month)
                            THEN nbv.net_bop_vip
                        ELSE 0 END)                                                                  AS pp_month_net_bop_vip
              , current_month_net_bop_vip * 1.0 / ((p_month_net_bop_vip + pp_month_net_bop_vip) / 2) AS net_bop_vip_yoy
         FROM _net_bop_vip nbv
         GROUP BY nbv.business_unit
     ) yoy ON yoy.business_unit = cca.business_unit
GROUP BY cca.business_unit;


CREATE OR REPLACE TEMPORARY TABLE _outlook_gsheet AS
SELECT (CASE
            WHEN a.business_unit = 'JFNA' THEN 'JUSTFAB NA'
            WHEN a.business_unit = 'JFEU' THEN 'JUSTFAB EU'
            WHEN a.business_unit = 'SDNA' THEN 'SHOEDAZZLE'
            WHEN a.business_unit = 'FKNA' THEN 'FABKIDS'
    END)                                                                             AS business_unit
     , a.outlook_month
     , a.date
     , a.media_spend
     , a.vip_cpa
     , b.activating_aov_incl_shipping
     , b.repeat_order_count
     , b.repeat_aov_incl_shipping
     , b.activating_gross_margin_percent
     , b.repeat_gross_margin_percent
     , b.activating_upt
     , b.repeat_upt
     , COALESCE(a.cancels, ltv.total_cancels)                                        AS cancels
     , a.credit_billings * 1.0 / 49.95                                               AS credit_billing_count
     , credit_billing_count * 1.0 / ltv.bop_vips                                     AS credit_billed_percent
     , rp.repeat_credit_redemption_amount * 1.0 / a.credit_billings                  AS credit_redeemed_percent
     , cmcc.membership_credit_refunds_chargebacks_count * 1.0 / credit_billing_count AS credit_cancelled_percent
     , b.activating_product_margin_percent
     , b.repeat_product_margin_percent
     , b.activating_discount_percent
     , b.repeat_discount_percent
     , b.repeat_product_margin
     , b.activating_product_margin
FROM lake_view.sharepoint.jfb_outlook_input a
         LEFT JOIN reporting_prod.gfb.gfb055_02_outlook_gsheet_his b
                   ON UPPER(CASE
                                WHEN UPPER(b.business_unit) = 'JUSTFAB'
                                    THEN b.business_unit || ' ' || b.region
                                ELSE b.business_unit END) = (CASE
                                                                 WHEN a.business_unit = 'JFNA' THEN 'JUSTFAB NA'
                                                                 WHEN a.business_unit = 'JFEU' THEN 'JUSTFAB EU'
                                                                 WHEN a.business_unit = 'SDNA' THEN 'SHOEDAZZLE'
                                                                 WHEN a.business_unit = 'FKNA' THEN 'FABKIDS'
                       END)
                       AND b.month_date = a.outlook_month
                       AND b.updated_date = DATE_TRUNC(WEEK, a.date)
         LEFT JOIN _lead_to_vip ltv
                   ON ltv.business_unit = (CASE
                                               WHEN a.business_unit = 'JFNA' THEN 'JUSTFAB NA'
                                               WHEN a.business_unit = 'JFEU' THEN 'JUSTFAB EU'
                                               WHEN a.business_unit = 'SDNA' THEN 'SHOEDAZZLE'
                                               WHEN a.business_unit = 'FKNA' THEN 'FABKIDS'
                       END)
                       AND ltv.month = a.outlook_month
         LEFT JOIN
     (
         SELECT UPPER(CASE
                          WHEN UPPER(a.business_unit) = 'JUSTFAB'
                              THEN a.business_unit || ' ' || a.region
                          ELSE a.business_unit END)               AS business_unit
              , DATE_TRUNC(MONTH, a.date)                         AS month
              , SUM(a.gaap_revenue * a.credit_redemption_percent) AS repeat_credit_redemption_amount
         FROM lake_view.sharepoint.gfb_daily_sale_standup_repeat_tsos a
         WHERE a.source = 'Forecast/Actuals'
         GROUP BY UPPER(CASE
                            WHEN UPPER(a.business_unit) = 'JUSTFAB'
                                THEN a.business_unit || ' ' || a.region
                            ELSE a.business_unit END)
                , DATE_TRUNC(MONTH, a.date)
     ) rp ON rp.business_unit = (CASE
                                     WHEN a.business_unit = 'JFNA' THEN 'JUSTFAB NA'
                                     WHEN a.business_unit = 'JFEU' THEN 'JUSTFAB EU'
                                     WHEN a.business_unit = 'SDNA' THEN 'SHOEDAZZLE'
                                     WHEN a.business_unit = 'FKNA' THEN 'FABKIDS'
         END)
         AND rp.month = a.outlook_month
         LEFT JOIN _current_month_credit_cancel cmcc
                   ON cmcc.business_unit = (CASE
                                                WHEN a.business_unit = 'JFNA' THEN 'JUSTFAB NA'
                                                WHEN a.business_unit = 'JFEU' THEN 'JUSTFAB EU'
                                                WHEN a.business_unit = 'SDNA' THEN 'SHOEDAZZLE'
                                                WHEN a.business_unit = 'FKNA' THEN 'FABKIDS'
                       END);


CREATE OR REPLACE TEMPORARY TABLE _last_month_dc AS
SELECT (CASE
            WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
            WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
            WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
            WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
    END)                                                                              AS business_unit
     , SUM(a.product_order_cash_chargeback_amount + a.billing_cash_chargeback_amount) AS chargeback_lm
     , SUM(a.cash_net_revenue)                                                        AS cash_net_revenue_lm
FROM edw_prod.reporting.daily_cash_final_output a
WHERE a.report_mapping IN ('JF-TREV-NA', 'JF-TREV-EU', 'SD-TREV-NA', 'FK-TREV-NA')
  AND a.currency_type = 'USD'
  AND a.date_object = 'placed'
  AND DATE_TRUNC(MONTH, a.date) <= DATEADD(MONTH, -1, $current_month)
GROUP BY (CASE
              WHEN a.report_mapping = 'JF-TREV-NA' THEN 'JUSTFAB NA'
              WHEN a.report_mapping = 'JF-TREV-EU' THEN 'JUSTFAB EU'
              WHEN a.report_mapping = 'SD-TREV-NA' THEN 'SHOEDAZZLE'
              WHEN a.report_mapping = 'FK-TREV-NA' THEN 'FABKIDS'
    END);

CREATE OR REPLACE TEMPORARY TABLE _outlook AS
SELECT business_unit
     , DATE_TRUNC('month', date)        AS month_date
     , SUM(repeat_gaap_net_revenue)     AS repeat_gaap_net_revenue
     , SUM(activating_gaap_net_revenue) AS activating_gaap_net_revenue
FROM reporting_prod.gfb.outlook
GROUP BY business_unit
       , DATE_TRUNC('month', date);

CREATE OR REPLACE TEMPORARY TABLE _forecast_refund AS
SELECT (CASE
            WHEN a.bu = 'JFNA' THEN 'JUSTFAB NA'
            WHEN a.bu = 'SDNA' THEN 'SHOEDAZZLE'
            WHEN a.bu = 'FKNA' THEN 'FABKIDS'
            WHEN a.bu = 'JFEU' THEN 'JUSTFAB EU'
    END)                                                   AS business_unit
     , a.month
     , a.version_date
     , SUM(a.membership_credits_refunded_plus_chargebacks) AS credit_refunds_fc
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
         FROM lake_view.sharepoint.jfb_adhoc_forecast_input a
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
    END)                   AS business_unit
     , a.month
     , a.version_date
     , ABS(a.less_refunds) AS credit_refunds_fc
FROM lake_view.sharepoint.jfb_adhoc_forecast_input a
WHERE a.business_unit IN ('JFNA', 'JFEU', 'SDNA', 'FKNA', 'SD', 'FK');

CREATE OR REPLACE TEMPORARY TABLE _forecast_max_month AS
SELECT business_unit
     , month
     , MAX(version_date) AS max_version_date
FROM _forecast_refund
GROUP BY business_unit
       , month;

CREATE OR REPLACE TEMPORARY TABLE _forecast_final AS
SELECT DISTINCT fr.business_unit
              , fr.month
              , fr.version_date
              , fr.credit_refunds_fc
FROM _forecast_refund fr
         JOIN _forecast_max_month fmm
              ON fr.business_unit = fmm.business_unit
                  AND fr.month = fmm.month
                  AND fr.version_date = fmm.max_version_date;


CREATE OR REPLACE TEMPORARY TABLE _outlook_cal AS
SELECT DISTINCT og.business_unit
              , og.date
              , COALESCE(k.month, DATE_TRUNC('month', og.date))                              AS month
              , og.media_spend
              , og.media_spend / k.cpl                                                       AS leads
              , og.media_spend / k.cpl * k.m1_lead_to_vip_percent                            AS m1_vip
              , og.media_spend / og.vip_cpa - m1_vip                                         AS aged_lead_conversion
              , m1_vip + aged_lead_conversion                                                AS total_new_vip               -- this can also be og.MEDIA_SPEND / og.VIP_CPA
              , total_new_vip                                                                AS activating_order_count
              , og.activating_aov_incl_shipping
              , activating_order_count * og.activating_aov_incl_shipping                     AS activating_gaap_gross_revenue_cal
              , dcl.activating_gaap_net_revenue / dcl.activating_gaap_gross_revenue *
                activating_gaap_gross_revenue_cal                                            AS activating_cash_net_revenue -- activating gaap is same as activating cash, since no credit for activating orders
              , og.repeat_order_count * og.repeat_aov_incl_shipping                          AS repeat_gaap_gross_revenue_cal
              , dcl.repeat_gaap_net_revenue / dcl.repeat_gaap_gross_revenue *
                repeat_gaap_gross_revenue_cal                                                AS repeat_gaap_net_revenue_cal
              , ltv.bop_vips * og.credit_billed_percent * 49.95                              AS membership_credit_charged   -- 39.95 should be replaced by membership price percentage in future
              , membership_credit_charged * og.credit_redeemed_percent                       AS membership_credit_redeemed
              , og.credit_redeemed_percent
              , CASE
                    WHEN k.month >= '2024-04-01' THEN ff.credit_refunds_fc
                    ELSE membership_credit_charged * og.credit_cancelled_percent END         AS membership_credit_refund_chargeback
              , membership_credit_charged - membership_credit_redeemed -
                membership_credit_refund_chargeback                                          AS net_unredeemed_credit_billing
              , repeat_gaap_net_revenue_cal + net_unredeemed_credit_billing -
                dcr.refund_credit_redeemed_amount                                            AS repeat_cash_net_revenue
              , activating_cash_net_revenue + repeat_cash_net_revenue                        AS cash_net_revenue
              , activating_cash_net_revenue * og.activating_gross_margin_percent             AS activating_cash_gross_margin_amount
              , repeat_gaap_net_revenue_cal * og.repeat_gross_margin_percent                 AS repeat_gaap_gross_margin_amount
              , repeat_gaap_gross_margin_amount + net_unredeemed_credit_billing -
                dcr.refund_credit_redeemed_amount                                            AS repeat_cash_gross_margin_amount
              , activating_cash_gross_margin_amount + repeat_cash_gross_margin_amount        AS cash_gross_margin_amount
              , cash_gross_margin_amount / cash_net_revenue                                  AS cash_gross_margin_percent
              , cash_gross_margin_amount - og.media_spend                                    AS contribution_after_media
              , net_unredeemed_credit_billing / cash_net_revenue                             AS net_unredeemed_credit_billing_percent
              , og.activating_upt
              , og.repeat_upt
              , activating_order_count * og.activating_upt                                   AS activating_shipped_units
              , og.repeat_order_count * og.repeat_upt                                        AS repeat_shipped_units
              , activating_order_count * og.activating_upt + og.repeat_order_count *
                                                             og.repeat_upt                   AS total_units_shipped
              , activating_order_count + og.repeat_order_count                               AS total_orders_shipped
              , og.vip_cpa                                                                   AS total_vip_cac
              , total_vip_cac - activating_cash_gross_margin_amount / activating_order_count AS cpaam
              , og.cancels
              , og.repeat_order_count / ltv.bop_vips                                         AS merch_purchase_rate
              , og.cancels / (ltv.bop_vips + m1_vip)                                         AS attrition_rate
              , ltv.bop_vips + m1_vip - og.cancels                                           AS eom_vip_count
              , og.media_spend / m1_vip                                                      AS m1_vip_cpa
              , og.activating_gross_margin_percent
              , og.repeat_gross_margin_percent
              , og.repeat_aov_incl_shipping
              , k.cpl
              , k.m1_lead_to_vip_percent
              , dcr.refund_credit_redeemed_amount
              , og.repeat_order_count
              , ltv.bop_vips
              , og.credit_billing_count
              , og.activating_product_margin_percent
              , og.repeat_product_margin_percent
              , og.activating_discount_percent
              , og.repeat_discount_percent
              , dcr.refund_as_credit_amount
              , lmd.chargeback_lm
              , lmd.cash_net_revenue_lm
              , cash_net_revenue * lmd.chargeback_lm / lmd.cash_net_revenue_lm               AS chargeback
              , ol.activating_gaap_net_revenue
              , ol.repeat_gaap_net_revenue
              , ol.repeat_gaap_net_revenue - membership_credit_redeemed                      AS vip_cash
              , og.repeat_product_margin
              , og.activating_product_margin
              , k.activating_acquisition_margin
              , k.total_vip_on_date_cac
FROM _outlook_gsheet og
         LEFT JOIN _cpa k
                   ON k.business_unit = og.business_unit
                       AND k.month = og.outlook_month
         LEFT JOIN _daily_cash_ly dcl
                   ON dcl.business_unit = og.business_unit
                       AND dcl.month_ty = og.outlook_month
         LEFT JOIN _lead_to_vip ltv
                   ON ltv.business_unit = og.business_unit
                       AND ltv.month = og.outlook_month
         LEFT JOIN _daily_cash_rr dcr
                   ON dcr.business_unit = og.business_unit
                       AND dcr.month = og.outlook_month
         LEFT JOIN _last_month_dc lmd
                   ON lmd.business_unit = og.business_unit
         LEFT JOIN _outlook ol
                   ON ol.business_unit = og.business_unit
                       AND ol.month_date = og.outlook_month
         LEFT JOIN _forecast_final ff ON og.business_unit = ff.business_unit
    AND k.month = ff.month;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb055_01_outlook_metrics AS
SELECT oc.business_unit
     , oc.month
     , oc.date
     , oc.cash_net_revenue
     , oc.repeat_gaap_net_revenue + oc.activating_gaap_net_revenue +
       oc.membership_credit_charged - oc.membership_credit_redeemed AS cash_gross_revenue
     , oc.cash_gross_margin_amount
     , oc.cash_gross_margin_percent
     , oc.contribution_after_media
     , oc.membership_credit_charged                                 AS credit_billing
     , oc.membership_credit_redeemed                                AS credit_redeemed
     , oc.membership_credit_refund_chargeback                       AS credit_refund
     , oc.net_unredeemed_credit_billing
     , oc.net_unredeemed_credit_billing_percent
     , oc.activating_gaap_gross_revenue_cal                         AS activating_product_revenue
     , oc.activating_aov_incl_shipping
     , oc.activating_upt
     , oc.activating_gross_margin_percent
     , oc.repeat_gaap_gross_revenue_cal                             AS repeat_product_revenue
     , oc.repeat_aov_incl_shipping
     , oc.repeat_upt
     , oc.repeat_gross_margin_percent
     , oc.total_units_shipped
     , oc.total_orders_shipped
     , oc.merch_purchase_rate
     , oc.media_spend
     , oc.total_new_vip                                             AS new_vip
     , oc.total_vip_cac
     , oc.cpaam
     , oc.cpl
     , oc.m1_lead_to_vip_percent
     , oc.m1_vip_cpa
     , oc.cancels
     , oc.attrition_rate
     , oc.eom_vip_count
     , oc.activating_order_count
     , oc.activating_shipped_units
     , oc.repeat_order_count
     , oc.repeat_shipped_units
     , oc.bop_vips
     , oc.leads
     , oc.m1_vip
     , oc.activating_cash_net_revenue
     , oc.repeat_cash_net_revenue
     , oc.credit_billing_count
     , oc.activating_product_margin_percent
     , oc.repeat_product_margin_percent
     , oc.refund_credit_redeemed_amount
     , oc.activating_discount_percent
     , oc.repeat_discount_percent
     , oc.repeat_gaap_net_revenue_cal                               AS repeat_gaap_net_revenue
     , oc.activating_cash_gross_margin_amount
     , oc.repeat_cash_gross_margin_amount
     , oc.refund_as_credit_amount
     , oc.chargeback
     , oc.vip_cash / oc.repeat_gaap_net_revenue                     AS vip_cash_percent
     , oc.repeat_product_margin
     , oc.activating_product_margin
     , oc.activating_acquisition_margin
     , oc.total_vip_on_date_cac
     , oc.credit_redeemed_percent
FROM _outlook_cal oc
UNION
SELECT a.business_unit
     , a.outlook_month                                         AS month
     , a.outlook_version_date                                  AS date
     , a.cash_net_revenue
     , a.repeat_product_net_revenue + a.activating_product_revenue + a.credit_billing -
       a.credit_redeemed                                       AS cash_gross_revenue
     , a.cash_gross_margin_amount
     , a.cash_gross_margin_amount / a.cash_net_revenue         AS cash_gross_margin_percent
     , a.cash_gross_margin_amount - a.media_spend              AS contribution_after_media
     , a.credit_billing                                        AS credit_billing
     , a.credit_redeemed                                       AS credit_redeemed
     , a.credit_refund                                         AS credit_refund
     , a.net_unredeemed_credit_billing
     , a.net_unredeemed_credit_billing / a.credit_billing      AS net_unredeemed_credit_billing_percent
     , a.activating_product_revenue                            AS activating_product_revenue
     , a.activating_product_revenue / a.activating_order_count AS activating_aov_incl_shipping
     , a.activating_shipped_units / a.activating_order_count   AS activating_upt
     , a.activating_product_gross_margin_percent               AS activating_gross_margin_percent
     , a.repeat_product_revenue                                AS repeat_product_revenue
     , a.repeat_product_revenue / a.repeat_order_count         AS repeat_aov_incl_shipping
     , a.repeat_shipped_units / a.repeat_order_count           AS repeat_upt
     , a.repeat_product_gross_margin_percent                   AS repeat_gross_margin_percent
     , a.total_units_shipped
     , a.total_orders_shipped
     , a.merch_purchase_rate
     , a.media_spend
     , a.new_vip                                               AS new_vip
     , a.media_spend / a.new_vip                               AS total_vip_cac
     , a.cpaam
     , a.media_spend / a.new_leads                             AS cpl
     , a.m1_vips_from_leads / a.new_leads                      AS m1_lead_to_vip_percent
     , a.media_spend / a.m1_vips_from_leads                    AS m1_vip_cpa
     , a.cancels
     , a.cancels / a.bop_vips                                  AS attrition_rate
     , a.eom_vip_count
     , a.activating_order_count
     , a.activating_shipped_units
     , a.repeat_order_count
     , a.repeat_shipped_units
     , a.bop_vips
     , a.new_leads                                             AS leads
     , a.m1_vips_from_leads                                    AS m1_vip
     , a.activating_cash_net_revenue
     , a.repeat_cash_net_revenue
     , a.credit_billing_count
     , a.activating_product_gross_margin_percent               AS activating_product_margin_percent
     , a.repeat_product_gross_margin_percent                   AS repeat_product_margin_percent
     , a.refund_credit_redeemed_amount
     , a.activating_discount_percent
     , a.repeat_discount_percent
     , a.repeat_product_net_revenue                            AS repeat_gaap_net_revenue
     , a.activating_cash_gross_margin_amount
     , a.repeat_cash_gross_margin_amount
     , a.refund_as_credit_amount
     , a.chargeback
     , (a.repeat_product_net_revenue - a.credit_redeemed) /
       a.repeat_product_net_revenue                            AS vip_cash_percent
     , repeat_product_margin                                   AS repeat_product_margin
     , activating_product_margin                               AS activating_product_margin
     , 0                                                       AS activating_acquisition_margin
     , cpaam                                                   AS total_vip_on_date_cac
     , 0                                                       AS credit_redeemed_percent
FROM lake_view.sharepoint.jfb_manual_outlook_input a
         LEFT JOIN _daily_cash_ly dcl
                   ON dcl.business_unit = a.business_unit
                       AND dcl.month_ty = a.outlook_month;
