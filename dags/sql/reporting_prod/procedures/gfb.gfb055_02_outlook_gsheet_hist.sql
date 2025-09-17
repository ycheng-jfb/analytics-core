CREATE OR REPLACE TEMPORARY TABLE _activating_outlook_input AS
SELECT a.business_unit
     , a.region
     , DATE_TRUNC(WEEK, CURRENT_DATE())             AS updated_date
     , DATE_TRUNC(MONTH, a.date)                    AS month_date
     , SUM(a.gaap_revenue) * 1.0 / SUM(a.orders)    AS activating_aov_incl_shipping
     , SUM(a.units) * 1.0 / SUM(a.orders)           AS activating_upt
     , AVG(a.discount_rate)                         AS activating_discount_percent
     , AVG(gaap_gross_margin_percent)               AS activating_gross_margin_percent
     , SUM(a.gaap_revenue)                          AS activating_gaap_revenue
     , (SUM(a.aur) - SUM(a.auc)) * 1.0 / SUM(a.aur) AS activating_product_margin_percent
     , SUM(gaap_gross_margin)                       AS activating_product_margin
FROM lake_view.sharepoint.gfb_daily_sale_standup_activating_tsos a
WHERE a.source = 'Forecast/Actuals'
GROUP BY a.business_unit
       , a.region
       , DATE_TRUNC(WEEK, CURRENT_DATE())
       , DATE_TRUNC(MONTH, a.date);

CREATE OR REPLACE TEMPORARY TABLE _repeat_outlook_input AS
SELECT a.business_unit
     , a.region
     , DATE_TRUNC(WEEK, CURRENT_DATE())             AS updated_date
     , DATE_TRUNC(MONTH, a.date)                    AS month_date
     , SUM(a.orders)                                AS repeat_order_count
     , SUM(a.gaap_revenue) * 1.0 / SUM(a.orders)    AS repeat_aov_incl_shipping
     , SUM(a.units) * 1.0 / SUM(a.orders)           AS repeat_upt
     , AVG(a.discount_rate)                         AS repeat_discount_percent
     , AVG(gaap_gross_margin_percent)               AS repeat_gross_margin_percent
     , SUM(a.gaap_revenue)                          AS repeat_gaap_revenue
     , (SUM(a.aur) - SUM(a.auc)) * 1.0 / SUM(a.aur) AS repeat_product_margin_percent
     , SUM(a.gaap_gross_margin)                     AS repeat_product_margin
FROM lake_view.sharepoint.gfb_daily_sale_standup_repeat a
WHERE a.source = 'Forecast/Actuals'
GROUP BY a.business_unit
       , a.region
       , DATE_TRUNC(WEEK, CURRENT_DATE())
       , DATE_TRUNC(MONTH, a.date);

CREATE OR REPLACE TEMPORARY TABLE _outlook_input AS
SELECT aoi.*
     , roi.repeat_order_count
     , roi.repeat_aov_incl_shipping
     , roi.repeat_upt
     , roi.repeat_discount_percent
     , roi.repeat_gross_margin_percent
     , roi.repeat_gaap_revenue
     , roi.repeat_product_margin_percent
     ,roi.repeat_product_margin
FROM _activating_outlook_input aoi
         LEFT JOIN _repeat_outlook_input roi
                   ON roi.business_unit = aoi.business_unit
                       AND roi.region = aoi.region
                       AND roi.updated_date = aoi.updated_date
                       AND roi.month_date = aoi.month_date;

MERGE INTO reporting_prod.gfb.gfb055_02_outlook_gsheet_his t USING _outlook_input s
    ON s.business_unit = t.business_unit
        AND s.region = t.region
        AND s.updated_date = t.updated_date
        AND s.month_date = t.month_date
    WHEN MATCHED THEN
        UPDATE SET
            t.activating_aov_incl_shipping = s.activating_aov_incl_shipping
            ,t.activating_discount_percent = s.activating_discount_percent
            ,t.activating_gross_margin_percent = s.activating_gross_margin_percent
            ,t.activating_upt = s.activating_upt
            ,t.activating_gaap_revenue = s.activating_gaap_revenue
            ,t.repeat_aov_incl_shipping = s.repeat_aov_incl_shipping
            ,t.repeat_discount_percent = s.repeat_discount_percent
            ,t.repeat_gross_margin_percent = s.repeat_gross_margin_percent
            ,t.repeat_upt = s.repeat_upt
            ,t.repeat_order_count = s.repeat_order_count
            ,t.repeat_gaap_revenue = s.repeat_gaap_revenue
            ,t.repeat_product_margin_percent = s.repeat_product_margin_percent
            ,t.activating_product_margin_percent = s.activating_product_margin_percent
            ,t.repeat_product_margin = s.repeat_product_margin
            ,t.activating_product_margin = s.activating_product_margin
    WHEN NOT MATCHED THEN
        INSERT
            (
             business_unit, region, updated_date, month_date, activating_aov_incl_shipping, activating_upt,
             activating_discount_percent, activating_gross_margin_percent, activating_gaap_revenue,
             activating_product_margin_percent, repeat_order_count, repeat_aov_incl_shipping, repeat_upt,
             repeat_discount_percent, repeat_gross_margin_percent, repeat_gaap_revenue, repeat_product_margin_percent,repeat_product_margin,activating_product_margin
                )
            VALUES ( s.business_unit
                   , s.region
                   , s.updated_date
                   , s.month_date
                   , s.activating_aov_incl_shipping
                   , s.activating_upt
                   , s.activating_discount_percent
                   , s.activating_gross_margin_percent
                   , s.activating_gaap_revenue
                   , s.activating_product_margin_percent
                   , s.repeat_order_count
                   , s.repeat_aov_incl_shipping
                   , s.repeat_upt
                   , s.repeat_discount_percent
                   , s.repeat_gross_margin_percent
                   , s.repeat_gaap_revenue
                   , s.repeat_product_margin_percent
                   , s.repeat_product_margin
                   ,s.activating_product_margin);
