CREATE OR REPLACE TEMP TABLE _actuals AS
SELECT
    store_brand_name,
    region,
    channel_optimization AS channel,
    date,
    ui_optimization_window,
    SUM(spend_usd) AS spend_usd,
    SUM(spend_with_vendor_fees_usd) AS spend_with_vendor_fees,
    SUM(ui_optimization_pixel_vip) AS ui_optimization_pixel_vip
FROM reporting_media_prod.dbo.all_channel_optimization
    WHERE channel_optimization NOT IN ('affiliate', 'reddit', 'influencers')
GROUP BY 1,2,3,4,5;

CREATE OR REPLACE TEMP TABLE _targets AS
SELECT
    store_brand_name,
    region,
    channel,
    month,
    MAX(ui_cac_target) AS ui_cac_target
FROM lake_view.sharepoint.med_media_outlook_ui_cac_target
GROUP BY 1,2,3,4;

CREATE OR REPLACE TEMP TABLE _combos as
SELECT DISTINCT store_brand_name, region, channel, date
FROM _actuals
UNION
SELECT DISTINCT store_brand_name, region, channel, month as date
FROM _targets
WHERE ui_cac_target IS NOT NULL;

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.dbo.media_channel_ui_targets_actuals as
SELECT
    c.*,
    a.ui_optimization_window,
    a.spend_usd,
    a.spend_with_vendor_fees,
    a.ui_optimization_pixel_vip,
    t.ui_cac_target
FROM _combos AS c
LEFT JOIN _actuals AS a
    ON a.channel = c.channel
    AND a.region = c.region
    AND a.store_brand_name = c.store_brand_name
    AND a.date = c.date
LEFT JOIN _targets AS t
    ON t.channel = c.channel
    AND t.region = c.region
    AND t.store_brand_name = c.store_brand_name
    AND t.month = DATE_TRUNC(MONTH, c.date);

