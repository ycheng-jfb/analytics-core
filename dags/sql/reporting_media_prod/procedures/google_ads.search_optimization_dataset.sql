
create or replace transient table reporting_media_prod.google_ads.search_optimization_dataset as
select
    n.*,

    a.date,
    a.spend_account_currency,
    a.spend_usd,
    a.spend_local,
    a.impressions,
    a.clicks,

    pixel_lead_30x7_click,
    pixel_lead_30x7_view,
    pixel_lead_1x1_click,
    pixel_lead_1x1_view,
    pixel_lead_3x3_click,
    pixel_lead_3x3_view,
    pixel_lead_7x7_click,
    pixel_lead_7x7_view,
    pixel_lead_30x7x1_click,
    pixel_lead_30x7x1_view,
    pixel_lead_7x3x1_click,
    pixel_lead_7x3x1_view,
    pixel_lead_7x1_click,
    pixel_lead_7x1_view,
    pixel_lead_14x1_click,
    pixel_lead_14x1_view,
    pixel_lead_30dc_click,
    pixel_lead_1dc_click,

    pixel_vip_30x7_click,
    pixel_vip_30x7_view,
    pixel_vip_1x1_click,
    pixel_vip_1x1_view,
    pixel_vip_3x3_click,
    pixel_vip_3x3_view,
    pixel_vip_7x7_click,
    pixel_vip_7x7_view,
    pixel_vip_30x7x1_click,
    pixel_vip_30x7x1_view,
    pixel_vip_7x3x1_click,
    pixel_vip_7x3x1_view,
    pixel_vip_7x1_click,
    pixel_vip_7x1_view,
    pixel_vip_14x1_click,
    pixel_vip_14x1_view,
    pixel_vip_30dc_click,
    pixel_vip_1dc_click,

    a.high_watermark_datetime as pixel_spend_meta_update_datetime,
    c.pixel_conversion_meta_update_datetime,
    current_timestamp() as process_meta_update_datetime

from reporting_media_base_prod.google_ads.advertising_spend_metrics_daily_by_ad_with_pmax a
join reporting_media_base_prod.google_ads.vw_search_naming_convention n on a.ad_id = n.ad_id
left join reporting_media_base_prod.google_ads.pixel_conversion_metrics_daily_by_ad c on a.ad_id = c.ad_id
    and a.date = c.date;
