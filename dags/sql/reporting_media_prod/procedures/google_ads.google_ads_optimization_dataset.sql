
create or replace transient table reporting_media_prod.google_ads.google_ads_optimization_dataset as
select
    go.*,
    ad.date,
    v.tfee_pct_of_media_spend as vendor_fees,
    ad.spend_account_currency,
    spend_account_currency * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_account_currency,
    ad.spend_usd,
    spend_usd * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_usd,
    ad.spend_local,
    spend_local * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_local,
    ad.impressions,
    ad.clicks,
    ad.video_views,
    ad.video_views_25pct,
    ad.video_views_50pct,
    ad.video_views_75pct,
    ad.video_views_100pct,

    pixel_lead_30x7_click,
    pixel_lead_30x7_view,
    pixel_lead_1x1_click,
    pixel_lead_1x1_view,
    pixel_lead_3x3_click,
    pixel_lead_3x3_view,
    pixel_lead_7x7_click,
    pixel_lead_7x7_view,
    pixel_lead_1x1x1_click,
    pixel_lead_1x1x1_view,
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
    pixel_vip_1x1x1_click,
    pixel_vip_1x1x1_view,
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

    ad.high_watermark_datetime as pixel_spend_meta_update_datetime,
    pixel.pixel_conversion_meta_update_datetime,
    current_timestamp() as process_meta_update_datetime

from reporting_media_base_prod.google_ads.advertising_spend_metrics_daily_by_ad ad
join reporting_media_base_prod.google_ads.vw_google_ads_naming_convention go on go.ad_id = ad.ad_id
left join reporting_media_base_prod.google_ads.pixel_conversion_metrics_daily_by_ad pixel on ad.ad_id = pixel.ad_id and ad.date = pixel.date
left join lake_view.sharepoint.med_vendor_fees_media v
    on (case when lower(v.identifier) = 'campaign' and go.campaign_name ilike '%' || lower(v.identifier_value) || '%'
            and ad.date between v.start_date_include and v.end_date_exclude - 1
        then cast(go.account_id as varchar) = v.account_id
        when lower(v.identifier) = 'ad labels' and go.ad_labels ilike '%' || lower(v.identifier_value) || '%'
            and ad.date between v.start_date_include and v.end_date_exclude - 1
        then cast(go.account_id as varchar) = v.account_id
        when lower(v.identifier) = 'ad' and go.ad_name ilike '%' || lower(v.identifier_value) || '%'
            and ad.date between v.start_date_include and v.end_date_exclude - 1
        then cast(go.account_id as varchar) = v.account_id end)
    and lower(v.channel) = 'youtube';
