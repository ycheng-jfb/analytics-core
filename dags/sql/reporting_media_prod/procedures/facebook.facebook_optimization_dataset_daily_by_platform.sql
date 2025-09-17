set end_date = current_date();
set start_date = date_trunc(year,dateadd(year,-2,$end_date));

create or replace temporary table _spend_conv_combo as
select store_id, date, ad_id, publisher_platform, platform_position, impression_device from reporting_media_base_prod.facebook.advertising_spend_metrics_daily_by_platform
union
select store_id, date, ad_id, publisher_platform, platform_position, impression_device from reporting_media_base_prod.facebook.conversion_metrics_daily_by_platform;

create or replace transient table reporting_media_prod.facebook.facebook_optimization_dataset_platform as
select
    n.*,
    s.date,

    s.store_id,
    st.store_country as country,

    replace(s.publisher_platform,'_',' ') as publisher_platform,
    replace(s.platform_position,'_',' ') as platform_position,
    replace(s.impression_device,'_',' ') as impression_device,

    tfee_pct_of_media_spend as vendor_fees,
    spend_account_currency,
    spend_account_currency * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_account_currency,
    spend_usd,
    spend_usd * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_usd,
    spend_local,
    spend_local * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_local,

    impressions,
    clicks,

    pixel_lead_click_1d,
    pixel_lead_view_1d,
    pixel_lead_click_7d,
    pixel_lead_view_7d,
    0 as pixel_lead_click_28d,
    0 as pixel_lead_view_28d,

    pixel_vip_click_1d,
    pixel_vip_view_1d,
    pixel_vip_click_7d,
    pixel_vip_view_7d,
    0 as pixel_vip_click_28d,
    0 as pixel_vip_view_28d,

    pixel_purchase_click_1d,
    pixel_purchase_view_1d,
    pixel_purchase_click_7d,
    pixel_purchase_view_7d,
    0 as pixel_purchase_click_28d,
    0 as pixel_purchase_view_28d,

    pixel_subscribe_click_1d,
    pixel_subscribe_view_1d,
    pixel_subscribe_click_7d,
    pixel_subscribe_view_7d,
    0 as pixel_subscribe_click_28d,
    0 as pixel_subscribe_view_28d,

    pixel_subscribe_revenue_click_1d,
    pixel_subscribe_revenue_view_1d,
    pixel_subscribe_revenue_click_7d,
    pixel_subscribe_revenue_view_7d,
    0 as pixel_subscribe_revenue_click_28d,
    0 as pixel_subscribe_revenue_view_28d,

    sp.pixel_spend_meta_update_datetime,
    c.pixel_conversion_meta_update_datetime,
    current_timestamp()::timestamp_ltz as process_meta_update_datetime

from _spend_conv_combo s
left join reporting_media_base_prod.facebook.advertising_spend_metrics_daily_by_platform sp on s.ad_id = sp.ad_id
        and s.date = sp.date
        and s.publisher_platform = sp.publisher_platform
        and s.platform_position = sp.platform_position
        and s.impression_device = sp.impression_device
left join reporting_media_base_prod.facebook.conversion_metrics_daily_by_platform c on s.ad_id = c.ad_id
        and s.date = c.date
        and s.publisher_platform = c.publisher_platform
        and s.platform_position = c.platform_position
        and s.impression_device = c.impression_device
left join edw_prod.data_model.dim_store st on st.store_id = s.store_id
left join reporting_media_base_prod.facebook.vw_facebook_naming_convention n on n.ad_id = s.ad_id
left join lake_view.sharepoint.med_vendor_fees_media v
    on (case when lower(v.identifier) = 'account'
            and s.date between v.start_date_include and v.end_date_exclude - 1
        then cast(n.account_id as varchar) = v.identifier_value
        when lower(v.identifier) = 'adset'
            and n.adgroup_name ilike '%' || lower(v.identifier_value) || '%'
            and s.date between v.start_date_include and v.end_date_exclude - 1
        then cast(n.account_id as varchar) = v.account_id
        when lower(v.identifier) = 'campaign'
            and n.campaign_name ilike '%' || lower(v.identifier_value) || '%'
            and s.date between v.start_date_include and v.end_date_exclude - 1
        then cast(n.account_id as varchar) = v.account_id
        when lower(v.identifier) = 'ad'
            and n.ad_name ilike '%' || lower(v.identifier_value) || '%'
            and s.date between v.start_date_include and v.end_date_exclude - 1
            then cast(n.account_id as varchar) = v.account_id end)
    and lower(v.channel) = 'facebook'
    where s.date < $end_date
    and s.date >= $start_date;
