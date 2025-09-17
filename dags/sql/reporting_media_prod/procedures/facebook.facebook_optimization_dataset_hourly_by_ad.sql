
create or replace temporary table _spend_conv_combo as
select store_id, ad_id, date, hour from reporting_media_base_prod.facebook.advertising_spend_metrics_hourly_by_ad
union
select store_id, ad_id, date, hour from reporting_media_base_prod.facebook.conversion_metrics_hourly_by_ad;

create or replace transient table reporting_media_prod.facebook.facebook_optimization_dataset_hourly_by_ad as
select
    n.*,
    s.date,
    s.hour,
    s.store_id,
    st.store_country as country,

    spend_account_currency,
    spend_usd,
    spend_local,
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
join edw_prod.data_model.dim_store st on st.store_id = s.store_id
left join reporting_media_base_prod.facebook.advertising_spend_metrics_hourly_by_ad sp on s.ad_id = sp.ad_id
        and s.date = sp.date
        and s.hour = sp.hour
        and s.store_id = sp.store_id
left join reporting_media_base_prod.facebook.conversion_metrics_hourly_by_ad c on s.ad_id = c.ad_id
        and s.date = c.date
        and s.hour = c.hour
        and s.store_id = c.store_id
left join reporting_media_base_prod.facebook.vw_facebook_naming_convention n on n.ad_id = s.ad_id;
