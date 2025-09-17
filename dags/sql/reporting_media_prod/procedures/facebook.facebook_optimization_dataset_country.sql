create or replace temporary table _spend_conv_combo as
select distinct store_id, ad_id, date from reporting_media_base_prod.facebook.advertising_spend_metrics_daily_by_country
union
select distinct store_id, ad_id, date from reporting_media_base_prod.facebook.conversion_metrics_daily_by_country;


create or replace temporary table _combined_no_ui_cac_target as
select
    cc.date,
    cc.store_id,
    n.*,
    st.store_country as country,

    tfee_pct_of_media_spend as vendor_fees,
    spend_account_currency,
    sum(spend_account_currency) over(partition by n.ad_id order by cc.date asc) as runningsum_spendacctcurrency_by_date_ad_id,
    spend_account_currency * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_account_currency,
    sum(spend_with_vendor_fees_account_currency) over(partition by n.ad_id order by cc.date asc) as runningsum_spendacctcurrency_fees_by_date_ad_id,
    spend_usd,
    sum(spend_usd) over(partition by n.ad_id order by cc.date asc) as runningsum_spendusd_by_date_ad_id,
    spend_usd * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_usd,
    sum(spend_with_vendor_fees_usd) over(partition by n.ad_id order by cc.date asc) as runningsum_spendusd_fees_by_date_ad_id,
    spend_local,
    sum(spend_local) over(partition by n.ad_id order by cc.date asc) as runningsum_spendlocal_by_date_ad_id,
    spend_local * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_local,
    sum(spend_with_vendor_fees_local) over(partition by n.ad_id order by cc.date asc) as runningsum_spendlocal_fees_by_date_ad_id,

    impressions,
    video_play_impressions as video_views,
    sum(impressions) over(partition by n.ad_id order by cc.date asc) as runningsum_impressions_by_date_ad_id,
    clicks,
    video_views_2s,
    video_views_p75,
    avg_video_view,

    pixel_lead_click_1d,
    pixel_lead_view_1d,
    pixel_lead_click_7d,
    pixel_lead_view_7d,
--     0 as pixel_lead_click_28d,
--     0 as pixel_lead_view_28d,

    pixel_vip_click_1d,
    pixel_vip_view_1d,
    pixel_vip_click_7d,
    pixel_vip_view_7d,
--     0 as pixel_vip_click_28d,
--     0 as pixel_vip_view_28d,

--     pixel_vip_click_1d_old,
--     pixel_vip_view_1d_old,
--     pixel_vip_click_7d_old,
--     pixel_vip_view_7d_old,
--     0 as pixel_vip_click_28d_old,
--     0 as pixel_vip_view_28d_old,

    pixel_purchase_click_1d,
    pixel_purchase_view_1d,
    pixel_purchase_click_7d,
    pixel_purchase_view_7d,
--     0 as pixel_purchase_click_28d,
--     0 as pixel_purchase_view_28d,

    pixel_subscribe_click_1d,
    pixel_subscribe_view_1d,
    pixel_subscribe_click_7d,
    pixel_subscribe_view_7d,
--    0 as pixel_subscribe_click_28d,
--    0 as pixel_subscribe_view_28d,

--     pixel_m1_vip_click_1d,
--     pixel_m1_vip_view_1d,
--     pixel_d1_vip_click_1d,
--     pixel_d1_vip_view_1d,

--     pixel_subscribe_men_click_1d,
--     pixel_subscribe_men_view_1d,
--     pixel_subscribe_men_click_7d,
--     pixel_subscribe_men_view_7d,
--     0 as pixel_subscribe_men_click_28d,
--     0 as pixel_subscribe_men_view_28d,

--     pixel_lead_men_click_1d,
--     pixel_lead_men_view_1d,
--     pixel_lead_men_click_7d,
--     pixel_lead_men_view_7d,
--     0 as pixel_lead_men_click_28d,
--     0 as pixel_lead_men_view_28d,
--     pixel_lead_women_click_1d,
--     pixel_lead_women_view_1d,
--     pixel_lead_women_click_7d,
--     pixel_lead_women_view_7d,
--     0 as pixel_lead_women_click_28d,
--     0 as pixel_lead_women_view_28d,

        pixel_purchase_men_click_1d,
        pixel_purchase_men_view_1d,
        pixel_purchase_men_click_7d,
        pixel_purchase_men_view_7d,
        pixel_purchase_women_click_1d,
        pixel_purchase_women_view_1d,
        pixel_purchase_women_click_7d,
        pixel_purchase_women_view_7d,
--
--     pixel_atc_men_click_1d,
--     pixel_atc_men_view_1d,
--     pixel_atc_men_click_7d,
--     pixel_atc_men_view_7d,
--     pixel_atc_women_click_1d,
--     pixel_atc_women_view_1d,
--     pixel_atc_women_click_7d,
--     pixel_atc_women_view_7d,

--     pixel_view_content_men_click_1d,
--     pixel_view_content_men_view_1d,
--     pixel_view_content_men_click_7d,
--     pixel_view_content_men_view_7d,
--     pixel_view_content_women_click_1d,
--     pixel_view_content_women_view_1d,
--     pixel_view_content_women_click_7d,
--     pixel_view_content_women_view_7d,

    pixel_subscribe_revenue_click_1d,
    pixel_subscribe_revenue_view_1d,
    pixel_subscribe_revenue_click_7d,
    pixel_subscribe_revenue_view_7d,
--     0 as pixel_subscribe_revenue_click_28d,
--     0 as pixel_subscribe_revenue_view_28d,

    pixel_add_to_cart_click_1d,
    pixel_add_to_cart_view_1d,
    pixel_add_to_cart_click_7d,
    pixel_add_to_cart_view_7d,
--     0 as pixel_add_to_cart_click_28d,
--     0 as pixel_add_to_cart_view_28d,

    pixel_landing_page_click_1d,
    pixel_landing_page_view_1d,
    pixel_landing_page_click_7d,
    pixel_landing_page_view_7d,
--     0 as pixel_landing_page_click_28d,
--     0 as pixel_landing_page_view_28d,

    pixel_view_content_click_1d,
    pixel_view_content_view_1d,
    pixel_view_content_click_7d,
    pixel_view_content_view_7d,
--     0 as pixel_view_content_click_28d,
--     0 as pixel_view_content_view_28d,

--     pixel_video_view_3s_click_1d,
--     pixel_video_view_3s_view_1d,
--     pixel_video_view_3s_click_7d,
--     pixel_video_view_3s_view_7d,
--     0 as pixel_video_view_3s_click_28d,
--     0 as pixel_video_view_3s_view_28d,

    ios_percentimpressions,
    android_percentimpressions,

    s.pixel_spend_meta_update_datetime,
    c.pixel_conversion_meta_update_datetime,
    current_timestamp()::timestamp_ltz as process_meta_update_datetime
from _spend_conv_combo cc
left join reporting_media_base_prod.facebook.advertising_spend_metrics_daily_by_country s on s.ad_id = cc.ad_id
        and s.date = cc.date
        and s.store_id = cc.store_id
left join reporting_media_base_prod.facebook.conversion_metrics_daily_by_country c on cc.ad_id = c.ad_id
        and cc.date = c.date
        and cc.store_id = c.store_id
join edw_prod.data_model.dim_store st on st.store_id = cc.store_id
left join reporting_media_base_prod.facebook.vw_facebook_naming_convention n on n.ad_id = cc.ad_id
left join reporting_media_base_prod.facebook.operating_system_percent_daily_impressions os on os.ad_id=cc.ad_id and os.date=cc.date
left join lake_view.sharepoint.med_vendor_fees_media v
    on (case when lower(v.identifier) = 'account'
            and cc.date between v.start_date_include and v.end_date_exclude - 1
        then cast(n.account_id as varchar) = v.identifier_value
        when lower(v.identifier) = 'adset'
            and n.adgroup_name ilike '%' || lower(v.identifier_value) || '%'
            and cc.date between v.start_date_include and v.end_date_exclude - 1
        then cast(n.account_id as varchar) = v.account_id
        when lower(v.identifier) = 'campaign'
            and n.campaign_name ilike '%' || lower(v.identifier_value) || '%'
            and cc.date between v.start_date_include and v.end_date_exclude - 1
        then cast(n.account_id as varchar) = v.account_id
        when lower(v.identifier) = 'ad'
            and n.ad_name ilike '%' || lower(v.identifier_value) || '%'
            and cc.date between v.start_date_include and v.end_date_exclude - 1
            then cast(n.account_id as varchar) = v.account_id end)
    and lower(v.channel) = 'facebook';


create or replace transient table reporting_media_prod.facebook.facebook_optimization_dataset_country as
select c.*,
       ui_cac_target,
       ui_optimization_window
from _combined_no_ui_cac_target c
left join reporting_media_base_prod.dbo.media_channel_ui_targets_actuals t on t.store_brand_name = c.store_brand_name
                                and t.date=c.date
                                and t.channel = 'fb+ig'
                                and t.region = c.region;
