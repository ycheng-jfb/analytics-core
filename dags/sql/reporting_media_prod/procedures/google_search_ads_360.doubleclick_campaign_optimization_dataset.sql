
create or replace transient table reporting_media_prod.google_search_ads_360.doubleclick_campaign_optimization_dataset as
select
    vw.*,
    date,

    tfee_pct_of_media_spend as vendor_fees,
    spend_account_currency,
    spend_account_currency * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_account_currency,
    spend_usd,
    spend_usd * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_usd,
    spend_local,
    spend_local * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_local,
    impressions,
    clicks,
    lead_30x7x1,
    lead_7x3x1,
    lead_14x1,
    lead_7x7,
    lead_3x3,
    lead_1x1,
    lead_30dc,
    lead_1dc,
    lead,
    vip_30x7x1,
    vip_7x3x1,
    vip_14x1,
    vip_7x7,
    vip_3x3,
    vip_1x1,
    vip_30dc,
    vip_1dc,
    vip,
    add_to_cart,
    purchase,

    current_timestamp()::datetime as meta_update_datetime

from reporting_media_base_prod.google_search_ads_360.doubleclick_spend_metrics_daily m
join reporting_media_base_prod.google_search_ads_360.vw_doubleclick_naming_convention vw on vw.campaign_id = m.campaign_id
left join lake_view.sharepoint.med_vendor_fees_media v
    on (case when lower(v.identifier) = 'account' then cast(vw.account_id as varchar) = v.identifier_value
        when lower(v.identifier) = 'campaign' and lower(vw.campaign_name) ilike '%' || lower(v.identifier_value) || '%'
            then cast(vw.account_id as varchar) = v.account_id end)
    and m.date between v.start_date_include and v.end_date_exclude - 1
    and lower(v.channel) = 'google';
