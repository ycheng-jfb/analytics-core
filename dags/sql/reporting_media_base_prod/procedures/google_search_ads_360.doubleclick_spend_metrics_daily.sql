
create or replace temporary table _spend as
select
    ds.store_id,
    customer_id as account_id,
    date,
    campaign_id as campaign_id,
    max(date) as meta_update_dateime,
    sum(cost_micros/1000000) as spend_account_currency,
    sum((cost_micros/1000000) * coalesce(lkpusd.exchange_rate, 1)) as spend_usd,
    sum((cost_micros/1000000) * coalesce(lkplocal.exchange_rate, 1)) as spend_local,
    sum(cs.impressions) as impressions,
    sum(cs.clicks) as clicks
from lake_view.google_search_ads_360.campaign_stats cs
join lake_view.sharepoint.med_account_mapping_media am on cs.customer_id = am.source_id and lower(source) ilike '%doubleclick%' -- figure out better way to map this in account mapping
join edw_prod.data_model.dim_store ds on am.store_id = ds.store_id
left join edw_prod.reference.currency_exchange_rate_by_date lkpusd
    on am.currency = lkpusd.src_currency
    and cs.date = lkpusd.rate_date_pst
    and lkpusd.dest_currency = 'USD'
left join edw_prod.reference.currency_exchange_rate_by_date lkplocal
    on am.currency = lkplocal.src_currency
    and cs.date = lkplocal.rate_date_pst
    and lkplocal.dest_currency = ds.store_currency
group by 1,2,3,4;

create or replace temporary table _conversions as
select
    ds.store_id,
    customer_id as account_id,
    date,
    campaign_id as campaign_id,

    sum(iff(lower(conversion_action_name) like '%lead 30x7x1%', all_conversions, 0)) as lead_30x7x1,
    sum(iff(lower(conversion_action_name) like '%lead 7x3x1%', all_conversions, 0)) as lead_7x3x1,
    sum(iff(lower(conversion_action_name) like '%lead 14x1%', all_conversions, 0)) as lead_14x1,
    sum(iff(lower(conversion_action_name) like '%lead 7x7%', all_conversions, 0)) as lead_7x7,
    sum(iff(lower(conversion_action_name) like '%lead 3x3%', all_conversions, 0)) as lead_3x3,
    sum(iff(lower(conversion_action_name) like '%lead 1x1%', all_conversions, 0)) as lead_1x1,
    sum(iff(lower(conversion_action_name) like '%lead 30dc%', all_conversions, 0)) as lead_30dc,
    sum(iff(lower(conversion_action_name) like '%lead 1dc%', all_conversions, 0)) as lead_1dc,
    sum(iff(lower(conversion_action_name) like '%lead%', all_conversions, 0)) as lead,
    sum(iff(lower(conversion_action_name) like '%vip 30x7x1%', all_conversions, 0)) as vip_30x7x1,
    sum(iff(lower(conversion_action_name) like '%vip 7x3x1%', all_conversions, 0)) as vip_7x3x1,
    sum(iff(lower(conversion_action_name) like '%vip 14x1%', all_conversions, 0)) as vip_14x1,
    sum(iff(lower(conversion_action_name) like '%vip 7x7%', all_conversions, 0)) as vip_7x7,
    sum(iff(lower(conversion_action_name) like '%vip 3x3%', all_conversions, 0)) as vip_3x3,
    sum(iff(lower(conversion_action_name) like '%vip 1x1%', all_conversions, 0)) as vip_1x1,
    sum(iff(lower(conversion_action_name) like '%vip 30dc%', all_conversions, 0)) as vip_30dc,
    sum(iff(lower(conversion_action_name) like '%vip 1dc%', all_conversions, 0)) as vip_1dc,
    sum(iff(lower(conversion_action_name) like '%vip%', all_conversions, 0)) as vip,
    sum(iff(lower(conversion_action_name) like '%addtocart%', all_conversions, 0)) as add_to_cart,
    sum(iff(lower(conversion_action_name) like '%purchase%', all_conversions, 0)) as purchase

from lake_view.google_search_ads_360.campaign_conversions c
join  lake_view.sharepoint.med_google_ads_conversion_mapping m on lower(c.conversion_action_name) = lower(m.conversion_event_name)
join lake_view.sharepoint.med_account_mapping_media am on c.customer_id = am.source_id and lower(source) ilike '%doubleclick%'
join edw_prod.data_model.dim_store ds on am.store_id = ds.store_id
group by 1,2,3,4;

--scaffold
create or replace temporary table _scaffold as
select distinct store_id, account_id, date, campaign_id from _spend
union
select distinct store_id, account_id, date, campaign_id from _conversions;

create or replace transient table reporting_media_base_prod.google_search_ads_360.doubleclick_spend_metrics_daily as
select
    cc.store_id,
    cc.account_id,
    cc.date,
    cc.campaign_id,
    spend_account_currency,
    spend_usd,
    spend_local,
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
    meta_update_dateime
from _scaffold cc
left join _spend s on cc.store_id = s.store_id and cc.account_id = s.account_id and cc.date = s.date and cc.campaign_id = s.campaign_id
left join _conversions c on cc.store_id = c.store_id and cc.account_id = c.account_id and cc.date = c.date and cc.campaign_id = c.campaign_id;
