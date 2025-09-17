set high_watermark_datetime = (select max(meta_update_datetime) from lake_view.microsoft_ads.ad_performance_daily_report);

create or replace transient table reporting_media_base_prod.microsoft_ads.daily_media_spend as
select
    account_id,
    ad_group_id as adgroup_id,
    st.store_id,
    date,

    sum(spend) as spend_account_currency,
    sum((spend)*coalesce(lkpusd.exchange_rate,1)) as spend_usd,
    sum((spend)*coalesce(lkplocal.exchange_rate,1)) as spend_local,
    sum(impressions) as impressions,
    sum(clicks) as clicks,

    $high_watermark_datetime as latest_spend_update_datetime,
    current_timestamp()::timestamp_ltz as meta_create_datetime,
    current_timestamp()::timestamp_ltz as meta_update_datetime

from lake_view.microsoft_ads.ad_performance_daily_report a
join lake_view.sharepoint.med_account_mapping_media am on am.source_id = a.account_id
    and lower(am.source) ilike '%microsoft%'
join edw_prod.data_model.dim_store st on st.store_id = am.store_id
left join edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
    and a.date = lkpusd.rate_date_pst
    and lower(lkpusd.dest_currency) = 'usd'
left join edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
    and a.date = lkplocal.rate_date_pst
    and lkplocal.dest_currency = st.store_currency
group by 1,2,3,4;
