create or replace transient table reporting_media_base_prod.twitter.spend_and_conversion_metrics_daily_by_unique_ad_id as
select distinct
    t.account_id,
    t.promoted_tweet_id as unique_ad_id,
    t.date,
    st.store_id,
    sum(spend) as spend_account_currency,
    sum((spend) * coalesce(lkpusd.exchange_rate, 1)) as spend_usd,
    sum((spend) * coalesce(lkplocal.exchange_rate, 1)) as spend_local,
    sum(impressions) as impressions,
    sum(link_clicks) as clicks,
    sum(leads) as leads,
    sum(vips) as vips
from lake_view.twitter.twitter_spend_and_conversion_metrics_by_promoted_tweet_id t
join lake_view.sharepoint.med_account_mapping_media am on am.source_id = t.account_id
    and lower(am.source) = 'twitter'
join edw_prod.data_model.dim_store st on st.store_id = am.store_id
left join edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
    and t.date = lkpusd.rate_date_pst
    and lower(lkpusd.dest_currency) = 'usd'
left join edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
    and t.date = lkplocal.rate_date_pst
    and lkplocal.dest_currency = st.store_currency
group by 1,2,3,4;
