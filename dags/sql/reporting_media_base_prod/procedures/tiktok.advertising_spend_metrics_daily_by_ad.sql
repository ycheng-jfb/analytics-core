
set high_watermark_datetime = (select max(meta_update_datetime) from LAKE_VIEW.TIKTOK.DAILY_SPEND);

create or replace transient table reporting_media_base_prod.tiktok.advertising_spend_metrics_daily_by_ad as
select
    advertiser_id as account_id,
    ad_id,
    st.store_id,
    date,

    sum(media_cost) as spend_account_currency,
    sum((media_cost)*coalesce(lkpusd.exchange_rate,1)) as spend_usd,
    sum((media_cost)*coalesce(lkplocal.exchange_rate,1)) as spend_local,
    sum(impressions) as impressions,
    sum(clicks) as clicks,

    $high_watermark_datetime as latest_spend_update_datetime,
    current_timestamp()::timestamp_ltz as meta_create_datetime,
    current_timestamp()::timestamp_ltz as meta_update_datetime

from LAKE_VIEW.TIKTOK.DAILY_SPEND tt
    join lake_view.sharepoint.med_account_mapping_media am on am.source_id = tt.advertiser_id
        and lower(am.source) ilike '%tiktok%'
    join edw_prod.data_model.dim_store st on st.store_id = am.store_id
    left join edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
        and tt.date = lkpusd.rate_date_pst
        and lkpusd.dest_currency = 'USD'
    left join edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
        and tt.date = lkplocal.rate_date_pst
        and lkplocal.dest_currency = st.store_currency
group by 1,2,3,4;
