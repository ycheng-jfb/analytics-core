-------------------------------------------------
--tatari optimization pipeline

set spend = (select max(spend_meta_update_datetime) from reporting_media_base_prod.tatari.daily_spend_by_creative);
set spend_date = (select max(spend_meta_update_datetime) from reporting_media_base_prod.tatari.daily_spend_by_creative);
set conversion_date = (select max(conversion_meta_update_datetime) from reporting_media_base_prod.tatari.conversions_credit_on_spend_date);

-- get the most recent creative name from spend table
create or replace temporary table _creative_name as
select distinct
        creative_code,
        subchannel,
        ad_name,
        max(date) as max_date,
        row_number() over (partition by creative_code,subchannel order by max(date) desc) as rn
from reporting_media_base_prod.tatari.daily_spend_by_creative
group by 1,2,3;

--join conversion table to spend and create final reporting optimization dataset
create or replace transient table reporting_media_prod.tatari.tatari_optimization_dataset as
select
    s.channel,
    s.subchannel,
    s.store_brand_name,
    s.store_brand_abbr,
    s.store_id,
    'NA' as region,
    'US' as country,
    s.ad_id,
    n.ad_name,
    s.campaign_id,
    s.date,
    s.creative_code,
    case when s.subchannel = 'streaming' then s.platform
        when s.subchannel = 'tv' then s.network end as network,
    case when s.subchannel = 'streaming' then s.platform
        when s.subchannel = 'tv' then s.program end as program,
    'USD' as currency,

    sum(s.spend) as spend,
    round(sum(s.spend * 1.095),2) as spend_with_vendor_fees,
    sum(s.impressions) as impressions,

    sum(vip_view_1d) as vip_view_1d,
    sum(vip_view_7d) as vip_view_7d,
    sum(vip_view_30d) as vip_view_30d,
    sum(vip_incremental) as vip_incremental,
    sum(vip_men_view_1d) as vip_men_view_1d,
    sum(vip_men_view_7d) as vip_men_view_7d,
    sum(vip_men_view_30d) as vip_men_view_30d,
    sum(vip_men_incremental) as vip_men_incremental,
    sum(purchase_view_1d) as purchase_view_1d,
    sum(purchase_view_7d) as purchase_view_7d,
    sum(purchase_view_30d) as purchase_view_30d,
    sum(purchase_incremental) as purchase_incremental,
    sum(unique_visit_view_1d) as unique_visit_view_1d,
    sum(unique_visit_view_7d) as unique_visit_view_7d,
    sum(unique_visit_view_30d) as unique_visit_view_30d,
    sum(unique_visit_incremental) as unique_visit_incremental,
    sum(lead_view_1d) as lead_view_1d,
    sum(lead_view_7d) as lead_view_7d,
    sum(lead_view_30d) as lead_view_30d,
    sum(lead_incremental) as lead_incremental,

    least($spend, $spend_date, $conversion_date) as latest_source_update,
    current_timestamp()::timestamp_ltz as meta_create_datetime,
    current_timestamp()::timestamp_ltz as meta_update_datetime

from reporting_media_base_prod.tatari.daily_spend_by_creative s
left join reporting_media_base_prod.tatari.conversions_credit_on_spend_date c on s.ad_id = c.ad_id
    and s.date = c.date
left join _creative_name n on s.creative_code = n.creative_code
    and s.subchannel = n.subchannel
    and rn = 1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,39,40,41;
