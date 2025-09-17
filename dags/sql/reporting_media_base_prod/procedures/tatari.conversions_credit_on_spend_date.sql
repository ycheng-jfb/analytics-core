
---------------------------
-- purpose: create cleansed conversion table that includes linear and streaming

--streaming

--1).
-- create a column for conversion window by creating all combos of conversion_metric + methodology
--after this step, conv table grain will be: campaign_id, creative_name, impression_date, conversion_window
create or replace temporary table _streaming as
select
    'tv+streaming' as channel,
    'streaming' as subchannel,
    case when lower(creative_name) ilike '%flm%' then 'Fabletics Men'
        when lower(creative_name) ilike '%yty%' then 'Yitty'
        when lower(creative_name) ilike '%scrubs%' then 'Fabletics Scrubs'
    else t3.store_brand end as store_brand_name,
    iff(store_brand_name='Fabletics Men','FLM',store_brand_abbr) as store_brand_abbr,
    t3.store_id,
    iff(right(campaign_id, 1) = '_', concat(campaign_id,creative_code), concat(campaign_id, '_', creative_code)) as ad_id,
    creative_name as ad_name,
    campaign_id,
    impression_date as date,
    creative_code,
    publisher,
    'null' as rotation,
    lift,

    --vip
    case when (lower(conversion_metric) = 'vip conversion' and lower(methodology) = 'tatari_view_through_day') then 'vip_view_1d'
        when (lower(conversion_metric) = 'vip conversion' and lower(methodology) = 'tatari_view_through_week') then 'vip_view_7d'
        when (lower(conversion_metric) = 'vip conversion' and lower(methodology) = 'tatari_view_through_month') then 'vip_view_30d'
        when (lower(conversion_metric) = 'vip conversion' and lower(methodology) = 'incremental') then 'vip_incremental'
    --purchase
        when (lower(conversion_metric) = 'order completed' and lower(methodology) = 'tatari_view_through_day') then 'purchase_view_1d'
        when (lower(conversion_metric) = 'order completed' and lower(methodology) = 'tatari_view_through_week') then 'purchase_view_7d'
        when (lower(conversion_metric) = 'order completed' and lower(methodology) = 'tatari_view_through_month') then 'purchase_view_30d'
        when (lower(conversion_metric) = 'order completed' and lower(methodology) = 'incremental') then 'purchase_incremental'
    --vip men
        when (lower(conversion_metric) = 'vip conversion men' and lower(methodology) = 'tatari_view_through_day') then 'vip_men_view_1d'
        when (lower(conversion_metric) = 'vip conversion men' and lower(methodology) = 'tatari_view_through_week') then 'vip_men_view_7d'
        when (lower(conversion_metric) = 'vip conversion men' and lower(methodology) = 'tatari_view_through_month') then 'vip_men_view_30d'
        when (lower(conversion_metric) = 'vip conversion men' and lower(methodology) = 'incremental') then 'vip_men_incremental'
    --unique visits
        when (lower(conversion_metric) = 'unique visitors' and lower(methodology) = 'tatari_view_through_day') then 'unique_visit_view_1d'
        when (lower(conversion_metric) = 'unique visitors' and lower(methodology) = 'tatari_view_through_week') then 'unique_visit_view_7d'
        when (lower(conversion_metric) = 'unique visitors' and lower(methodology) = 'tatari_view_through_month') then 'unique_visit_view_30d'
        when (lower(conversion_metric) = 'unique visitors' and lower(methodology) = 'incremental') then 'unique_visit_incremental'
    --leads
        when (lower(conversion_metric) = 'complete registration' and lower(methodology) = 'tatari_view_through_day') then 'lead_view_1d'
        when (lower(conversion_metric) = 'complete registration' and lower(methodology) = 'tatari_view_through_week') then 'lead_view_7d'
        when (lower(conversion_metric) = 'complete registration' and lower(methodology) = 'tatari_view_through_month') then 'lead_view_30d'
        when (lower(conversion_metric) = 'complete registration' and lower(methodology) = 'incremental') then 'lead_incremental'
    end as conversion_window

from lake.tatari.streaming_conversions t1
join lake_view.sharepoint.med_account_mapping_media t2 on t1.account_name = t2.source_id
    and lower(t2.source) ilike '%tatari%'
join edw_prod.data_model.dim_store t3 on t2.store_id = t3.store_id;

--2).
--pivot the conversion window values to columns
--grain turns into the same grain as the spend/imp staging table (creative_id, creative_name, date)
create or replace temporary table _streaming_pivoted as
select
    channel,
    subchannel,
    store_brand_name,
    store_brand_abbr,
    store_id,
    ad_id,
    ad_name,
    campaign_id,
    date,
    creative_code,
    publisher,
    rotation,
    vip_view_1d,
    vip_view_7d,
    vip_view_30d,
    vip_incremental,
    vip_men_view_1d,
    vip_men_view_7d,
    vip_men_view_30d,
    vip_men_incremental,
    purchase_view_1d,
    purchase_view_7d,
    purchase_view_30d,
    purchase_incremental,
    unique_visit_view_1d,
    unique_visit_view_7d,
    unique_visit_view_30d,
    unique_visit_incremental,
    lead_view_1d,
    lead_view_7d,
    lead_view_30d,
    lead_incremental
from _streaming
pivot (sum(lift) for conversion_window in ('vip_view_1d','vip_view_7d','vip_view_30d','vip_incremental','vip_men_view_1d','vip_men_view_7d',
  'vip_men_view_30d','vip_men_incremental','purchase_view_1d','purchase_view_7d','purchase_view_30d','purchase_incremental','unique_visit_view_1d',
  'unique_visit_view_7d','unique_visit_view_30d','unique_visit_incremental','lead_view_1d','lead_view_7d','lead_view_30d','lead_incremental')) as p
(
    channel,
    subchannel,
    store_brand_name,
    store_brand_abbr,
    store_id,
    ad_id,
    ad_name,
    campaign_id,
    date,
    creative_code,
    publisher,
    rotation,
    vip_view_1d,
    vip_view_7d,
    vip_view_30d,
    vip_incremental,
    vip_men_view_1d,
    vip_men_view_7d,
    vip_men_view_30d,
    vip_men_incremental,
    purchase_view_1d,
    purchase_view_7d,
    purchase_view_30d,
    purchase_incremental,
    unique_visit_view_1d,
    unique_visit_view_7d,
    unique_visit_view_30d,
    unique_visit_incremental,
    lead_view_1d,
    lead_view_7d,
    lead_view_30d,
    lead_incremental
);

--linear

--1).
--grabbing the fields needed from the conversion table and creating a new column for conversion values
create or replace temporary table _linear as
select
    'tv+streaming' as channel,
    'tv' as subchannel,
    case when lower(creative_name) ilike '%flm%' then 'Fabletics Men'
        when lower(creative_name) ilike '%yty%' then 'Yitty'
        when lower(creative_name) ilike '%scrubs%' then 'Fabletics Scrubs'
    else t3.store_brand end as store_brand_name,
    iff(store_brand_name='Fabletics Men','FLM',store_brand_abbr) as store_brand_abbr,
    t3.store_id,
    iff(right(spot_id, 1) = '_', concat(spot_id,creative_code), concat(spot_id, '_', creative_code)) as ad_id,
    creative_name as ad_name,
    spot_id as campaign_id,
    cast(spot_datetime as date) as date,
    creative_code,
    'null' as publisher,
    rotation,
    lift,

    --creating conversion window column
    case when lower(conversion_metric) = 'ttm_vipconversion' then 'vip_incremental'
    when lower(conversion_metric) = 'ttm_ordercompleted' then 'purchase_incremental'
    when lower(conversion_metric) = 'ttm_vipconversion_men' then 'vip_men_incremental'
    when lower(conversion_metric) = 'uvs' then 'unique_visit_incremental'
    when lower(conversion_metric) = 'ttm_completeregistration' then 'lead_incremental'
    end as conversion_window

from lake.tatari.linear_conversions t1
join lake_view.sharepoint.med_account_mapping_media t2 on t1.account_name = t2.source_id
    and lower(t2.source) ilike '%tatari%'
join edw_prod.data_model.dim_store t3 on t2.store_id = t3.store_id;

--2).
--pivot the conversion_window values into columns
create or replace temporary table _linear_pivoted as
select
    channel,
    subchannel,
    store_brand_name,
    store_brand_abbr,
    store_id,
    ad_id,
    ad_name,
    campaign_id,
    date,
    creative_code,
    publisher,
    rotation,
    vip_incremental,
    vip_men_incremental,
    purchase_incremental,
    unique_visit_incremental,
    lead_incremental
from _linear
pivot(sum(lift) for conversion_window in ('vip_incremental','vip_men_incremental','purchase_incremental','unique_visit_incremental','lead_incremental')) as p
(
    channel,
    subchannel,
    store_brand_name,
    store_brand_abbr,
    store_id,
    ad_id,
    ad_name,
    campaign_id,
    date,
    creative_code,
    publisher,
    rotation,
    vip_incremental,
    vip_men_incremental,
    purchase_incremental,
    unique_visit_incremental,
    lead_incremental

);

--3).
--create final linear table with all necessary conversion combos in order to union both tables
create or replace temporary table _linear_final as
select
    channel,
    subchannel,
    store_brand_name,
    store_brand_abbr,
    store_id,
    ad_id,
    ad_name,
    campaign_id,
    date,
    creative_code,
    publisher,
    rotation,

    0 as vip_view_1d,
    0 as vip_view_7d,
    0 as vip_view_30d,
    vip_incremental,
    0 as vip_men_view_1d,
    0 as vip_men_view_7d,
    0 as vip_men_view_30d,
    vip_men_incremental,
    0 as purchase_view_1d,
    0 as purchase_view_7d,
    0 as purchase_view_30d,
    purchase_incremental,
    0 as unique_visit_view_1d,
    0 as unique_visit_view_7d,
    0 as unique_visit_view_30d,
    unique_visit_incremental,
    0 as lead_view_1d,
    0 as lead_view_7d,
    0 as lead_view_30d,
    lead_incremental
from _linear_pivoted;


create or replace transient table reporting_media_base_prod.tatari.conversions_credit_on_spend_date as
select
    *,
    current_timestamp()::timestamp_ltz as conversion_meta_create_datetime,
    current_timestamp()::timestamp_ltz as conversion_meta_update_datetime
from (
         select *
         from _streaming_pivoted
         union
         select *
         from _linear_final
     );
