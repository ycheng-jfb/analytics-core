
-- daily scrubs vips by gender
create or replace temporary table _scrubs_pct_womens as
select date,
       sum(new_vips) as total_vips,
       sum(iff(lower(customer_gender) != 'm', new_vips, 0)) as womens_vips,
       iff(total_vips = 0, 0, womens_vips / iff(total_vips = 0, null, total_vips)) as womens_pct_total
from edw_prod.analytics_base.acquisition_media_spend_daily_agg a
join edw_prod.data_model_jfb.dim_store ds on ds.store_id = a.event_store_id
where lower(currency_type) = 'usd'
    and lower(date_object) = 'placed'
    and is_scrubs_customer = true
    and new_vips > 0
group by 1;


-- scrubs total spend
create or replace temporary table _total_scrubs_spend as
select coalesce(womens_pct_total,1) as pct_total_w,
       round(cost * pct_total_w,4) as cost_w,
       round(impressions * pct_total_w) as impressions_w,
       round(clicks * pct_total_w) as clicks_w,
       cost - cost_w as cost_m,
       impressions - impressions_w as impressions_m,
       clicks - clicks_w as clicks_m,
       fmc.*
from reporting_media_prod.dbo.vw_fact_media_cost fmc
join edw_prod.data_model_jfb.dim_store ds on ds.store_id = fmc.store_id
left join _scrubs_pct_womens s on s.date = to_date(fmc.media_cost_date)
where is_scrubs_flag = true;

create or replace temporary table _scrubs_spend_split as
-- scrubs womens
select channel,
       subchannel,
       vendor,
       spend_type,
       media_cost_date,
       targeting,
       source,
       cost_w as cost,
       impressions_w as impressions,
       clicks_w as clicks,
       spend_iso_currency_code,
       store_iso_currency_code,
       spend_date_eur_conv_rate,
       spend_date_usd_conv_rate,
       local_store_conv_rate,
       store_id,
       is_specialty_store,
       specialty_store,
       0 as is_mens_flag,
       is_scrubs_flag
from _total_scrubs_spend
union all
-- scrubs mens
select channel,
       subchannel,
       vendor,
       spend_type,
       media_cost_date,
       targeting,
       source,
       cost_m as cost,
       impressions_m as impressions,
       clicks_m as clicks,
       spend_iso_currency_code,
       store_iso_currency_code,
       spend_date_eur_conv_rate,
       spend_date_usd_conv_rate,
       local_store_conv_rate,
       store_id,
       is_specialty_store,
       specialty_store,
       1 as is_mens_flag,
       is_scrubs_flag
from _total_scrubs_spend;


create or replace transient table reporting_media_prod.dbo.fact_media_cost_scrubs_split as
select channel,
       subchannel,
       vendor,
       spend_type,
       media_cost_date,
       targeting,
       source,
       cost,
       impressions,
       clicks,
       spend_iso_currency_code,
       store_iso_currency_code,
       spend_date_eur_conv_rate,
       spend_date_usd_conv_rate,
       local_store_conv_rate,
       store_id,
       is_specialty_store,
       specialty_store,
       is_mens_flag,
       ifnull(is_scrubs_flag,false) as is_scrubs_flag
from reporting_media_prod.dbo.vw_fact_media_cost
where ifnull(is_scrubs_flag,false) = false
union
select * from _scrubs_spend_split;
