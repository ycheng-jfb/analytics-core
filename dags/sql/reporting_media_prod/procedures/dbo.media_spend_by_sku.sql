
create or replace temporary table _all_channel as
select store_brand_name,
       region,
       date,
       lower(channel) as channel,
       ad_id,
       image_url as ad_image,
       link_to_post as ad_link,
       sku1,
       sku2,
       sku3,
       sku4,
       sku5,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       count(distinct ad_id) as ads_live,
       sum(ui_optimization_pixel_vip) as pixel_vips
from reporting_media_prod.dbo.all_channel_optimization
where lower(store_brand_name) in ('fabletics','fabletics men','yitty','fabletics scrubs')
    and lower(region) = 'na'
    and date >= '2023-04-01'
    and lower(channel) in ('fb+ig','tiktok','youtube','snapchat','pinterest')
    and sku1 is not null
    and lower(sku1) != 'none'
group by 1,2,3,4,5,6,7,8,9,10,11,12;


create or replace temporary table _spend_pivot as
select store_brand_name,
       region,
       ad_id,
       ad_image,
       ad_link,
       date,
       sku1,
       sku2,
       sku3,
       sku4,
       sku5,

       -- totals
       sum(spend) as total_spend,
       sum(impressions) as total_impressions,
       sum(clicks) as total_clicks,
       sum(ads_live) as total_ads_live,
       sum(pixel_vips) as total_pixel_vips,

       -- fb+ig
       sum(iff(channel = 'fb+ig', spend, 0)) as fbig_spend,
       sum(iff(channel = 'fb+ig', impressions, 0)) as fbig_impressions,
       sum(iff(channel = 'fb+ig', clicks, 0)) as fbig_clicks,
       sum(iff(channel = 'fb+ig', ads_live, 0)) as fbig_adslive,
       sum(iff(channel = 'fb+ig', pixel_vips, 0)) as fbig_pixel_vips,

       -- tiktok
       sum(iff(channel = 'tiktok', spend, 0)) as tiktok_spend,
       sum(iff(channel = 'tiktok', impressions, 0)) as tiktok_impressions,
       sum(iff(channel = 'tiktok', clicks, 0)) as tiktok_clicks,
       sum(iff(channel = 'tiktok', ads_live, 0)) as tiktok_adslive,
       sum(iff(channel = 'tiktok', pixel_vips, 0)) as tiktok_pixel_vips,

       -- youtube
       sum(iff(channel = 'youtube', spend, 0)) as youtube_spend,
       sum(iff(channel = 'youtube', impressions, 0)) as youtube_impressions,
       sum(iff(channel = 'youtube', clicks, 0)) as youtube_clicks,
       sum(iff(channel = 'youtube', ads_live, 0)) as youtube_adslive,
       sum(iff(channel = 'youtube', pixel_vips, 0)) as youtube_pixel_vips,

       -- pinterest
       sum(iff(channel = 'pinterest', spend, 0)) as pinterest_spend,
       sum(iff(channel = 'pinterest', impressions, 0)) as pinterest_impressions,
       sum(iff(channel = 'pinterest', clicks, 0)) as pinterest_clicks,
       sum(iff(channel = 'pinterest', ads_live, 0)) as pinterest_adslive,
       sum(iff(channel = 'pinterest', pixel_vips, 0)) as pinterest_pixel_vips,

       -- snapchat
       sum(iff(channel = 'snapchat', spend, 0)) as snapchat_spend,
       sum(iff(channel = 'snapchat', impressions, 0)) as snapchat_impressions,
       sum(iff(channel = 'snapchat', clicks, 0)) as snapchat_clicks,
       sum(iff(channel = 'snapchat', ads_live, 0)) as snapchat_adslive,
       sum(iff(channel = 'snapchat', pixel_vips, 0)) as snapchat_pixel_vips

from _all_channel
group by 1,2,3,4,5,6,7,8,9,10,11;

create or replace temporary table _sku_pivot as
select *
from _spend_pivot
unpivot (sku for ad_sku_rank in (sku1, sku2, sku3, sku4, sku5));

create or replace temporary table _final_sku_dataset as
select store_brand_name,
       region,
       ad_id,
       ad_image,
       ad_link,
       date,
       sku,
       sum(total_ads_live) as total_ads_live,
       sum(total_spend) as total_spend,
       sum(total_impressions) as total_impressions,
       sum(total_clicks) as total_clicks,
       sum(total_pixel_vips) as total_pixel_vips,

       sum(fbig_adslive) as fbig_adslive,
       sum(fbig_spend) as fbig_spend,
       sum(fbig_impressions) as fbig_impressions,
       sum(fbig_clicks) as fbig_clicks,
       sum(fbig_pixel_vips) as fbig_pixel_vips,

       sum(youtube_adslive) as youtube_adslive,
       sum(youtube_spend) as youtube_spend,
       sum(youtube_impressions) as youtube_impressions,
       sum(youtube_clicks) as youtube_clicks,
       sum(youtube_pixel_vips) as youtube_pixel_vips,

       sum(snapchat_adslive) as snapchat_adslive,
       sum(snapchat_spend) as snapchat_spend,
       sum(snapchat_impressions) as snapchat_impressions,
       sum(snapchat_clicks) as snapchat_clicks,
       sum(snapchat_pixel_vips) as snapchat_pixel_vips,

       sum(pinterest_adslive) as pinterest_adslive,
       sum(pinterest_spend) as pinterest_spend,
       sum(pinterest_impressions) as pinterest_impressions,
       sum(pinterest_clicks) as pinterest_clicks,
       sum(pinterest_pixel_vips) as pinterest_pixel_vips,

       sum(tiktok_adslive) as tiktok_adslive,
       sum(tiktok_spend) as tiktok_spend,
       sum(tiktok_impressions) as tiktok_impressions,
       sum(tiktok_clicks) as tiktok_clicks,
       sum(tiktok_pixel_vips) as tiktok_pixel_vips

from _sku_pivot
where contains(sku,'-')
group by 1,2,3,4,5,6,7;


create or replace temporary table _product_data_raw as
select distinct product_sku,
                current_showroom_date,
                replace(product_name,'* ','') as product_name,
                product_category,
                department,
                category,
                color,
                image_url,
                is_active,
                meta_update_datetime,
                row_number() over (partition by product_sku order by meta_update_datetime desc) as rn
from edw_prod.data_model.dim_product
where store_id in (52,241)
    and is_active = true;

create or replace temporary table _product_data as
select distinct product_sku,
                current_showroom_date,
                product_name,
                product_category,
                department,
                category,
                color,
                image_url
from _product_data_raw
where rn = 1;


create or replace temporary table _media_spend_by_product as
select *,
       dense_rank() over (partition by store_brand_name, date order by fbig_spend desc nulls last) as daily_fb_spend_rank,
       dense_rank() over (partition by store_brand_name, date order by tiktok_spend desc nulls last) as daily_tiktok_spend_rank,
       dense_rank() over (partition by store_brand_name, date order by youtube_spend desc nulls last) as daily_youtube_spend_rank,
       dense_rank() over (partition by store_brand_name, date order by snapchat_spend desc nulls last) as daily_snap_spend_rank,
       dense_rank() over (partition by store_brand_name, date order by pinterest_spend desc nulls last) as daily_pinterest_spend_rank
from _final_sku_dataset d
join _product_data p on d.sku = p.product_sku;

create or replace temporary table _sku_data as
select distinct sku,
                size,
                meta_update_datetime,
                row_number() over (partition by sku order by meta_update_datetime desc) as rn
from edw_prod.data_model.dim_product
where store_id in (52,241)
  and is_active = true;


create or replace temporary table _inventory_levels as
select m.date,
       m.product_sku,
       m.sku,
       max(size) as size,
       sum(ty_bop_available_to_sell_qty) as bod_inventory,
       sum(ty_eop_available_to_sell_qty) as eod_inventory,
       count(distinct m.sku) over (partition by product_sku) as count_sizes_offered,
       iff(eod_inventory = 0, 1, 0) as count_sizes_sold_out
from edw_prod.reporting.merch_planning_item_sku m
left join (select distinct sku, size from _sku_data where rn = 1) p on m.sku = p.sku
where lower(date_type) in ('placed','inventory')
    and lower(store_brand) in ('fabletics','yitty')
    and lower(country) = 'us'
    and lower(store_type) != 'retail'
    and date >= '2023-04-01'
group by 1,2,3;


create or replace temporary table _inventory_product_grain as
select date,
       product_sku,
       sum(bod_inventory) as bod_inventory,
       sum(eod_inventory) as eod_inventory,
       max(count_sizes_offered) as count_sizes_offered,
       sum(count_sizes_sold_out) as count_sizes_sold_out_on_date
from _inventory_levels
group by 1,2;

create or replace transient table reporting_media_prod.dbo.media_spend_by_sku as
-- create or replace temporary table _final as
select s.*,
       coalesce(bod_inventory,0) as bod_inventory,
       coalesce(eod_inventory,0) as eod_inventory,
       count_sizes_offered,
       count_sizes_sold_out_on_date,
       iff(count_sizes_sold_out_on_date > 0, 1, 0) as size_broken_on_date,
       iff(eod_inventory = 0, 1, 0) as all_sold_out_on_date
from _media_spend_by_product s
left join _inventory_product_grain i on i.product_sku = s.product_sku
    and i.date = s.date
where s.date < current_date();
