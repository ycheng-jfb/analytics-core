
create or replace transient table reporting_media_prod.attribution.top_down_media_mix_planner as
select v.store_brand_name,
       to_date(date) as date,
       case when spend_change = '0' then 'Yesterday Spend'
           when spend_change = '0.15' then 'Increase Spend by 15%'
           when spend_change = '-0.15' then 'Decrease Spend by 15%'
           when spend_change = '0.1' then 'Increase Spend by 10%'
           when spend_change = '-0.1' then 'Decrease Spend by 10%'
           end as set_spend_level,
       spend_change,
       case when v.channel = 'programmatic>tradedesk' then 'programmatic ttd'
           else replace(v.channel, '>', ' ') end as channel,
       coalesce(c.is_optimizable, true) as is_optimizable,
       feature,
       feature_value as stimulation,
       cps,
       cps_transform,
       spend as yesterday_spend,
       vips as yesterday_vips,
       optimal_spend as recommended_spend,
       optimal_vips as predicted_vips
from reporting_media_base_prod.attribution.top_down_vip_dash v
left join lake_view.sharepoint.med_optimizable_channels c on c.channel = v.channel
    and c.store_brand_name = v.store_brand_name;



------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
-- old logic --

-- set spend_date = (select max(to_date(date)) from reporting_media_base_prod.attribution.top_down_vip_dash);
--
-- create or replace transient table med_db_reporting.attribution.top_down_media_mix_planner as
-- with _modeled_channels as (
--     select distinct store_brand_name,
--                     case
--                         when channel ilike '%fb+ig%' then 'fb+ig'
--                         when channel = 'programmatic>tradedesk' then 'programmatic'
--                         when channel ilike '%youtube%' then 'youtube'
--                         when channel ilike any ('%tv%', '%streaming%') then 'tv+streaming'
--                         when channel ilike any ('%google display%','%google discovery%') then 'programmatic-gdn'
--                         else channel end as channel
--     from reporting_media_base_prod.attribution.top_down_vip_dash
-- )
-- , _fmc as (
--     select case when is_mens_flag = 1 then 'Fabletics Men' else store_brand end as store_brand_name,
--            media_cost_date,
--            case when lower(store_brand_abbr) = 'sx' and channel = 'influencers' then 'remove' -- remove influencer spend for sxf
--                 when lower(store_brand_abbr) = 'sx' and channel = 'programmatic'
--                          and (subchannel ilike '%dooh%' or vendor ilike '%milkmoney%') then 'dooh'
--                          else channel end as channel,
--            sum(cost) as spend
--     from med_db_reporting.dbo.vw_fact_media_cost fmc
--     join edw.data_model.dim_store ds on ds.store_id = fmc.store_id
--     where lower(store_brand_abbr) in ('fl', 'sx', 'sd', 'jf')
--       and lower(store_country) = 'us'
--       and media_cost_date = $spend_date
--       and cost > 0
--     group by 1,2,3
-- )
-- , _unmodeled_spend as (
--     select f.channel,
--             null as feature,
--             null as feature_value,
--             spend,
--             0 as vips,
--             0 as original_vips,
--             0 as optimal_spend,
--             0 as optimal_vips,
--             0 as optimal_original_vips,
--             0 as cps,
--             0 as cps_transform,
--             0 as scaling_coef,
--             0 as a,
--             0 as b,
--             0 as alpha,
--             media_cost_date as date,
--             f.store_brand_name,
--             'not modeled' as is_modeled
--     from _fmc f
--     left join _modeled_channels mc on f.channel = mc.channel
--         and f.store_brand_name = mc.store_brand_name
--     where mc.channel is null
--     )
--
-- select * from _unmodeled_spend
-- where channel != 'remove'
-- union
-- select *, 'modeled' as is_modeled
-- from reporting_media_base_prod.attribution.top_down_vip_dash;
