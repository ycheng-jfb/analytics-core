
---------------------------------------------------------------
-- mmm outputs

create or replace temporary table _mmm_outputs as
select case when lower(brand) = 'jf' then 'JustFab'
            when lower(brand) = 'sd' then 'ShoeDazzle'
            when lower(brand) = 'fk' then 'FabKids'
            when lower(brand) = 'fl' then 'Fabletics'
            when lower(brand) = 'flm' then 'Fabletics Men'
            when lower(brand) = 'sx' then 'Savage X'
            end as store_brand_name,
       date::date as date,
       case when feature in ('agedlead_clicks','crosspromo_clicks') then 'base'
            else feature_type end as feature_type, -- may need to update for FK
       feature as feature_raw,
       case when replace(final_grouping,'_',' ') = 'programmatic>tradedesk' then 'ttd programmatic'
            else replace(replace(final_grouping,'_',' '),' 2','') end as feature,
       modeled_metric,
       modeled_signal,
       modeled_signal_scaled,
       impressions,
       clicks,
       "impact score" as impact_score,
       opens as opens,
       "pixel vips" as pixel_vips,
       sends,
       signal,
       spend,
       "spend incl credits" as spend_with_credits,
       predicted_vips as predicted_vips_feature,
       channel_pct_of_predicted_credit as feature_pct_predicted_credit,
       channel_pct_of_marketing_predicted_credit as feature_pct_marketing_predicted_credit,
       attributed_vips_with_base as feature_attributed_vips_with_base,
       attributed_vips_without_base as feature_attributed_vips_without_base,
       spend / iff(pixel_vips = 0, null, pixel_vips) as pixel_cac_on_date,
       spend / iff(feature_attributed_vips_with_base = 0, null, feature_attributed_vips_with_base) as mmm_cac_on_date_with_base,
       spend / iff(feature_attributed_vips_without_base = 0, null, feature_attributed_vips_without_base) as mmm_cac_on_date_without_base,
       total_daily_predicted_vips,
       total_daily_predicted_marketing_vips,
       "vip conversions exlc affiliate" as actual_vips_excl_affiliate,
       vip_conversions as actual_total_vips,
       last_updated

from reporting_media_base_prod.attribution.mmm_outputs
where feature_type is not null
    and date < to_date(current_date());

---------------------------------------------------------------
-- forecast targets (NA)

create or replace temporary table _forecast as
select
    case when lower(store_tab_abbreviation) in ('fl-m-o-na') then 'Fabletics Men'
            else store_brand end as store_brand_name,
    store_region as region,

    date as month,
    sum(media_spend)/sum(total_vips_on_date) as total_vip_cac_forecast

from reporting_media_prod.attribution.acquisition_budget_targets_cac
where lower(source) in ('forecast-btfx')
    and lower(store_tab_abbreviation) in ('fl-m-o-na','fl-w-o-na','jf-na','sd-na','fk-na','sx-o-na')
    and date <= date_trunc(month,current_date())
group by 1,2,3;


---------------------------------------------------------------
-- media outlook (NA only)
create or replace temporary table _media_outlook_enteringmonth as
select store_brand_name,
       month,
       total_vip_cac as total_vip_cac_outlook_entering
from reporting_media_base_prod.dbo.media_overall_budget_outlook_actuals
where lower(segment) = 'media outlook entering month'
    and lower(store_brand_name) in ('fabletics', 'savage x', 'justfab', 'shoedazzle', 'fabletics men', 'fabkids')
    and channel = 'all'
    and lower(region) = 'na';

create or replace temporary table _media_outlook_midmonth as
select store_brand_name,
       month,
       total_vip_cac as total_vip_cac_outlook_midmonth
from reporting_media_base_prod.dbo.media_overall_budget_outlook_actuals
where lower(segment) = 'media outlook mid-month'
    and lower(store_brand_name) in ('fabletics', 'savage x', 'justfab', 'shoedazzle', 'fabletics men', 'fabkids')
    and channel = 'all'
    and lower(region) = 'na';

---------------------------------------------------------------


create or replace temporary table _final_output as
select c.*,

       f.total_vip_cac_forecast,
       me.total_vip_cac_outlook_entering,
       mm.total_vip_cac_outlook_midmonth,
       coalesce(mm.total_vip_cac_outlook_midmonth,me.total_vip_cac_outlook_entering) as total_vip_cac_media_outlook,

       -- target adjustment calculations
       -- cac moving averages (10d)
       avg(pixel_cac_on_date) over (partition by c.store_brand_name, c.feature order by c.date rows between 10 preceding and current row) as pixel_cac_10d_ma,

       -- pixel cac to mmm
       1 + (pixel_cac_10d_ma - mmm_cac_on_date_without_base)
               / iff(mmm_cac_on_date_without_base = 0, null, mmm_cac_on_date_without_base) as scaling_factor,
       pixel_cac_on_date / iff(scaling_factor = 0, null, scaling_factor) as adjusted_pixel_cac,

      -- adjusted pixel cac to target
      (adjusted_pixel_cac - total_vip_cac_forecast)
            / iff(total_vip_cac_forecast = 0, null, total_vip_cac_forecast) as forecast_adjusted_cac_pct_diff,
      (pixel_cac_on_date / iff((1 + forecast_adjusted_cac_pct_diff) = 0, null, (1 + forecast_adjusted_cac_pct_diff))) as new_pixel_target_forecast,

      (adjusted_pixel_cac - total_vip_cac_media_outlook)
          / iff(total_vip_cac_media_outlook = 0, null, total_vip_cac_media_outlook) as outlook_adjusted_cac_pct_diff,
      (pixel_cac_on_date / iff((1 + outlook_adjusted_cac_pct_diff) = 0, null, (1 + outlook_adjusted_cac_pct_diff))) as new_pixel_target_outlook,

       current_timestamp()::timestamp_ltz as process_run_datetime


from _mmm_outputs c
left join _forecast f on date_trunc(month,c.date) = f.month
    and c.store_brand_name = f.store_brand_name
left join _media_outlook_enteringmonth me on date_trunc(month,c.date) = me.month
    and c.store_brand_name = me.store_brand_name
left join _media_outlook_midmonth mm on date_trunc(month,c.date) = mm.month
    and c.store_brand_name = mm.store_brand_name;


------------------------------------------------------------------------------------

create or replace transient table reporting_media_prod.attribution.mmm_outputs_and_target_setting as
select v.*,
       case when feature_type = 'base' or modeled_metric = 'not modeled' then false else coalesce(c.is_optimizable, true) end as is_optimizable_daily
from _final_output v
left join lake_view.sharepoint.med_optimizable_channels c on c.store_brand_name = v.store_brand_name
        and case when c.channel = 'programmatic>tradedesk' then v.feature = 'ttd programmatic'
                        else c.channel = v.feature end;
