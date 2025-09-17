-- Monthly Top Down Planner Datasource
CREATE OR REPLACE TEMPORARY TABLE _top_down_rolling_spend_forecast_with_monthly_optimizable_mix AS (
SELECT * FROM reporting_media_base_prod.ATTRIBUTION.top_down_rolling_spend_forecast_with_monthly_optimizable_mix_fl_us
UNION ALL (SELECT * FROM reporting_media_base_prod.ATTRIBUTION.top_down_rolling_spend_forecast_with_monthly_optimizable_mix_flm_us)
UNION ALL (SELECT * FROM reporting_media_base_prod.ATTRIBUTION.top_down_rolling_spend_forecast_with_monthly_optimizable_mix_sx_us)
UNION ALL (SELECT * FROM reporting_media_base_prod.ATTRIBUTION.top_down_rolling_spend_forecast_with_monthly_optimizable_mix_jf_us)
UNION ALL (SELECT * FROM reporting_media_base_prod.ATTRIBUTION.top_down_rolling_spend_forecast_with_monthly_optimizable_mix_sd_us)
UNION ALL (SELECT * FROM reporting_media_base_prod.ATTRIBUTION.top_down_rolling_spend_forecast_with_monthly_optimizable_mix_fk_us)
);

set last_updated = (select max(last_updated) from _top_down_rolling_spend_forecast_with_monthly_optimizable_mix);

create or replace transient table reporting_media_prod.attribution.top_down_monthly_media_mix_planner as
select

    store_brand_name,
    channel,
    date,
    budget_month,
    iff(locked_channel = true,'Not Modeled','Modeled') as is_modeled,
    $last_updated as last_updated,

    -- monthly top down model outputs
    sum(flat_daily_channel_outlook_spend) as outlook_spend,
    sum(flat_daily_channel_outlook_predicted_vips) as predicted_outlook_vips,
    sum(flat_daily_channel_outlook_lower_spend) as lower_spend,
    sum(flat_daily_channel_outlook_upper_spend) as upper_spend,
    sum(cps) as cps,
    sum(cpm) as cpm,
    sum(cpc) as cpc,
    sum(flat_daily_channel_optimal_spend) as optimal_spend,
    sum(flat_daily_channel_optimal_predicted_vips) as predicted_optimal_vips,
    sum(daily_channel_outlook_spend_from_day_one) as daily_channel_outlook_spend_from_day_one,
    sum(daily_channel_outlook_from_day_one_predicted_vips) as daily_channel_outlook_from_day_one_predicted_vips,
    sum(daily_channel_optimal_from_day_one_spend) as daily_channel_optimal_from_day_one_spend,
    sum(daily_channel_optimal_from_day_one_predicted_vips) as daily_channel_optimal_from_day_one_predicted_vips,

    -- daily top down outputs
    sum(actual_spend) as actual_spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(actual_spend_filled_forward_with_outlook_pct_of_rolling_forecast) as actual_outlook_spend,
    sum(actual_spend_filled_forward_with_optimal_pct_of_rolling_forecast) as actual_optimal_spend,
    sum(optimal_spend_from_actual_total) mtd_optimal_spend,
    sum(predicted_vips_if_followed_optimal_spend_from_actual_total_by_channel) as mtd_predicted_vips_from_optimal_spend

from _top_down_rolling_spend_forecast_with_monthly_optimizable_mix
group by 1,2,3,4,5;
