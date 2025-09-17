
create or replace transient table reporting_media_prod.dbo.med61_hourly_projections_vs_actuals as
select
    t1.store_brand_name,
    t1.region,
    t1.ds as datetime,
    t1.forecast_start_time,
    t3.lasthour_rs_spend as actual_spend,
    t1.today_rs_spend_yesterday_forecast_eod as spend_forecast_yesterday,
    t2.today_rs_spend_yesterday_forecast_eod as spend_forecast_yesterday_eod,
    t1.today_rs_spend_lastweek_forecast_eod as spend_forecast_lastweek,
    t2.today_rs_spend_lastweek_forecast_eod as spend_forecast_lastweek_eod,
    t1.today_rs_spend_lastmonth_forecast_eod as spend_forecast_lastmonth,
    t2.today_rs_spend_lastmonth_forecast_eod as spend_forecast_lastmonth_eod,
    t1.today_rs_spend_lastyear_forecast_eod as spend_forecast_lastyear,
    t2.today_rs_spend_lastyear_forecast_eod as spend_forecast_lastyear_eod,
    t1.today_rs_spend_lastyear_same_day_forecast_eod as spend_forecast_lastyear_sameday,
    t2.today_rs_spend_lastyear_same_day_forecast_eod as spend_forecast_lastyear_sameday_eod,
    t3.lasthour_rs_vips as actual_vips,
    t1.today_rs_vips_yesterday_forecast_eod as vips_forecast_yesterday,
    t2.today_rs_vips_yesterday_forecast_eod as vips_forecast_yesterday_eod,
    t1.today_rs_vips_lastweek_forecast_eod as vips_forecast_lastweek,
    t2.today_rs_vips_lastweek_forecast_eod as vips_forecast_lastweek_eod,
    t1.today_rs_vips_lastmonth_forecast_eod as vips_forecast_lastmonth,
    t2.today_rs_vips_lastmonth_forecast_eod as vips_forecast_lastmonth_eod,
    t1.today_rs_vips_lastyear_forecast_eod as vips_forecast_lastyear,
    t2.today_rs_vips_lastyear_forecast_eod as vips_forecast_lastyear_eod,
    t1.today_rs_vips_lastyear_same_day_forecast_eod as vips_forecast_lastyear_sameday,
    t2.today_rs_vips_lastyear_same_day_forecast_eod as vips_forecast_lastyear_sameday_eod
from reporting_media_prod.dbo.med61_hourly_projections_stored t1
left join (select distinct * from reporting_media_prod.dbo.med61_hourly_projections_stored where ds != forecast_start_time) t2 on t1.store_brand_name = t2.store_brand_name and t1.region = t2.region and t1.ds = t2.forecast_start_time
left join (select distinct * from reporting_media_prod.dbo.med61_hourly_projections_stored where ds = forecast_start_time) t3 on t1.store_brand_name = t3.store_brand_name and t1.region = t3.region and t1.ds = dateadd('hour',-1,t3.ds)
where t1.ds = t1.forecast_start_time;
