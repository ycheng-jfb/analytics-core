
-- model outputs --

create or replace transient table reporting_media_prod.dbo.friction_finder_model_outputs as
select brand as store_brand_name,
       datetime as session_datehour,
       channel_group,
       rate_name,
       rate_actual,
       rate_predicted,
       rate_lower_bound,
       rate_upper_bound,
       cast(over_index as boolean) as over_index_boolean,
       cast(under_index as boolean) as under_index_boolean,
       last_updated
from reporting_media_prod.dbo.friction_finder_hourly_rates_and_preds;


create or replace transient table reporting_media_prod.dbo.friction_finder_model_outputs_daily as
select brand as store_brand_name,
       to_date(datetime) as session_date,
       channel_group,
       rate_name,
       rate_actual,
       rate_predicted,
       rate_lower_bound,
       rate_upper_bound,
       cast(over_index as boolean) as over_index_boolean,
       cast(under_index as boolean) as under_index_boolean,
       last_updated
from reporting_media_prod.dbo.friction_finder_daily_rates_and_preds;
