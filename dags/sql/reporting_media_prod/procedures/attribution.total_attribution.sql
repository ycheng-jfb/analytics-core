------------------------------------------------------------------------------------
-- set date logic for session (start date on monday for week)

alter session set week_start = 1, week_of_year_policy = 1;

create or replace temporary table _dim_date as
select
    full_date,
    dayofweek(full_date) as day_of_week,
    case dayname(full_date)
        when 'Sun' then 'sunday'
        when 'Mon' then 'monday'
        when 'Tue' then 'tuesday'
        when 'Wed' then 'wednesday'
        when 'Thu' then 'thursday'
        when 'Fri' then 'friday'
        when 'Sat' then 'saturday'
    end as day_name_of_week,
    dayofmonth(full_date) as day_of_month,
    dayofyear(full_date) as day_of_year,
    weekofyear(full_date) as week_of_year,
    case monthname(full_date)
        when 'Jan' then 'january'
        when 'Feb' then 'february'
        when 'Mar' then 'march'
        when 'Apr' then 'april'
        when 'May' then 'may'
        when 'Jun' then 'june'
        when 'Jul' then 'july'
        when 'Aug' then 'august'
        when 'Sep' then 'september'
        when 'Oct' then 'october'
        when 'Nov' then 'november'
        when 'Dec' then 'december'
    end as month_name,
    date_trunc('month', full_date) as month_date,
    quarter(full_date) as calendar_quarter,
    year(full_date) as calendar_year,
    year(full_date)||'q'||quarter(full_date) as calendar_year_quarter
from edw_prod.data_model.dim_date;

------------------------------------------------------------------------------------
-- hard code dates to use for PoP comparisons (week, month, year) for WTD, MTD, YTD comparisons
-- ie: for comparing this month, MTD, to last month...we don't want to grab the full month so set specific start and end to grab the dates in the  "first 5 days of last month"
-- otherwise, we will compare PoP as normal for anything that's not in the current month

set start_date = '2022-01-01';
set cbl_start_date = '2024-01-01'; -- this is a variable used for the supplemental cac datasource for FL
set max_date = (select current_date() - 1);
set day_of_week = (select day_of_week from _dim_date where full_date = $max_date);
set day_of_month = (select day_of_month from _dim_date where full_date = $max_date);
set current_date_week_year = (select week_of_year from _dim_date where full_date = $max_date);

-- week dates
set current_week_start = (select $max_date - ($day_of_week-1));
set last_week_start = (select dateadd('week',-1,$current_week_start));
set last_week_end = (select $last_week_start + ($day_of_week-1));
set last_year_same_week_start = (select full_date from _dim_date where week_of_year = $current_date_week_year and year($max_date) = calendar_year + 1 and day_of_week = 1);
set last_year_same_week_end = (select $last_year_same_week_start + ($day_of_week-1));

create or replace temporary table _week_pop_dates as
select full_date as date from _dim_date
where (full_date between $current_week_start and $max_date)
or (full_date between $last_week_start and $last_week_end)
or (full_date between $last_year_same_week_start and $last_year_same_week_end);

-- month dates
set current_month_start = (select $max_date - ($day_of_month-1));
set last_month_start = (select dateadd('month',-1,$current_month_start));
set last_month_end = (select $last_month_start + ($day_of_month-1));
set last_year_same_month_start = (select dateadd('year',-1,$current_month_start));
set last_year_same_month_end = (select $last_year_same_month_start + ($day_of_month-1));

create or replace temporary table _month_pop_dates as
select full_date as date from _dim_date
where (full_date between $current_month_start and $max_date)
or (full_date between $last_month_start and $last_month_end)
or (full_date between $last_year_same_month_start and $last_year_same_month_end);

-- year dates
set current_year_start = date_trunc('year', $max_date);
set current_year_end = $max_date;
set last_year_start = date_trunc('year', dateadd('year', -1, $max_date));
set last_year_end = dateadd('year', -1, $max_date);

create or replace temporary table _year_pop_dates as
select full_date as date from _dim_date
where (full_date between $last_year_start and $last_year_end)
or (full_date between $current_year_start and $current_year_end);

------------------------------------------------------------------------------------

-- structure for each data pull (total performance and performance by channel): union daily, weekly, monthly, yearly performance for 1) complete periods and 2) non-complete periods to calculate "to-date"
-- temp tables in order of appearance, starting with total then by channel
-- total performance: overall business, overall business online only, sessions
-- channel performance: media mix, media mix (shopping split), hdyh, cbl, cbl (shopping split), mmm, session, fb+ig ui, google ads ui, other ui , additional session calcs)
-- last step: union all temp tables together!

------------------------------------------------------------------------------------
-- total acq metrics

create or replace temporary table _overall_business as
with _raw_overall_business as (
select 'daily' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    a.date,
    dateadd(year,-1,a.date) + delta_ly_comp_this_year as date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,a.date) = th.year
where currency = 'USD'
and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    date_trunc('week',date) as date,
    date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',date))) = calendar_year and week(date_trunc('week',date)) = week_of_year
where currency = 'USD'
and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
where currency = 'USD'
  and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
where currency = 'USD'
  and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    a.date,
    dateadd(year,-1,a.date) + delta_ly_comp_this_year as date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,a.date) = th.year
where currency = 'USD'
and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    date_trunc('week',date) as date,
    date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',date))) = calendar_year and week(date_trunc('week',date)) = week_of_year
where currency = 'USD'
and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
where currency = 'USD'
  and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
where currency = 'USD'
  and date >= $start_date
group by 1,2,3,4,5,6,7)
select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    row_number() over (partition by date_segment, store_brand_name, region, country order by date desc) as latest_date_flag,
    total_media_spend,
    total_leads,
    total_vips,
    total_cpl,
    total_vip_cac
from _raw_overall_business;

create or replace temporary table _overall_business_pop as
with _raw_overall_business_pop as (
select 'daily' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    a.date,
    dateadd(year,-1,a.date) + delta_ly_comp_this_year as date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,a.date) = th.year
where currency = 'USD'
and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    date_trunc('week',date) as date,
    date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',date))) = calendar_year and week(date_trunc('week',date)) = week_of_year
where date in (select * from _week_pop_dates)
and currency = 'USD'
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
where date in (select * from _month_pop_dates)
and currency = 'USD'
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
where date in (select * from _year_pop_dates)
and currency = 'USD'
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    a.date,
    dateadd(year,-1,a.date) + delta_ly_comp_this_year as date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,a.date) = th.year
where currency = 'USD'
and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    date_trunc('week',date) as date,
    date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',date))) = calendar_year and week(date_trunc('week',date)) = week_of_year
where date in (select * from _week_pop_dates)
and currency = 'USD'
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
where date in (select * from _month_pop_dates)
and currency = 'USD'
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as total_media_spend,
    sum(primary_leads) as total_leads,
    sum(total_vips_on_date) as total_vips,
    total_media_spend / iff(total_leads=0,null,total_leads) as total_cpl,
    total_media_spend / iff(total_vips=0,null,total_vips) as total_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
where date in (select * from _year_pop_dates)
and currency = 'USD'
group by 1,2,3,4,5,6,7)
select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    row_number() over (partition by date_segment, store_brand_name, region, country order by date desc) as latest_date_flag,
    total_media_spend,
    total_leads,
    total_vips,
    total_cpl,
    total_vip_cac
from _raw_overall_business_pop;

create or replace temporary table _total as
select distinct
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    '$' || to_varchar(t1.total_media_spend, '999,999,999,999') as "Total Media Spend",
    to_varchar(((t1.total_media_spend-t2.total_media_spend)/iff(t2.total_media_spend=0 or t2.total_media_spend is null,1,t2.total_media_spend)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Media Spend",
    to_varchar(((t1.total_media_spend-t3.total_media_spend)/iff(t3.total_media_spend=0 or t3.total_media_spend is null,1,t3.total_media_spend)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Media Spend",
    to_varchar(((t1.total_media_spend-t4.total_media_spend)/iff(t4.total_media_spend=0 or t4.total_media_spend is null,1,t4.total_media_spend)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Media Spend",
    to_varchar(t1.total_leads, '999,999,999,999') as "Total Leads",
    to_varchar(((t1.total_leads-t2.total_leads)/iff(t2.total_leads=0 or t2.total_leads is null,1,t2.total_leads)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Leads",
    to_varchar(((t1.total_leads-t3.total_leads)/iff(t3.total_leads=0 or t3.total_leads is null,1,t3.total_leads)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Leads",
    to_varchar(((t1.total_leads-t4.total_leads)/iff(t4.total_leads=0 or t4.total_leads is null,1,t4.total_leads)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Leads",
    to_varchar(t1.total_vips, '999,999,999,999') as "Total VIPs",
    to_varchar(((t1.total_vips-t2.total_vips)/iff(t2.total_vips=0 or t2.total_vips is null,1,t2.total_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total VIPs",
    to_varchar(((t1.total_vips-t3.total_vips)/iff(t3.total_vips=0 or t3.total_vips is null,1,t3.total_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total VIPs",
    to_varchar(((t1.total_vips-t4.total_vips)/iff(t4.total_vips=0 or t4.total_vips is null,1,t4.total_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total VIPs",
    '$' || to_varchar(t1.total_cpl, '999,999,999,999.00') as "Total CPL",
    to_varchar(((t1.total_cpl-t2.total_cpl)/iff(t2.total_cpl=0 or t2.total_cpl is null,1,t2.total_cpl)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total CPL",
    to_varchar(((t1.total_cpl-t3.total_cpl)/iff(t3.total_cpl=0 or t3.total_cpl is null,1,t3.total_cpl)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total CPL",
    to_varchar(((t1.total_cpl-t4.total_cpl)/iff(t4.total_cpl=0 or t4.total_cpl is null,1,t4.total_cpl)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total CPL",
    '$' || to_varchar(t1.total_vip_cac, '999,999,999,999.00') as "Total VIP CAC",
    to_varchar(((t1.total_vip_cac-t2.total_vip_cac)/iff(t2.total_vip_cac=0 or t2.total_vip_cac is null,1,t2.total_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total VIP CAC",
    to_varchar(((t1.total_vip_cac-t3.total_vip_cac)/iff(t3.total_vip_cac=0 or t3.total_vip_cac is null,1,t3.total_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total VIP CAC",
    to_varchar(((t1.total_vip_cac-t4.total_vip_cac)/iff(t4.total_vip_cac=0 or t4.total_vip_cac is null,1,t4.total_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total VIP CAC"
from _overall_business t1
-- join table to itself and bring in performance from the date needed for either the PoP, YoY Same Day, or YoY Same Date
left join _overall_business t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.region = t2.region and t1.country = t2.country -- for PoP calcs
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
          when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
          when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
          when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _overall_business t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.region = t3.region and t1.country = t3.country -- for YoY, same date
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
          when t1.date_segment = 'weekly' then t1.date_pop = t3.date
          when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _overall_business t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.region = t4.region and t1.country = t4.country -- for YoY, dame day (only needed for daily segment)
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag > 1
union all
select distinct
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    '$' || to_varchar(t1.total_media_spend, '999,999,999,999') as "Total Media Spend",
    to_varchar(((t1.total_media_spend-t2.total_media_spend)/iff(t2.total_media_spend=0 or t2.total_media_spend is null,1,t2.total_media_spend)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Media Spend",
    to_varchar(((t1.total_media_spend-t3.total_media_spend)/iff(t3.total_media_spend=0 or t3.total_media_spend is null,1,t3.total_media_spend)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Media Spend",
    to_varchar(((t1.total_media_spend-t4.total_media_spend)/iff(t4.total_media_spend=0 or t4.total_media_spend is null,1,t4.total_media_spend)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Media Spend",
    to_varchar(t1.total_leads, '999,999,999,999') as "Total Leads",
    to_varchar(((t1.total_leads-t2.total_leads)/iff(t2.total_leads=0 or t2.total_leads is null,1,t2.total_leads)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Leads",
    to_varchar(((t1.total_leads-t3.total_leads)/iff(t3.total_leads=0 or t3.total_leads is null,1,t3.total_leads)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Leads",
    to_varchar(((t1.total_leads-t4.total_leads)/iff(t4.total_leads=0 or t4.total_leads is null,1,t4.total_leads)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Leads",
    to_varchar(t1.total_vips, '999,999,999,999') as "Total VIPs",
    to_varchar(((t1.total_vips-t2.total_vips)/iff(t2.total_vips=0 or t2.total_vips is null,1,t2.total_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total VIPs",
    to_varchar(((t1.total_vips-t3.total_vips)/iff(t3.total_vips=0 or t3.total_vips is null,1,t3.total_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total VIPs",
    to_varchar(((t1.total_vips-t4.total_vips)/iff(t4.total_vips=0 or t4.total_vips is null,1,t4.total_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total VIPs",
    '$' || to_varchar(t1.total_cpl, '999,999,999,999.00') as "Total CPL",
    to_varchar(((t1.total_cpl-t2.total_cpl)/iff(t2.total_cpl=0 or t2.total_cpl is null,1,t2.total_cpl)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total CPL",
    to_varchar(((t1.total_cpl-t3.total_cpl)/iff(t3.total_cpl=0 or t3.total_cpl is null,1,t3.total_cpl)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total CPL",
    to_varchar(((t1.total_cpl-t4.total_cpl)/iff(t4.total_cpl=0 or t4.total_cpl is null,1,t4.total_cpl)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total CPL",
    '$' || to_varchar(t1.total_vip_cac, '999,999,999,999.00') as "Total VIP CAC",
    to_varchar(((t1.total_vip_cac-t2.total_vip_cac)/iff(t2.total_vip_cac=0 or t2.total_vip_cac is null,1,t2.total_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total VIP CAC",
    to_varchar(((t1.total_vip_cac-t3.total_vip_cac)/iff(t3.total_vip_cac=0 or t3.total_vip_cac is null,1,t3.total_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total VIP CAC",
    to_varchar(((t1.total_vip_cac-t4.total_vip_cac)/iff(t4.total_vip_cac=0 or t4.total_vip_cac is null,1,t4.total_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total VIP CAC"
from _overall_business_pop t1
-- doing the same thing has above, but the "_overall_business_pop" (or any _pop table below) will include the days for "to-date" comparisons
left join _overall_business_pop t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
          when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
          when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
          when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _overall_business_pop t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
          when t1.date_segment = 'weekly' then t1.date_pop = t3.date
          when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _overall_business_pop t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag = 1;

------------------------------------------------------------------------------------
-- total acq metrics - online only

create or replace temporary table _overall_business_online_only as
with _raw_overall_business_online_only as (
select 'daily' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    date,
    dateadd(year,-1,a.date) + delta_ly_comp_this_year as date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,a.date) = th.year
where currency = 'USD'
    and date >= $start_date
    and retail_customer = false
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    date_trunc('week',date) as date,
    date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',a.date))) = d.calendar_year and week(date_trunc('week',a.date)) = d.week_of_year
where currency = 'USD'
    and date >= $start_date
    and retail_customer = false
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac
where currency = 'USD'
    and date >= $start_date
    and retail_customer = false
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac
where currency = 'USD'
    and date >= $start_date
    and retail_customer = false
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    date,
    dateadd(year,-1,a.date) + delta_ly_comp_this_year as date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,a.date) = th.year
where currency = 'USD'
    and date >= $start_date
    and retail_customer = false
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    date_trunc('week',date) as date,
    date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',a.date))) = d.calendar_year and week(date_trunc('week',a.date)) = d.week_of_year
where currency = 'USD'
    and date >= $start_date
    and retail_customer = false
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac
where currency = 'USD'
    and date >= $start_date
    and retail_customer = false
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac
where currency = 'USD'
    and date >= $start_date
    and retail_customer = false
group by 1,2,3,4,5,6,7)
select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    row_number() over (partition by date_segment, store_brand_name, region, country order by date desc) as latest_date_flag,
    online_only_media_spend,
    online_only_leads,
    online_only_vips,
    online_only_cpl,
    online_only_vip_cac
from _raw_overall_business_online_only;

create or replace temporary table _overall_business_online_only_pop as
with _raw_overall_business_online_only_pop as (
select 'daily' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    date,
    dateadd(year,-1,a.date) + delta_ly_comp_this_year as date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,a.date) = th.year
where currency = 'USD'
    and date >= $start_date
    and retail_customer = false
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    date_trunc('week',date) as date,
    date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',a.date))) = d.calendar_year and week(date_trunc('week',a.date)) = d.week_of_year
where date in (select * from _week_pop_dates)
    and currency = 'USD'
    and retail_customer = false
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac
where date in (select * from _month_pop_dates)
    and currency = 'USD'
    and retail_customer = false
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    iff(lower(region)='na','US+CA',store_region) as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac
where date in (select * from _year_pop_dates)
    and currency = 'USD'
    and retail_customer = false
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    date,
    dateadd(year,-1,a.date) + delta_ly_comp_this_year as date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,a.date) = th.year
where currency = 'USD'
    and date >= $start_date
    and retail_customer = false
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    date_trunc('week',date) as date,
    date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac a
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',a.date))) = d.calendar_year and week(date_trunc('week',a.date)) = d.week_of_year
where date in (select * from _week_pop_dates)
    and currency = 'USD'
    and retail_customer = false
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac
where date in (select * from _month_pop_dates)
    and currency = 'USD'
    and retail_customer = false
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when is_fl_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    'all' as channel,
    sum(total_spend) as online_only_media_spend,
    sum(primary_leads) as online_only_leads,
    sum(total_vips_on_date) as online_only_vips,
    online_only_media_spend / iff(online_only_leads=0,null,online_only_leads) as online_only_cpl,
    online_only_media_spend / iff(online_only_vips=0,null,online_only_vips) as online_only_vip_cac
from reporting_media_prod.attribution.daily_acquisition_metrics_cac
where date in (select * from _year_pop_dates)
    and currency = 'USD'
    and retail_customer = false
group by 1,2,3,4,5,6,7)
select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    row_number() over (partition by date_segment, store_brand_name, region, country order by date desc) as latest_date_flag,
    online_only_media_spend,
    online_only_leads,
    online_only_vips,
    online_only_cpl,
    online_only_vip_cac
from _raw_overall_business_online_only_pop;

create or replace temporary table _total_online_only as
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    to_varchar(t1.online_only_leads, '999,999,999,999') as "Total Leads - Online Only",
    to_varchar(((t1.online_only_leads-t2.online_only_leads)/iff(t2.online_only_leads=0 or t2.online_only_leads is null,1,t2.online_only_leads)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Leads - Online Only",
    to_varchar(((t1.online_only_leads-t3.online_only_leads)/iff(t3.online_only_leads=0 or t3.online_only_leads is null,1,t3.online_only_leads)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Leads - Online Only",
    to_varchar(((t1.online_only_leads-t4.online_only_leads)/iff(t4.online_only_leads=0 or t4.online_only_leads is null,1,t4.online_only_leads)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Leads - Online Only",
    to_varchar(t1.online_only_vips, '999,999,999,999') as "Total VIPs - Online Only",
    to_varchar(((t1.online_only_vips-t2.online_only_vips)/iff(t2.online_only_vips=0 or t2.online_only_vips is null,1,t2.online_only_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total VIPs - Online Only",
    to_varchar(((t1.online_only_vips-t3.online_only_vips)/iff(t3.online_only_vips=0 or t3.online_only_vips is null,1,t3.online_only_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total VIPs - Online Only",
    to_varchar(((t1.online_only_vips-t4.online_only_vips)/iff(t4.online_only_vips=0 or t4.online_only_vips is null,1,t4.online_only_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total VIPs - Online Only",
    '$' || to_varchar(t1.online_only_cpl, '999,999,999,999.00') as "Total CPL - Online Only",
    to_varchar(((t1.online_only_cpl-t2.online_only_cpl)/iff(t2.online_only_cpl=0 or t2.online_only_cpl is null,1,t2.online_only_cpl)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total CPL - Online Only",
    to_varchar(((t1.online_only_cpl-t3.online_only_cpl)/iff(t3.online_only_cpl=0 or t3.online_only_cpl is null,1,t3.online_only_cpl)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total CPL - Online Only",
    to_varchar(((t1.online_only_cpl-t4.online_only_cpl)/iff(t4.online_only_cpl=0 or t4.online_only_cpl is null,1,t4.online_only_cpl)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total CPL - Online Only",
    '$' || to_varchar(t1.online_only_vip_cac, '999,999,999,999.00') as "Total VIP CAC - Online Only",
    to_varchar(((t1.online_only_vip_cac-t2.online_only_vip_cac)/iff(t2.online_only_vip_cac=0 or t2.online_only_vip_cac is null,1,t2.online_only_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total VIP CAC - Online Only",
    to_varchar(((t1.online_only_vip_cac-t3.online_only_vip_cac)/iff(t3.online_only_vip_cac=0 or t3.online_only_vip_cac is null,1,t3.online_only_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total VIP CAC - Online Only",
    to_varchar(((t1.online_only_vip_cac-t4.online_only_vip_cac)/iff(t4.online_only_vip_cac=0 or t4.online_only_vip_cac is null,1,t4.online_only_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total VIP CAC - Online Only"
from _overall_business_online_only t1
left join _overall_business_online_only t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
                                    when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
                                    when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
                                    when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _overall_business_online_only t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
                                    when t1.date_segment = 'weekly' then t1.date_pop = t3.date
                                    when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _overall_business_online_only t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag > 1
union all
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    to_varchar(t1.online_only_leads, '999,999,999,999') as "Total Leads - Online Only",
    to_varchar(((t1.online_only_leads-t2.online_only_leads)/iff(t2.online_only_leads=0 or t2.online_only_leads is null,1,t2.online_only_leads)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Leads - Online Only",
    to_varchar(((t1.online_only_leads-t3.online_only_leads)/iff(t3.online_only_leads=0 or t3.online_only_leads is null,1,t3.online_only_leads)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Leads - Online Only",
    to_varchar(((t1.online_only_leads-t4.online_only_leads)/iff(t4.online_only_leads=0 or t4.online_only_leads is null,1,t4.online_only_leads)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Leads - Online Only",
    to_varchar(t1.online_only_vips, '999,999,999,999') as "Total VIPs - Online Only",
    to_varchar(((t1.online_only_vips-t2.online_only_vips)/iff(t2.online_only_vips=0 or t2.online_only_vips is null,1,t2.online_only_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total VIPs - Online Only",
    to_varchar(((t1.online_only_vips-t3.online_only_vips)/iff(t3.online_only_vips=0 or t3.online_only_vips is null,1,t3.online_only_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total VIPs - Online Only",
    to_varchar(((t1.online_only_vips-t4.online_only_vips)/iff(t4.online_only_vips=0 or t4.online_only_vips is null,1,t4.online_only_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total VIPs - Online Only",
    '$' || to_varchar(t1.online_only_cpl, '999,999,999,999.00') as "Total CPL - Online Only",
    to_varchar(((t1.online_only_cpl-t2.online_only_cpl)/iff(t2.online_only_cpl=0 or t2.online_only_cpl is null,1,t2.online_only_cpl)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total CPL - Online Only",
    to_varchar(((t1.online_only_cpl-t3.online_only_cpl)/iff(t3.online_only_cpl=0 or t3.online_only_cpl is null,1,t3.online_only_cpl)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total CPL - Online Only",
    to_varchar(((t1.online_only_cpl-t4.online_only_cpl)/iff(t4.online_only_cpl=0 or t4.online_only_cpl is null,1,t4.online_only_cpl)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total CPL - Online Only",
    '$' || to_varchar(t1.online_only_vip_cac, '999,999,999,999.00') as "Total VIP CAC - Online Only",
    to_varchar(((t1.online_only_vip_cac-t2.online_only_vip_cac)/iff(t2.online_only_vip_cac=0 or t2.online_only_vip_cac is null,1,t2.online_only_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total VIP CAC - Online Only",
    to_varchar(((t1.online_only_vip_cac-t3.online_only_vip_cac)/iff(t3.online_only_vip_cac=0 or t3.online_only_vip_cac is null,1,t3.online_only_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total VIP CAC - Online Only",
    to_varchar(((t1.online_only_vip_cac-t4.online_only_vip_cac)/iff(t4.online_only_vip_cac=0 or t4.online_only_vip_cac is null,1,t4.online_only_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total VIP CAC - Online Only"
from _overall_business_online_only_pop t1
left join _overall_business_online_only_pop t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
                                    when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
                                    when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
                                    when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _overall_business_online_only_pop t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
                                    when t1.date_segment = 'weekly' then t1.date_pop = t3.date
                                    when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _overall_business_online_only_pop t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag = 1;

------------------------------------------------------------------------------------
-- total session metrics

create or replace temporary table _total_sessions_raw as
with _session_raw as (
    select 'daily' as date_segment,
           case when isscrubsgateway = true then 'Fabletics Scrubs'
                when ismalegateway = true then 'Fabletics Men'
                else storebrand end as store_brand_name,
           storeregion as region,
           iff(lower(region)='na','US+CA',storeregion) as country,
           sessionlocaldate as date,
           dateadd(year,-1,svm.sessionlocaldate) + delta_ly_comp_this_year as date_pop,
           'all' as channel,
           sum(sessions) as total_sessions,
           sum(leads) as total_leads_from_session,
           sum(activatingorders24hrfromleads) as total_session_vips_from_leads_24h,
           sum(activatingorders) as total_same_session_vips,
           sum(quizstarts) as total_quiz_starts,
           sum(quizcompletes) as total_quiz_completes,
           (total_quiz_completes / iff(total_quiz_starts=0,null,total_quiz_starts)*100) as total_quiz_start_to_complete,
           (total_leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_lead,
           (total_session_vips_from_leads_24h / iff(total_leads_from_session = 0, null, total_leads_from_session) * 100) as total_session_lead_to_vip_24h,
           (total_same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_vip
    from reporting_prod.shared.single_view_media svm
             join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,svm.sessionlocaldate) = th.year
    where channel_type = 'Paid'
      and membershipstate = 'Prospect'
      and channel not in ('direct traffic','physical partnerships','testing','reddit')
      and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
    group by 1,2,3,4,5,6,7
    union all
    select 'weekly' as date_segment,
           case when isscrubsgateway = true then 'Fabletics Scrubs'
                when ismalegateway = true then 'Fabletics Men'
                else storebrand end as store_brand_name,
           storeregion as region,
           iff(lower(region)='na','US+CA',storeregion) as country,
           date_trunc('week',sessionlocaldate) as date,
           date_pop,
           'all' as channel,
           sum(sessions) as total_sessions,
           sum(leads) as total_leads_from_session,
           sum(activatingorders24hrfromleads) as total_session_vips_from_leads_24h,
           sum(activatingorders) as total_same_session_vips,
           sum(quizstarts) as total_quiz_starts,
           sum(quizcompletes) as total_quiz_completes,
           (total_quiz_completes / iff(total_quiz_starts=0,null,total_quiz_starts)*100) as total_quiz_start_to_complete,
           (total_leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_lead,
           (total_session_vips_from_leads_24h / iff(total_leads_from_session = 0, null, total_leads_from_session) * 100) as total_session_lead_to_vip_24h,
           (total_same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_vip
    from reporting_prod.shared.single_view_media svm
             join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
                  on year(dateadd(year, -1, date_trunc('week',svm.sessionlocaldate))) = d.calendar_year and week(date_trunc('week',svm.sessionlocaldate)) = d.week_of_year
    where channel_type = 'Paid'
      and membershipstate = 'Prospect'
      and channel not in ('direct traffic','physical partnerships','testing','reddit')
      and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
    group by 1,2,3,4,5,6,7
    union all
    select 'monthly' as date_segment,
           case when isscrubsgateway = true then 'Fabletics Scrubs'
                when ismalegateway = true then 'Fabletics Men'
                else storebrand end as store_brand_name,
           storeregion as region,
           iff(lower(region)='na','US+CA',storeregion) as country,
           date_trunc('month',sessionlocaldate) as date,
           dateadd(year,-1,date_trunc('month',sessionlocaldate)) as date_pop,
           'all' as channel,
           sum(sessions) as total_sessions,
           sum(leads) as total_leads_from_session,
           sum(activatingorders24hrfromleads) as total_session_vips_from_leads_24h,
           sum(activatingorders) as total_same_session_vips,
           sum(quizstarts) as total_quiz_starts,
           sum(quizcompletes) as total_quiz_completes,
           (total_quiz_completes / iff(total_quiz_starts=0,null,total_quiz_starts)*100) as total_quiz_start_to_complete,
           (total_leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_lead,
           (total_session_vips_from_leads_24h / iff(total_leads_from_session = 0, null, total_leads_from_session) * 100) as total_session_lead_to_vip_24h,
           (total_same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_vip
    from reporting_prod.shared.single_view_media svm
    where channel_type = 'Paid'
      and membershipstate = 'Prospect'
      and channel not in ('direct traffic','physical partnerships','testing','reddit')
      and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
    group by 1,2,3,4,5,6,7
    union all
        select 'daily' as date_segment,
           case when isscrubsgateway = true then 'Fabletics Scrubs'
                when ismalegateway = true then 'Fabletics Men'
                else storebrand end as store_brand_name,
           storeregion as region,
           storecountry as country,
           sessionlocaldate as date,
           dateadd(year,-1,svm.sessionlocaldate) + delta_ly_comp_this_year as date_pop,
           'all' as channel,
           sum(sessions) as total_sessions,
           sum(leads) as total_leads_from_session,
           sum(activatingorders24hrfromleads) as total_session_vips_from_leads_24h,
           sum(activatingorders) as total_same_session_vips,
           sum(quizstarts) as total_quiz_starts,
           sum(quizcompletes) as total_quiz_completes,
           (total_quiz_completes / iff(total_quiz_starts=0,null,total_quiz_starts)*100) as total_quiz_start_to_complete,
           (total_leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_lead,
           (total_session_vips_from_leads_24h / iff(total_leads_from_session = 0, null, total_leads_from_session) * 100) as total_session_lead_to_vip_24h,
           (total_same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_vip
    from reporting_prod.shared.single_view_media svm
             join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,svm.sessionlocaldate) = th.year
    where channel_type = 'Paid'
      and membershipstate = 'Prospect'
      and channel not in ('direct traffic','physical partnerships','testing','reddit')
      and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
    group by 1,2,3,4,5,6,7
    union all
    select 'weekly' as date_segment,
           case when isscrubsgateway = true then 'Fabletics Scrubs'
                when ismalegateway = true then 'Fabletics Men'
                else storebrand end as store_brand_name,
           storeregion as region,
           storecountry as country,
           date_trunc('week',sessionlocaldate) as date,
           date_pop,
           'all' as channel,
           sum(sessions) as total_sessions,
           sum(leads) as total_leads_from_session,
           sum(activatingorders24hrfromleads) as total_session_vips_from_leads_24h,
           sum(activatingorders) as total_same_session_vips,
           sum(quizstarts) as total_quiz_starts,
           sum(quizcompletes) as total_quiz_completes,
           (total_quiz_completes / iff(total_quiz_starts=0,null,total_quiz_starts)*100) as total_quiz_start_to_complete,
           (total_leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_lead,
           (total_session_vips_from_leads_24h / iff(total_leads_from_session = 0, null, total_leads_from_session) * 100) as total_session_lead_to_vip_24h,
           (total_same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_vip
    from reporting_prod.shared.single_view_media svm
             join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
                  on year(dateadd(year, -1, date_trunc('week',svm.sessionlocaldate))) = d.calendar_year and week(date_trunc('week',svm.sessionlocaldate)) = d.week_of_year
    where channel_type = 'Paid'
      and membershipstate = 'Prospect'
      and channel not in ('direct traffic','physical partnerships','testing','reddit')
      and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
    group by 1,2,3,4,5,6,7
    union all
    select 'monthly' as date_segment,
           case when isscrubsgateway = true then 'Fabletics Scrubs'
                when ismalegateway = true then 'Fabletics Men'
                else storebrand end as store_brand_name,
           storeregion as region,
           storecountry as country,
           date_trunc('month',sessionlocaldate) as date,
           dateadd(year,-1,date_trunc('month',sessionlocaldate)) as date_pop,
           'all' as channel,
           sum(sessions) as total_sessions,
           sum(leads) as total_leads_from_session,
           sum(activatingorders24hrfromleads) as total_session_vips_from_leads_24h,
           sum(activatingorders) as total_same_session_vips,
           sum(quizstarts) as total_quiz_starts,
           sum(quizcompletes) as total_quiz_completes,
           (total_quiz_completes / iff(total_quiz_starts=0,null,total_quiz_starts)*100) as total_quiz_start_to_complete,
           (total_leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_lead,
           (total_session_vips_from_leads_24h / iff(total_leads_from_session = 0, null, total_leads_from_session) * 100) as total_session_lead_to_vip_24h,
           (total_same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_vip
    from reporting_prod.shared.single_view_media svm
    where channel_type = 'Paid'
      and membershipstate = 'Prospect'
      and channel not in ('direct traffic','physical partnerships','testing','reddit')
      and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
    group by 1,2,3,4,5,6,7)

select date_segment,
       store_brand_name,
       region,
       country,
       date,
       date_pop,
       channel,
       row_number() over (partition by date_segment, store_brand_name, region, country order by date desc) as latest_date_flag,
       total_sessions,
       total_leads_from_session,
       total_session_vips_from_leads_24h,
       total_same_session_vips,
       total_quiz_starts,
       total_quiz_completes,
       total_quiz_start_to_complete,
       total_session_to_lead,
       total_session_lead_to_vip_24h,
       total_session_to_vip
from _session_raw;

create or replace temporary table _total_sessions_raw_pop as
with _session_raw_pop as (
    select 'daily' as date_segment,
           case when isscrubsgateway = true then 'Fabletics Scrubs'
                when ismalegateway = true then 'Fabletics Men'
                else storebrand end as store_brand_name,
           storeregion as region,
           iff(lower(region)='na','US+CA',storeregion) as country,
           sessionlocaldate as date,
           dateadd(year,-1,svm.sessionlocaldate) + delta_ly_comp_this_year as date_pop,
           'all' as channel,
           sum(sessions) as total_sessions,
           sum(leads) as total_leads_from_session,
           sum(activatingorders24hrfromleads) as total_session_vips_from_leads_24h,
           sum(activatingorders) as total_same_session_vips,
           sum(quizstarts) as total_quiz_starts,
           sum(quizcompletes) as total_quiz_completes,
           (total_quiz_completes / iff(total_quiz_starts=0,null,total_quiz_starts)*100) as total_quiz_start_to_complete,
           (total_leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_lead,
           (total_session_vips_from_leads_24h / iff(total_leads_from_session = 0, null, total_leads_from_session) * 100) as total_session_lead_to_vip_24h,
           (total_same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_vip
    from reporting_prod.shared.single_view_media svm
             join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,svm.sessionlocaldate) = th.year
    where channel_type = 'Paid'
      and membershipstate = 'Prospect'
      and channel not in ('direct traffic','physical partnerships','testing','reddit')
      and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
    group by 1,2,3,4,5,6,7
    union all
    select 'weekly' as date_segment,
           case when isscrubsgateway = true then 'Fabletics Scrubs'
                when ismalegateway = true then 'Fabletics Men'
                else storebrand end as store_brand_name,
           storeregion as region,
           iff(lower(region)='na','US+CA',storeregion) as country,
           date_trunc('week',sessionlocaldate) as date,
           date_pop,
           'all' as channel,
           sum(sessions) as total_sessions,
           sum(leads) as total_leads_from_session,
           sum(activatingorders24hrfromleads) as total_session_vips_from_leads_24h,
           sum(activatingorders) as total_same_session_vips,
           sum(quizstarts) as total_quiz_starts,
           sum(quizcompletes) as total_quiz_completes,
           (total_quiz_completes / iff(total_quiz_starts=0,null,total_quiz_starts)*100) as total_quiz_start_to_complete,
           (total_leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_lead,
           (total_session_vips_from_leads_24h / iff(total_leads_from_session = 0, null, total_leads_from_session) * 100) as total_session_lead_to_vip_24h,
           (total_same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_vip
    from reporting_prod.shared.single_view_media svm
             join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
                  on year(dateadd(year, -1, date_trunc('week',svm.sessionlocaldate))) = d.calendar_year and week(date_trunc('week',svm.sessionlocaldate)) = d.week_of_year
    where date in (select * from _week_pop_dates)
      and channel_type = 'Paid'
      and membershipstate = 'Prospect'
      and channel not in ('direct traffic','physical partnerships','testing','reddit')
      and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
    group by 1,2,3,4,5,6,7
    union all
    select 'monthly' as date_segment,
           case when isscrubsgateway = true then 'Fabletics Scrubs'
                when ismalegateway = true then 'Fabletics Men'
                else storebrand end as store_brand_name,
           storeregion as region,
           iff(lower(region)='na','US+CA',storeregion) as country,
           date_trunc('month',sessionlocaldate) as date,
           dateadd(year,-1,date_trunc('month',sessionlocaldate)) as date_pop,
           'all' as channel,
           sum(sessions) as total_sessions,
           sum(leads) as total_leads_from_session,
           sum(activatingorders24hrfromleads) as total_session_vips_from_leads_24h,
           sum(activatingorders) as total_same_session_vips,
           sum(quizstarts) as total_quiz_starts,
           sum(quizcompletes) as total_quiz_completes,
           (total_quiz_completes / iff(total_quiz_starts=0,null,total_quiz_starts)*100) as total_quiz_start_to_complete,
           (total_leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_lead,
           (total_session_vips_from_leads_24h / iff(total_leads_from_session = 0, null, total_leads_from_session) * 100) as total_session_lead_to_vip_24h,
           (total_same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_vip
    from reporting_prod.shared.single_view_media svm
    where date in (select * from _month_pop_dates)
      and channel_type = 'Paid'
      and membershipstate = 'Prospect'
      and channel not in ('direct traffic','physical partnerships','testing','reddit')
      and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
    group by 1,2,3,4,5,6,7
    union all
        select 'daily' as date_segment,
           case when isscrubsgateway = true then 'Fabletics Scrubs'
                when ismalegateway = true then 'Fabletics Men'
                else storebrand end as store_brand_name,
           storeregion as region,
           storecountry as country,
           sessionlocaldate as date,
           dateadd(year,-1,svm.sessionlocaldate) + delta_ly_comp_this_year as date_pop,
           'all' as channel,
           sum(sessions) as total_sessions,
           sum(leads) as total_leads_from_session,
           sum(activatingorders24hrfromleads) as total_session_vips_from_leads_24h,
           sum(activatingorders) as total_same_session_vips,
           sum(quizstarts) as total_quiz_starts,
           sum(quizcompletes) as total_quiz_completes,
           (total_quiz_completes / iff(total_quiz_starts=0,null,total_quiz_starts)*100) as total_quiz_start_to_complete,
           (total_leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_lead,
           (total_session_vips_from_leads_24h / iff(total_leads_from_session = 0, null, total_leads_from_session) * 100) as total_session_lead_to_vip_24h,
           (total_same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_vip
    from reporting_prod.shared.single_view_media svm
             join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,svm.sessionlocaldate) = th.year
    where channel_type = 'Paid'
      and membershipstate = 'Prospect'
      and channel not in ('direct traffic','physical partnerships','testing','reddit')
      and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
    group by 1,2,3,4,5,6,7
    union all
    select 'weekly' as date_segment,
           case when isscrubsgateway = true then 'Fabletics Scrubs'
                when ismalegateway = true then 'Fabletics Men'
                else storebrand end as store_brand_name,
           storeregion as region,
           storecountry as country,
           date_trunc('week',sessionlocaldate) as date,
           date_pop,
           'all' as channel,
           sum(sessions) as total_sessions,
           sum(leads) as total_leads_from_session,
           sum(activatingorders24hrfromleads) as total_session_vips_from_leads_24h,
           sum(activatingorders) as total_same_session_vips,
           sum(quizstarts) as total_quiz_starts,
           sum(quizcompletes) as total_quiz_completes,
           (total_quiz_completes / iff(total_quiz_starts=0,null,total_quiz_starts)*100) as total_quiz_start_to_complete,
           (total_leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_lead,
           (total_session_vips_from_leads_24h / iff(total_leads_from_session = 0, null, total_leads_from_session) * 100) as total_session_lead_to_vip_24h,
           (total_same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_vip
    from reporting_prod.shared.single_view_media svm
             join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
                  on year(dateadd(year, -1, date_trunc('week',svm.sessionlocaldate))) = d.calendar_year and week(date_trunc('week',svm.sessionlocaldate)) = d.week_of_year
    where date in (select * from _week_pop_dates)
      and channel_type = 'Paid'
      and membershipstate = 'Prospect'
      and channel not in ('direct traffic','physical partnerships','testing','reddit')
      and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
    group by 1,2,3,4,5,6,7
    union all
    select 'monthly' as date_segment,
           case when isscrubsgateway = true then 'Fabletics Scrubs'
                when ismalegateway = true then 'Fabletics Men'
                else storebrand end as store_brand_name,
           storeregion as region,
           storecountry as country,
           date_trunc('month',sessionlocaldate) as date,
           dateadd(year,-1,date_trunc('month',sessionlocaldate)) as date_pop,
           'all' as channel,
           sum(sessions) as total_sessions,
           sum(leads) as total_leads_from_session,
           sum(activatingorders24hrfromleads) as total_session_vips_from_leads_24h,
           sum(activatingorders) as total_same_session_vips,
           sum(quizstarts) as total_quiz_starts,
           sum(quizcompletes) as total_quiz_completes,
           (total_quiz_completes / iff(total_quiz_starts=0,null,total_quiz_starts)*100) as total_quiz_start_to_complete,
           (total_leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_lead,
           (total_session_vips_from_leads_24h / iff(total_leads_from_session = 0, null, total_leads_from_session) * 100) as total_session_lead_to_vip_24h,
           (total_same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as total_session_to_vip
    from reporting_prod.shared.single_view_media svm
    where date in (select * from _month_pop_dates)
      and channel_type = 'Paid'
      and membershipstate = 'Prospect'
      and channel not in ('direct traffic','physical partnerships','testing','reddit')
      and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
    group by 1,2,3,4,5,6,7)

select date_segment,
       store_brand_name,
       region,
       country,
       date,
       date_pop,
       channel,
       row_number() over (partition by date_segment, store_brand_name, region, country order by date desc) as latest_date_flag,
       total_sessions,
       total_leads_from_session,
       total_session_vips_from_leads_24h,
       total_same_session_vips,
       total_quiz_starts,
       total_quiz_completes,
       total_quiz_start_to_complete,
       total_session_to_lead,
       total_session_lead_to_vip_24h,
       total_session_to_vip
from _session_raw_pop;

create or replace temporary table _total_sessions as
select distinct
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    to_varchar(t1.total_sessions, '999,999,999,999') as "Total Sessions",
    to_varchar(((t1.total_sessions-t2.total_sessions)/iff(t2.total_sessions=0 or t2.total_sessions is null,1,t2.total_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Sessions",
    to_varchar(((t1.total_sessions-t3.total_sessions)/iff(t3.total_sessions=0 or t3.total_sessions is null,1,t3.total_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Sessions",
    to_varchar(((t1.total_sessions-t4.total_sessions)/iff(t4.total_sessions=0 or t4.total_sessions is null,1,t4.total_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Sessions",
    to_varchar(t1.total_leads_from_session, '999,999,999,999') as "Total Leads from Session",
    to_varchar(((t1.total_leads_from_session-t2.total_leads_from_session)/iff(t2.total_leads_from_session=0 or t2.total_leads_from_session is null,1,t2.total_leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Leads from Session",
    to_varchar(((t1.total_leads_from_session-t3.total_leads_from_session)/iff(t3.total_leads_from_session=0 or t3.total_leads_from_session is null,1,t3.total_leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Leads from Session",
    to_varchar(((t1.total_leads_from_session-t4.total_leads_from_session)/iff(t4.total_leads_from_session=0 or t4.total_leads_from_session is null,1,t4.total_leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Leads from Session",
    to_varchar(t1.total_session_vips_from_leads_24h, '999,999,999,999') as "Total Session VIPs from Leads 24H",
    to_varchar(((t1.total_session_vips_from_leads_24h-t2.total_session_vips_from_leads_24h)/iff(t2.total_session_vips_from_leads_24h=0 or t2.total_session_vips_from_leads_24h is null,1,t2.total_session_vips_from_leads_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Session VIPs from Leads 24H",
    to_varchar(((t1.total_session_vips_from_leads_24h-t3.total_session_vips_from_leads_24h)/iff(t3.total_session_vips_from_leads_24h=0 or t3.total_session_vips_from_leads_24h is null,1,t3.total_session_vips_from_leads_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Session VIPs from Leads 24H",
    to_varchar(((t1.total_session_vips_from_leads_24h-t4.total_session_vips_from_leads_24h)/iff(t4.total_session_vips_from_leads_24h=0 or t4.total_session_vips_from_leads_24h is null,1,t4.total_session_vips_from_leads_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Session VIPs from Leads 24H",
    to_varchar(t1.total_same_session_vips, '999,999,999,999') as "Total Same Session VIPs",
    to_varchar(((t1.total_same_session_vips-t2.total_same_session_vips)/iff(t2.total_same_session_vips=0 or t2.total_same_session_vips is null,1,t2.total_same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Same Session VIPs",
    to_varchar(((t1.total_same_session_vips-t3.total_same_session_vips)/iff(t3.total_same_session_vips=0 or t3.total_same_session_vips is null,1,t3.total_same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Same Session VIPs",
    to_varchar(((t1.total_same_session_vips-t4.total_same_session_vips)/iff(t4.total_same_session_vips=0 or t4.total_same_session_vips is null,1,t4.total_same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Same Session VIPs",
    to_varchar(t1.total_session_to_lead, '999,999,999,999.00') || '%' as "Total Session to Lead %",
    to_varchar(((t1.total_session_to_lead-t2.total_session_to_lead)/iff(t2.total_session_to_lead=0 or t2.total_session_to_lead is null,1,t2.total_session_to_lead)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Session to Lead %",
    to_varchar(((t1.total_session_to_lead-t3.total_session_to_lead)/iff(t3.total_session_to_lead=0 or t3.total_session_to_lead is null,1,t3.total_session_to_lead)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Session to Lead %",
    to_varchar(((t1.total_session_to_lead-t4.total_session_to_lead)/iff(t4.total_session_to_lead=0 or t4.total_session_to_lead is null,1,t4.total_session_to_lead)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Session to Lead %",
    to_varchar(t1.total_session_lead_to_vip_24h, '999,999,999,999.00') || '%' as "Total Session Lead to VIP 24H %",
    to_varchar(((t1.total_session_lead_to_vip_24h-t2.total_session_lead_to_vip_24h)/iff(t2.total_session_lead_to_vip_24h=0 or t2.total_session_lead_to_vip_24h is null,1,t2.total_session_lead_to_vip_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Session Lead to VIP 24H %",
    to_varchar(((t1.total_session_lead_to_vip_24h-t3.total_session_lead_to_vip_24h)/iff(t3.total_session_lead_to_vip_24h=0 or t3.total_session_lead_to_vip_24h is null,1,t3.total_session_lead_to_vip_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Session Lead to VIP 24H %",
    to_varchar(((t1.total_session_lead_to_vip_24h-t4.total_session_lead_to_vip_24h)/iff(t4.total_session_lead_to_vip_24h=0 or t4.total_session_lead_to_vip_24h is null,1,t4.total_session_lead_to_vip_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Session Lead to VIP 24H %",
    to_varchar(t1.total_session_to_vip, '999,999,999,999.00') || '%' as "Total Session to VIP %",
    to_varchar(((t1.total_session_to_vip-t2.total_session_to_vip)/iff(t2.total_session_to_vip=0 or t2.total_session_to_vip is null,1,t2.total_session_to_vip)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Session to VIP %",
    to_varchar(((t1.total_session_to_vip-t3.total_session_to_vip)/iff(t3.total_session_to_vip=0 or t3.total_session_to_vip is null,1,t3.total_session_to_vip)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Session to VIP %",
    to_varchar(((t1.total_session_to_vip-t4.total_session_to_vip)/iff(t4.total_session_to_vip=0 or t4.total_session_to_vip is null,1,t4.total_session_to_vip)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Session to VIP %",
    to_varchar(t1.total_quiz_starts, '999,999,999,999.00') as "Total Quiz Starts",
    to_varchar(((t1.total_quiz_starts-t2.total_quiz_starts)/iff(t2.total_quiz_starts=0 or t2.total_quiz_starts is null,1,t2.total_quiz_starts)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Quiz Starts",
    to_varchar(((t1.total_quiz_starts-t3.total_quiz_starts)/iff(t3.total_quiz_starts=0 or t3.total_quiz_starts is null,1,t3.total_quiz_starts)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Quiz Starts",
    to_varchar(((t1.total_quiz_starts-t4.total_quiz_starts)/iff(t4.total_quiz_starts=0 or t4.total_quiz_starts is null,1,t4.total_quiz_starts)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Quiz Starts",
    to_varchar(t1.total_quiz_completes, '999,999,999,999.00') as "Total Quiz Completes",
    to_varchar(((t1.total_quiz_completes-t2.total_quiz_completes)/iff(t2.total_quiz_completes=0 or t2.total_quiz_completes is null,1,t2.total_quiz_completes)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Quiz Completes",
    to_varchar(((t1.total_quiz_completes-t3.total_quiz_completes)/iff(t3.total_quiz_completes=0 or t3.total_quiz_completes is null,1,t3.total_quiz_completes)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Quiz Completes",
    to_varchar(((t1.total_quiz_completes-t4.total_quiz_completes)/iff(t4.total_quiz_completes=0 or t4.total_quiz_completes is null,1,t4.total_quiz_completes)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Quiz Completes",
    to_varchar(t1.total_quiz_start_to_complete, '999,999,999,999.00') || '%' as "Total Quiz Complete %",
    to_varchar(((t1.total_quiz_start_to_complete-t2.total_quiz_start_to_complete)/iff(t2.total_quiz_start_to_complete=0 or t2.total_quiz_start_to_complete is null,1,t2.total_quiz_start_to_complete)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Quiz Complete %",
    to_varchar(((t1.total_quiz_start_to_complete-t3.total_quiz_start_to_complete)/iff(t3.total_quiz_start_to_complete=0 or t3.total_quiz_start_to_complete is null,1,t3.total_quiz_start_to_complete)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Quiz Complete %",
    to_varchar(((t1.total_quiz_start_to_complete-t4.total_quiz_start_to_complete)/iff(t4.total_quiz_start_to_complete=0 or t4.total_quiz_start_to_complete is null,1,t4.total_quiz_start_to_complete)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Quiz Complete %"
from _total_sessions_raw t1
left join _total_sessions_raw t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
          when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
          when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
          when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _total_sessions_raw t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
          when t1.date_segment = 'weekly' then t1.date_pop = t3.date
          when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _total_sessions_raw t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag > 1
union all
select distinct
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    to_varchar(t1.total_sessions, '999,999,999,999') as "Total Sessions",
    to_varchar(((t1.total_sessions-t2.total_sessions)/iff(t2.total_sessions=0 or t2.total_sessions is null,1,t2.total_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Sessions",
    to_varchar(((t1.total_sessions-t3.total_sessions)/iff(t3.total_sessions=0 or t3.total_sessions is null,1,t3.total_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Sessions",
    to_varchar(((t1.total_sessions-t4.total_sessions)/iff(t4.total_sessions=0 or t4.total_sessions is null,1,t4.total_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Sessions",
    to_varchar(t1.total_leads_from_session, '999,999,999,999') as "Total Leads from Session",
    to_varchar(((t1.total_leads_from_session-t2.total_leads_from_session)/iff(t2.total_leads_from_session=0 or t2.total_leads_from_session is null,1,t2.total_leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Leads from Session",
    to_varchar(((t1.total_leads_from_session-t3.total_leads_from_session)/iff(t3.total_leads_from_session=0 or t3.total_leads_from_session is null,1,t3.total_leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Leads from Session",
    to_varchar(((t1.total_leads_from_session-t4.total_leads_from_session)/iff(t4.total_leads_from_session=0 or t4.total_leads_from_session is null,1,t4.total_leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Leads from Session",
    to_varchar(t1.total_session_vips_from_leads_24h, '999,999,999,999') as "Total Session VIPs from Leads 24H",
    to_varchar(((t1.total_session_vips_from_leads_24h-t2.total_session_vips_from_leads_24h)/iff(t2.total_session_vips_from_leads_24h=0 or t2.total_session_vips_from_leads_24h is null,1,t2.total_session_vips_from_leads_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Session VIPs from Leads 24H",
    to_varchar(((t1.total_session_vips_from_leads_24h-t3.total_session_vips_from_leads_24h)/iff(t3.total_session_vips_from_leads_24h=0 or t3.total_session_vips_from_leads_24h is null,1,t3.total_session_vips_from_leads_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Session VIPs from Leads 24H",
    to_varchar(((t1.total_session_vips_from_leads_24h-t4.total_session_vips_from_leads_24h)/iff(t4.total_session_vips_from_leads_24h=0 or t4.total_session_vips_from_leads_24h is null,1,t4.total_session_vips_from_leads_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Session VIPs from Leads 24H",
    to_varchar(t1.total_same_session_vips, '999,999,999,999') as "Total Same Session VIPs",
    to_varchar(((t1.total_same_session_vips-t2.total_same_session_vips)/iff(t2.total_same_session_vips=0 or t2.total_same_session_vips is null,1,t2.total_same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Same Session VIPs",
    to_varchar(((t1.total_same_session_vips-t3.total_same_session_vips)/iff(t3.total_same_session_vips=0 or t3.total_same_session_vips is null,1,t3.total_same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Same Session VIPs",
    to_varchar(((t1.total_same_session_vips-t4.total_same_session_vips)/iff(t4.total_same_session_vips=0 or t4.total_same_session_vips is null,1,t4.total_same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Same Session VIPs",
    to_varchar(t1.total_session_to_lead, '999,999,999,999.00') || '%' as "Total Session to Lead %",
    to_varchar(((t1.total_session_to_lead-t2.total_session_to_lead)/iff(t2.total_session_to_lead=0 or t2.total_session_to_lead is null,1,t2.total_session_to_lead)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Session to Lead %",
    to_varchar(((t1.total_session_to_lead-t3.total_session_to_lead)/iff(t3.total_session_to_lead=0 or t3.total_session_to_lead is null,1,t3.total_session_to_lead)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Session to Lead %",
    to_varchar(((t1.total_session_to_lead-t4.total_session_to_lead)/iff(t4.total_session_to_lead=0 or t4.total_session_to_lead is null,1,t4.total_session_to_lead)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Session to Lead %",
    to_varchar(t1.total_session_lead_to_vip_24h, '999,999,999,999.00') || '%' as "Total Session Lead to VIP 24H %",
    to_varchar(((t1.total_session_lead_to_vip_24h-t2.total_session_lead_to_vip_24h)/iff(t2.total_session_lead_to_vip_24h=0 or t2.total_session_lead_to_vip_24h is null,1,t2.total_session_lead_to_vip_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Session Lead to VIP 24H %",
    to_varchar(((t1.total_session_lead_to_vip_24h-t3.total_session_lead_to_vip_24h)/iff(t3.total_session_lead_to_vip_24h=0 or t3.total_session_lead_to_vip_24h is null,1,t3.total_session_lead_to_vip_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Session Lead to VIP 24H %",
    to_varchar(((t1.total_session_lead_to_vip_24h-t4.total_session_lead_to_vip_24h)/iff(t4.total_session_lead_to_vip_24h=0 or t4.total_session_lead_to_vip_24h is null,1,t4.total_session_lead_to_vip_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Session Lead to VIP 24H %",
    to_varchar(t1.total_session_to_vip, '999,999,999,999.00') || '%' as "Total Session to VIP %",
    to_varchar(((t1.total_session_to_vip-t2.total_session_to_vip)/iff(t2.total_session_to_vip=0 or t2.total_session_to_vip is null,1,t2.total_session_to_vip)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Session to VIP %",
    to_varchar(((t1.total_session_to_vip-t3.total_session_to_vip)/iff(t3.total_session_to_vip=0 or t3.total_session_to_vip is null,1,t3.total_session_to_vip)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Session to VIP %",
    to_varchar(((t1.total_session_to_vip-t4.total_session_to_vip)/iff(t4.total_session_to_vip=0 or t4.total_session_to_vip is null,1,t4.total_session_to_vip)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Session to VIP %",
    to_varchar(t1.total_quiz_starts, '999,999,999,999.00') as "Total Quiz Starts",
    to_varchar(((t1.total_quiz_starts-t2.total_quiz_starts)/iff(t2.total_quiz_starts=0 or t2.total_quiz_starts is null,1,t2.total_quiz_starts)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Quiz Starts",
    to_varchar(((t1.total_quiz_starts-t3.total_quiz_starts)/iff(t3.total_quiz_starts=0 or t3.total_quiz_starts is null,1,t3.total_quiz_starts)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Quiz Starts",
    to_varchar(((t1.total_quiz_starts-t4.total_quiz_starts)/iff(t4.total_quiz_starts=0 or t4.total_quiz_starts is null,1,t4.total_quiz_starts)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Quiz Starts",
    to_varchar(t1.total_quiz_completes, '999,999,999,999.00') as "Total Quiz Completes",
    to_varchar(((t1.total_quiz_completes-t2.total_quiz_completes)/iff(t2.total_quiz_completes=0 or t2.total_quiz_completes is null,1,t2.total_quiz_completes)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Quiz Completes",
    to_varchar(((t1.total_quiz_completes-t3.total_quiz_completes)/iff(t3.total_quiz_completes=0 or t3.total_quiz_completes is null,1,t3.total_quiz_completes)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Quiz Completes",
    to_varchar(((t1.total_quiz_completes-t4.total_quiz_completes)/iff(t4.total_quiz_completes=0 or t4.total_quiz_completes is null,1,t4.total_quiz_completes)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Quiz Completes",
    to_varchar(t1.total_quiz_start_to_complete, '999,999,999,999.00') || '%' as "Total Quiz Complete %",
    to_varchar(((t1.total_quiz_start_to_complete-t2.total_quiz_start_to_complete)/iff(t2.total_quiz_start_to_complete=0 or t2.total_quiz_start_to_complete is null,1,t2.total_quiz_start_to_complete)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Total Quiz Complete %",
    to_varchar(((t1.total_quiz_start_to_complete-t3.total_quiz_start_to_complete)/iff(t3.total_quiz_start_to_complete=0 or t3.total_quiz_start_to_complete is null,1,t3.total_quiz_start_to_complete)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Total Quiz Complete %",
    to_varchar(((t1.total_quiz_start_to_complete-t4.total_quiz_start_to_complete)/iff(t4.total_quiz_start_to_complete=0 or t4.total_quiz_start_to_complete is null,1,t4.total_quiz_start_to_complete)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Total Quiz Complete %"
from _total_sessions_raw_pop t1
left join _total_sessions_raw_pop t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
          when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
          when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
          when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _total_sessions_raw_pop t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
          when t1.date_segment = 'weekly' then t1.date_pop = t3.date
          when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _total_sessions_raw_pop t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag = 1;

------------------------------------------------------------------------------------
-- channel % media mix

create or replace temporary table _media_mix_calcs as
with _spend_channel as (
select 'daily' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_region) as country,
    to_date(media_cost_date) as date,
    dateadd(year,-1,to_date(f.media_cost_date)) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,f.media_cost_date) = th.year
where media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_region) as country,
    date_trunc('week',to_date(media_cost_date)) as date,
    date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',f.media_cost_date))) = d.calendar_year and week(date_trunc('week',f.media_cost_date)) = d.week_of_year
where media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_region) as country,
    date_trunc('month',to_date(media_cost_date)) as date,
    dateadd(year,-1,date_trunc('month',to_date(media_cost_date))) as date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
where media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_region) as country,
    date_trunc('year',to_date(media_cost_date)) as date,
    dateadd(year,-1,date_trunc('year',to_date(media_cost_date))) as date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
where media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    ds.store_country as country,
    to_date(media_cost_date) as date,
    dateadd(year,-1,to_date(f.media_cost_date)) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,f.media_cost_date) = th.year
where media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    ds.store_country as country,
    date_trunc('week',to_date(media_cost_date)) as date,
    date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',f.media_cost_date))) = d.calendar_year and week(date_trunc('week',f.media_cost_date)) = d.week_of_year
where media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    ds.store_country as country,
    date_trunc('month',to_date(media_cost_date)) as date,
    dateadd(year,-1,date_trunc('month',to_date(media_cost_date))) as date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
where media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    ds.store_country as country,
    date_trunc('year',to_date(media_cost_date)) as date,
    dateadd(year,-1,date_trunc('year',to_date(media_cost_date))) as date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
where media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7)

select *,
       row_number() over (partition by date_segment, store_brand_name, region, country, channel order by date desc) as latest_date_flag,
       sum(spend) over (partition by date_segment, store_brand_name, region, country, date order by date) as total_spend,
       spend as media_spend,
       (spend / iff(total_spend = 0, null, total_spend)) * 100 as pct_media_mix,
        sum(impressions) over (partition by date_segment, store_brand_name, region, country, date order by date) as total_impressions,
       (impressions / iff(total_impressions = 0, null, total_impressions)) * 100 as pct_impressions,
       spend / iff((impressions/1000) = 0, null, (impressions/1000)) as cpm,
       sum(clicks) over (partition by date_segment, store_brand_name, region, country, date order by date) as total_clicks,
       (clicks / iff(total_clicks = 0, null, total_clicks)) * 100 as pct_clicks,
       spend / iff(clicks = 0, null, clicks) as cpc
from _spend_channel;

create or replace temporary table _media_mix_calcs_shopping_split as
with _spend_channel as (
select 'daily' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    to_date(date) as date,
    dateadd(year,-1,to_date(f.date)) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google f
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,f.date) = th.year
where date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('week',to_date(date)) as date,
    date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google f
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',f.date))) = d.calendar_year and week(date_trunc('week',f.date)) = d.week_of_year
where date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('month',to_date(date)) as date,
    dateadd(year,-1,date_trunc('month',to_date(date))) as date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('year',to_date(date)) as date,
    dateadd(year,-1,date_trunc('year',to_date(date))) as date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    to_date(date) as date,
    dateadd(year,-1,to_date(f.date)) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google f
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,f.date) = th.year
where date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    date_trunc('week',to_date(date)) as date,
    date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google f
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',f.date))) = d.calendar_year and week(date_trunc('week',f.date)) = d.week_of_year
where date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    date_trunc('month',to_date(date)) as date,
    dateadd(year,-1,date_trunc('month',to_date(date))) as date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    date_trunc('year',to_date(date)) as date,
    dateadd(year,-1,date_trunc('year',to_date(date))) as date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7)

select *,
       sum(spend) over (partition by date_segment, store_brand_name, region, country, date order by date) as total_spend,
       spend as media_spend,
       (spend / iff(total_spend = 0, null, total_spend)) * 100 as pct_media_mix
from _spend_channel;

create or replace temporary table _media_mix_final as
select * from _media_mix_calcs
union all
select date_segment,
       store_brand_name,
       region,
       country,
       date,
       date_pop,
       channel,
       spend,
       null as impressions,
       null as clicks,
       row_number() over (partition by date_segment, store_brand_name, region, country, channel order by date desc) as latest_date_flag,
       total_spend,
       media_spend,
       pct_media_mix,
       null as total_impressions,
       null as pct_impressions,
       null as cpm,
       null as total_clicks,
       null as pct_clicks,
       null as cpc
from _media_mix_calcs_shopping_split
where channel in ('branded shopping','non branded shopping');

create or replace temporary table _media_mix_calcs_pop as
with _spend_channel as (
select 'daily' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_region) as country,
    to_date(media_cost_date) as date,
    dateadd(year,-1,to_date(f.media_cost_date)) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,f.media_cost_date) = th.year
where media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_region) as country,
    date_trunc('week',to_date(media_cost_date)) as date,
    date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',f.media_cost_date))) = d.calendar_year and week(date_trunc('week',f.media_cost_date)) = d.week_of_year
where media_cost_date in (select * from _week_pop_dates)
    and media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_region) as country,
    date_trunc('month',to_date(media_cost_date)) as date,
    dateadd(year,-1,date_trunc('month',to_date(media_cost_date))) as date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
where media_cost_date in (select * from _month_pop_dates)
    and media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_region) as country,
    date_trunc('year',to_date(media_cost_date)) as date,
    dateadd(year,-1,date_trunc('year',to_date(media_cost_date))) as date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
where media_cost_date in (select * from _year_pop_dates)
    and media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    ds.store_country as country,
    to_date(media_cost_date) as date,
    dateadd(year,-1,to_date(f.media_cost_date)) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,f.media_cost_date) = th.year
where media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    ds.store_country as country,
    date_trunc('week',to_date(media_cost_date)) as date,
    date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',f.media_cost_date))) = d.calendar_year and week(date_trunc('week',f.media_cost_date)) = d.week_of_year
where media_cost_date in (select * from _week_pop_dates)
    and media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    ds.store_country as country,
    date_trunc('month',to_date(media_cost_date)) as date,
    dateadd(year,-1,date_trunc('month',to_date(media_cost_date))) as date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
where media_cost_date in (select * from _month_pop_dates)
    and media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  is_mens_flag = true then 'Fabletics Men'
        else store_brand end as store_brand_name,
    ds.store_region as region,
    ds.store_country as country,
    date_trunc('year',to_date(media_cost_date)) as date,
    dateadd(year,-1,date_trunc('year',to_date(media_cost_date))) as date_pop,
    channel,
    sum(cost) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
where media_cost_date in (select * from _year_pop_dates)
    and media_cost_date >= $start_date
    and media_cost_date < current_date()
group by 1,2,3,4,5,6,7)

select *,
       row_number() over (partition by date_segment, store_brand_name, region, country, channel order by date desc) as latest_date_flag,
       sum(spend) over (partition by date_segment, store_brand_name, region, country, date order by date) as total_spend,
       spend as media_spend,
       (spend / iff(total_spend = 0, null, total_spend)) * 100 as pct_media_mix,
        sum(impressions) over (partition by date_segment, store_brand_name, region, country, date order by date) as total_impressions,
       (impressions / iff(total_impressions = 0, null, total_impressions)) * 100 as pct_impressions,
       spend / iff((impressions/1000) = 0, null, (impressions/1000)) as cpm,
       sum(clicks) over (partition by date_segment, store_brand_name, region, country, date order by date) as total_clicks,
       (clicks / iff(total_clicks = 0, null, total_clicks)) * 100 as pct_clicks,
       spend / iff(clicks = 0, null, clicks) as cpc
from _spend_channel;

create or replace temporary table _media_mix_calcs_shopping_split_pop as
with _spend_channel_pop as (
select 'daily' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    to_date(date) as date,
    dateadd(year,-1,to_date(f.date)) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google f
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,f.date) = th.year
where date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('week',to_date(date)) as date,
    date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google f
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',f.date))) = d.calendar_year and week(date_trunc('week',f.date)) = d.week_of_year
where date in (select * from _week_pop_dates)
    and date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('month',to_date(date)) as date,
    dateadd(year,-1,date_trunc('month',to_date(date))) as date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where date in (select * from _month_pop_dates)
    and date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('year',to_date(date)) as date,
    dateadd(year,-1,date_trunc('year',to_date(date))) as date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where date in (select * from _year_pop_dates)
    and date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    to_date(date) as date,
    dateadd(year,-1,to_date(f.date)) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google f
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,f.date) = th.year
where date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    date_trunc('week',to_date(date)) as date,
    date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google f
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',f.date))) = d.calendar_year and week(date_trunc('week',f.date)) = d.week_of_year
where date in (select * from _week_pop_dates)
    and date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    date_trunc('month',to_date(date)) as date,
    dateadd(year,-1,date_trunc('month',to_date(date))) as date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where date in (select * from _month_pop_dates)
    and date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    date_trunc('year',to_date(date)) as date,
    dateadd(year,-1,date_trunc('year',to_date(date))) as date_pop,
    channel,
    sum(spend) as spend
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where date in (select * from _year_pop_dates)
    and date >= $cbl_start_date
    and date < current_date()
group by 1,2,3,4,5,6,7)

select *,
       sum(spend) over (partition by date_segment, store_brand_name, region, country, date order by date) as total_spend,
       spend as media_spend,
       (spend / iff(total_spend = 0, null, total_spend)) * 100 as pct_media_mix
from _spend_channel_pop;

create or replace temporary table _media_mix_final_pop as
select * from _media_mix_calcs_pop
union all
select date_segment,
       store_brand_name,
       region,
       country,
       date,
       date_pop,
       channel,
       spend,
       null as impressions,
       null as clicks,
       row_number() over (partition by date_segment, store_brand_name, region, country, channel order by date desc) as latest_date_flag,
       total_spend,
       media_spend,
       pct_media_mix,
       null as total_impressions,
       null as pct_impressions,
       null as cpm,
       null as total_clicks,
       null as pct_clicks,
       null as cpc
from _media_mix_calcs_shopping_split_pop
where channel in ('branded shopping','non branded shopping');


create or replace temporary table _media_eng as
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    to_varchar(t1.impressions, '999,999,999,999') as "Impressions",
    to_varchar(((t1.impressions-t2.impressions)/iff(t2.impressions=0 or t2.impressions is null,1,t2.impressions)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Impressions",
    to_varchar(((t1.impressions-t3.impressions)/iff(t3.impressions=0 or t3.impressions is null,1,t3.impressions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Impressions",
    to_varchar(((t1.impressions-t4.impressions)/iff(t4.impressions=0 or t4.impressions is null,1,t4.impressions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day  Impressions",
    to_varchar(t1.pct_impressions, '999,999,999,999.0') || '%' as "% Impressions",
    '$' || to_varchar(t1.cpm, '999,999,999,999.00') as "CPM",
    to_varchar(((t1.cpm-t2.cpm)/iff(t2.cpm=0 or t2.cpm is null,1,t2.cpm)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP CPM",
    to_varchar(((t1.cpm-t3.cpm)/iff(t3.cpm=0 or t3.cpm is null,1,t3.cpm)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date CPM",
    to_varchar(((t1.cpm-t4.cpm)/iff(t4.cpm=0 or t4.cpm is null,1,t4.cpm)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day  CPM",
    to_varchar(t1.clicks, '999,999,999,999') as "Clicks",
    to_varchar(t1.pct_clicks, '999,999,999,999.0') || '%' as "% Clicks",
    '$' || to_varchar(t1.cpc, '999,999,999,999.00') as "CPC",
    to_varchar(((t1.cpc-t2.cpc)/iff(t2.cpc=0 or t2.cpc is null,1,t2.cpc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP CPC",
    to_varchar(((t1.cpc-t3.cpc)/iff(t3.cpc=0 or t3.cpc is null,1,t3.cpc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date CPC",
    to_varchar(((t1.cpc-t4.cpc)/iff(t4.cpc=0 or t4.cpc is null,1,t4.cpc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day  CPC"
from _media_mix_final t1
         left join _media_mix_final t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
    and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
              when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
              when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
              when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
         left join _media_mix_final t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
    and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
              when t1.date_segment = 'weekly' then t1.date_pop = t3.date
              when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
         left join _media_mix_final t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
    and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag > 1
and t1.channel not in ('branded shopping','non branded shopping')
union all
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    to_varchar(t1.impressions, '999,999,999,999') as "Impressions",
    to_varchar(((t1.impressions-t2.impressions)/iff(t2.impressions=0 or t2.impressions is null,1,t2.impressions)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Impressions",
    to_varchar(((t1.impressions-t3.impressions)/iff(t3.impressions=0 or t3.impressions is null,1,t3.impressions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Impressions",
    to_varchar(((t1.impressions-t4.impressions)/iff(t4.impressions=0 or t4.impressions is null,1,t4.impressions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day  Impressions",
    to_varchar(t1.pct_impressions, '999,999,999,999.0') || '%' as "% Impressions",
    '$' || to_varchar(t1.cpm, '999,999,999,999.00') as "CPM",
    to_varchar(((t1.cpm-t2.cpm)/iff(t2.cpm=0 or t2.cpm is null,1,t2.cpm)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP CPM",
    to_varchar(((t1.cpm-t3.cpm)/iff(t3.cpm=0 or t3.cpm is null,1,t3.cpm)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date CPM",
    to_varchar(((t1.cpm-t4.cpm)/iff(t4.cpm=0 or t4.cpm is null,1,t4.cpm)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day  CPM",
    to_varchar(t1.clicks, '999,999,999,999') as "Clicks",
    to_varchar(t1.pct_clicks, '999,999,999,999.0') || '%' as "% Clicks",
    '$' || to_varchar(t1.cpc, '999,999,999,999.00') as "CPC",
    to_varchar(((t1.cpc-t2.cpc)/iff(t2.cpc=0 or t2.cpc is null,1,t2.cpc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP CPC",
    to_varchar(((t1.cpc-t3.cpc)/iff(t3.cpc=0 or t3.cpc is null,1,t3.cpc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date CPC",
    to_varchar(((t1.cpc-t4.cpc)/iff(t4.cpc=0 or t4.cpc is null,1,t4.cpc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day  CPC"
from _media_mix_final_pop t1
         left join _media_mix_final_pop t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
    and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
              when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
              when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
              when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
         left join _media_mix_final_pop t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
    and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
              when t1.date_segment = 'weekly' then t1.date_pop = t3.date
              when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
         left join _media_mix_final_pop t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
    and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag = 1
and t1.channel not in ('branded shopping','non branded shopping');

create or replace temporary table _media_mix as
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    to_varchar(t1.pct_media_mix, '999,999,999,999.0') || '%' as "% Media Mix",
    to_varchar(((t1.pct_media_mix-t2.pct_media_mix)/iff(t2.pct_media_mix=0 or t2.pct_media_mix is null,1,t2.pct_media_mix)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP % Media Mix",
    to_varchar(((t1.pct_media_mix-t3.pct_media_mix)/iff(t3.pct_media_mix=0 or t3.pct_media_mix is null,1,t3.pct_media_mix)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date % Media Mix",
    to_varchar(((t1.pct_media_mix-t4.pct_media_mix)/iff(t4.pct_media_mix=0 or t4.pct_media_mix is null,1,t4.pct_media_mix)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day  % Media Mix"
from _media_mix_final t1
         left join _media_mix_final t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
    and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
              when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
              when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
              when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
         left join _media_mix_final t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
    and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
              when t1.date_segment = 'weekly' then t1.date_pop = t3.date
              when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
         left join _media_mix_final t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
    and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag > 1
union all
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    to_varchar(t1.pct_media_mix, '999,999,999,999.0') || '%' as "% Media Mix",
    to_varchar(((t1.pct_media_mix-t2.pct_media_mix)/iff(t2.pct_media_mix=0 or t2.pct_media_mix is null,1,t2.pct_media_mix)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP % Media Mix",
    to_varchar(((t1.pct_media_mix-t3.pct_media_mix)/iff(t3.pct_media_mix=0 or t3.pct_media_mix is null,1,t3.pct_media_mix)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date % Media Mix",
    to_varchar(((t1.pct_media_mix-t4.pct_media_mix)/iff(t4.pct_media_mix=0 or t4.pct_media_mix is null,1,t4.pct_media_mix)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day  % Media Mix"
from _media_mix_final_pop t1
         left join _media_mix_final_pop t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
    and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
              when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
              when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
              when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
         left join _media_mix_final_pop t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
    and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
              when t1.date_segment = 'weekly' then t1.date_pop = t3.date
              when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
         left join _media_mix_final_pop t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
    and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag = 1;

------------------------------------------------------------------------------------
-- hdyh cac

create or replace temporary table _hdyh_calcs as
with _hdyh_cac as (
select
    'daily' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  gender = 'M' then 'Fabletics Men'
    else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_country) as country,
    to_date(activation_locaL_datetime) as date,
    dateadd(year,-1,to_date(fa.activation_locaL_datetime)) + delta_ly_comp_this_year as date_pop,
    lower(hdyh.channel) as channel,
    sum(1) as hdyh_vips,
    sum(iff((datediff(day,fr.registration_local_datetime,activation_local_datetime) + 1) = 1,1,0)) as d1_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 2 and 7,1,0)) as d2to7_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 8 and 30, 1,0)) as d8to30_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 31 and 60, 1,0)) as d31to60_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 61 and 90, 1,0)) as d61to90_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) >= 91, 1,0)) as d91plus_vips
from edw_prod.data_model.fact_registration fr
join edw_prod.data_model.fact_activation fa on fr.customer_id = fa.customer_id
join edw_prod.data_model.dim_customer dc on fa.customer_id = dc.customer_id
join reporting_media_base_prod.dbo.vw_med_hdyh_mapping hdyh on hdyh.hdyh_value = lower(replace(fr.how_did_you_hear_parent,' ',''))
join edw_prod.data_model.dim_store ds on fa.store_id = ds.store_id
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,fa.activation_locaL_datetime) = th.year
where is_fake_retail_registration = false
    and is_secondary_registration = false
    and is_retail_registration = false
    and lower(hdyh.channel) != 'ignore'
    and lower(hdyh.channel) in (select distinct channel from reporting_media_prod.attribution.cac_by_lead_channel_daily)
    and to_date(activation_locaL_datetime) >= $start_date and to_date(activation_locaL_datetime) < current_date()
group by 1,2,3,4,5,6,7
union all
select
    'weekly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  gender = 'M' then 'Fabletics Men'
    else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_country) as country,
    date_trunc('week',to_date(activation_locaL_datetime)) as date,
    date_pop,
    lower(hdyh.channel) as channel,
    sum(1) as hdyh_vips,
    sum(iff((datediff(day,fr.registration_local_datetime,activation_local_datetime) + 1) = 1,1,0)) as d1_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 2 and 7,1,0)) as d2to7_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 8 and 30, 1,0)) as d8to30_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 31 and 60, 1,0)) as d31to60_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 61 and 90, 1,0)) as d61to90_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) >= 91, 1,0)) as d91plus_vips
from edw_prod.data_model.fact_registration fr
join edw_prod.data_model.fact_activation fa on fr.customer_id = fa.customer_id
join edw_prod.data_model.dim_customer dc on fa.customer_id = dc.customer_id
join reporting_media_base_prod.dbo.vw_med_hdyh_mapping hdyh on hdyh.hdyh_value = lower(replace(fr.how_did_you_hear_parent,' ',''))
join edw_prod.data_model.dim_store ds on fa.store_id = ds.store_id
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',to_date(fa.activation_locaL_datetime)))) = d.calendar_year and week(date_trunc('week',to_date(fa.activation_locaL_datetime))) = d.week_of_year
where is_fake_retail_registration = false
    and is_secondary_registration = false
    and is_retail_registration = false
    and lower(hdyh.channel) != 'ignore'
    and lower(hdyh.channel) in (select distinct channel from reporting_media_prod.attribution.cac_by_lead_channel_daily)
    and to_date(activation_locaL_datetime) >= $start_date and to_date(activation_locaL_datetime) < current_date()
group by 1,2,3,4,5,6,7
union all
select
    'monthly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  gender = 'M' then 'Fabletics Men'
    else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_country) as country,
    date_trunc('month',to_date(activation_locaL_datetime)) as date,
    dateadd(year,-1,date_trunc('month',to_date(activation_locaL_datetime))) as date_pop,
    lower(hdyh.channel) as channel,
    sum(1) as hdyh_vips,
    sum(iff((datediff(day,fr.registration_local_datetime,activation_local_datetime) + 1) = 1,1,0)) as d1_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 2 and 7,1,0)) as d2to7_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 8 and 30, 1,0)) as d8to30_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 31 and 60, 1,0)) as d31to60_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 61 and 90, 1,0)) as d61to90_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) >= 91, 1,0)) as d91plus_vips
from edw_prod.data_model.fact_registration fr
join edw_prod.data_model.fact_activation fa on fr.customer_id = fa.customer_id
join edw_prod.data_model.dim_customer dc on fa.customer_id = dc.customer_id
join reporting_media_base_prod.dbo.vw_med_hdyh_mapping hdyh on hdyh.hdyh_value = lower(replace(fr.how_did_you_hear_parent,' ',''))
join edw_prod.data_model.dim_store ds on fa.store_id = ds.store_id
where is_fake_retail_registration = false
    and is_secondary_registration = false
    and is_retail_registration = false
    and lower(hdyh.channel) != 'ignore'
    and lower(hdyh.channel) in (select distinct channel from reporting_media_prod.attribution.cac_by_lead_channel_daily)
    and to_date(activation_locaL_datetime) >= $start_date and to_date(activation_locaL_datetime) < current_date()
group by 1,2,3,4,5,6,7
union all
select
    'yearly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  gender = 'M' then 'Fabletics Men'
    else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_country) as country,
    date_trunc('year',to_date(activation_locaL_datetime)) as date,
    dateadd(year,-1,date_trunc('year',to_date(activation_locaL_datetime))) as date_pop,
    lower(hdyh.channel) as channel,
    sum(1) as hdyh_vips,
    sum(iff((datediff(day,fr.registration_local_datetime,activation_local_datetime) + 1) = 1,1,0)) as d1_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 2 and 7,1,0)) as d2to7_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 8 and 30, 1,0)) as d8to30_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 31 and 60, 1,0)) as d31to60_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 61 and 90, 1,0)) as d61to90_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) >= 91, 1,0)) as d91plus_vips
from edw_prod.data_model.fact_registration fr
join edw_prod.data_model.fact_activation fa on fr.customer_id = fa.customer_id
join edw_prod.data_model.dim_customer dc on fa.customer_id = dc.customer_id
join reporting_media_base_prod.dbo.vw_med_hdyh_mapping hdyh on hdyh.hdyh_value = lower(replace(fr.how_did_you_hear_parent,' ',''))
join edw_prod.data_model.dim_store ds on fa.store_id = ds.store_id
where is_fake_retail_registration = false
    and is_secondary_registration = false
    and is_retail_registration = false
    and lower(hdyh.channel) != 'ignore'
    and lower(hdyh.channel) in (select distinct channel from reporting_media_prod.attribution.cac_by_lead_channel_daily)
    and to_date(activation_locaL_datetime) >= $start_date and to_date(activation_locaL_datetime) < current_date()
group by 1,2,3,4,5,6,7)

select
    hdyh.date_segment,
    hdyh.store_brand_name,
    hdyh.region,
    hdyh.country,
    hdyh.date,
    hdyh.date_pop,
    hdyh.channel,
    row_number() over (partition by hdyh.date_segment, hdyh.store_brand_name, hdyh.region, hdyh.country, hdyh.channel order by hdyh.date desc) as latest_date_flag,
    media_spend,
    hdyh_vips,
    media_spend / iff(hdyh_vips=0,null,hdyh_vips) as hdyh_vip_cac,
    d1_vips,
    (d1_vips / hdyh_vips) * 100 as pct_d1_vips,
    media_spend / iff(d1_vips=0,null,d1_vips) as d1_hdyh_vip_cac,
    (d2to7_vips / hdyh_vips) * 100 as pct_d2to7_vips,
    (d8to30_vips / hdyh_vips) * 100 as pct_d8to30_vips,
    (d31to60_vips / hdyh_vips) * 100 as pct_d31to60_vips,
    (d61to90_vips / hdyh_vips) * 100 as pct_d61to90_vips,
    d91plus_vips,
    (d91plus_vips / hdyh_vips) * 100 as pct_d91plus_vips,
    media_spend / iff(d91plus_vips=0,null,d91plus_vips)  as d91_hdyh_vip_cac
from _hdyh_cac hdyh
left join _media_mix_calcs mmm on hdyh.date_segment = mmm.date_segment
and hdyh.store_brand_name = mmm.store_brand_name and hdyh.region = mmm.region and hdyh.date = mmm.date and hdyh.channel = mmm.channel and hdyh.country = mmm.country;

create or replace temporary table _hdyh_calcs_pop as
with _hdyh_cac as (
select
    'daily' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  gender = 'M' then 'Fabletics Men'
    else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_region) as country,
    to_date(activation_locaL_datetime) as date,
    dateadd(year,-1,to_date(fa.activation_locaL_datetime)) + delta_ly_comp_this_year as date_pop,
    lower(hdyh.channel) as channel,
    sum(1) as hdyh_vips,
    sum(iff((datediff(day,fr.registration_local_datetime,activation_local_datetime) + 1) = 1,1,0)) as d1_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 2 and 7,1,0)) as d2to7_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 8 and 30, 1,0)) as d8to30_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 31 and 60, 1,0)) as d31to60_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 61 and 90, 1,0)) as d61to90_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) >= 91, 1,0)) as d91plus_vips
from edw_prod.data_model.fact_registration fr
join edw_prod.data_model.fact_activation fa on fr.customer_id = fa.customer_id
join edw_prod.data_model.dim_customer dc on fa.customer_id = dc.customer_id
join reporting_media_base_prod.dbo.vw_med_hdyh_mapping hdyh on hdyh.hdyh_value = lower(replace(fr.how_did_you_hear_parent,' ',''))
join edw_prod.data_model.dim_store ds on fa.store_id = ds.store_id
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,fa.activation_locaL_datetime) = th.year
where is_fake_retail_registration = false
    and is_secondary_registration = false
    and is_retail_registration = false
    and lower(hdyh.channel) != 'ignore'
    and lower(hdyh.channel) in (select distinct channel from reporting_media_prod.attribution.cac_by_lead_channel_daily)
    and to_date(activation_locaL_datetime) >= $start_date and to_date(activation_locaL_datetime) < current_date()
group by 1,2,3,4,5,6,7
union all
select
    'weekly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  gender = 'M' then 'Fabletics Men'
    else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_region) as country,
    date_trunc('week',to_date(activation_locaL_datetime)) as date,
    date_pop,
    lower(hdyh.channel) as channel,
    sum(1) as hdyh_vips,
    sum(iff((datediff(day,fr.registration_local_datetime,activation_local_datetime) + 1) = 1,1,0)) as d1_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 2 and 7,1,0)) as d2to7_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 8 and 30, 1,0)) as d8to30_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 31 and 60, 1,0)) as d31to60_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 61 and 90, 1,0)) as d61to90_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) >= 91, 1,0)) as d91plus_vips
from edw_prod.data_model.fact_registration fr
join edw_prod.data_model.fact_activation fa on fr.customer_id = fa.customer_id
join edw_prod.data_model.dim_customer dc on fa.customer_id = dc.customer_id
join reporting_media_base_prod.dbo.vw_med_hdyh_mapping hdyh on hdyh.hdyh_value = lower(replace(fr.how_did_you_hear_parent,' ',''))
join edw_prod.data_model.dim_store ds on fa.store_id = ds.store_id
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',to_date(fa.activation_locaL_datetime)))) = d.calendar_year and week(date_trunc('week',to_date(fa.activation_locaL_datetime))) = d.week_of_year
where is_fake_retail_registration = false
    and is_secondary_registration = false
    and is_retail_registration = false
    and lower(hdyh.channel) != 'ignore'
    and lower(hdyh.channel) in (select distinct channel from reporting_media_prod.attribution.cac_by_lead_channel_daily)
    and to_date(activation_locaL_datetime) in (select * from _week_pop_dates)
group by 1,2,3,4,5,6,7
union all
select
    'monthly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  gender = 'M' then 'Fabletics Men'
    else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_region) as country,
    date_trunc('month',to_date(activation_locaL_datetime)) as date,
    dateadd(year,-1,date_trunc('month',to_date(activation_locaL_datetime))) as date_pop,
    lower(hdyh.channel) as channel,
    sum(1) as hdyh_vips,
    sum(iff((datediff(day,fr.registration_local_datetime,activation_local_datetime) + 1) = 1,1,0)) as d1_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 2 and 7,1,0)) as d2to7_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 8 and 30, 1,0)) as d8to30_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 31 and 60, 1,0)) as d31to60_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 61 and 90, 1,0)) as d61to90_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) >= 91, 1,0)) as d91plus_vips
from edw_prod.data_model.fact_registration fr
join edw_prod.data_model.fact_activation fa on fr.customer_id = fa.customer_id
join edw_prod.data_model.dim_customer dc on fa.customer_id = dc.customer_id
join reporting_media_base_prod.dbo.vw_med_hdyh_mapping hdyh on hdyh.hdyh_value = lower(replace(fr.how_did_you_hear_parent,' ',''))
join edw_prod.data_model.dim_store ds on fa.store_id = ds.store_id
where is_fake_retail_registration = false
    and is_secondary_registration = false
    and is_retail_registration = false
    and lower(hdyh.channel) != 'ignore'
    and lower(hdyh.channel) in (select distinct channel from reporting_media_prod.attribution.cac_by_lead_channel_daily)
    and to_date(activation_locaL_datetime) in (select * from _month_pop_dates)
group by 1,2,3,4,5,6,7
union all
select
    'yearly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  gender = 'M' then 'Fabletics Men'
    else store_brand end as store_brand_name,
    ds.store_region as region,
    iff(lower(region)='na','US+CA',ds.store_region) as country,
    date_trunc('year',to_date(activation_locaL_datetime)) as date,
    dateadd(year,-1,date_trunc('year',to_date(activation_locaL_datetime))) as date_pop,
    lower(hdyh.channel) as channel,
    sum(1) as hdyh_vips,
    sum(iff((datediff(day,fr.registration_local_datetime,activation_local_datetime) + 1) = 1,1,0)) as d1_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 2 and 7,1,0)) as d2to7_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 8 and 30, 1,0)) as d8to30_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 31 and 60, 1,0)) as d31to60_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 61 and 90, 1,0)) as d61to90_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) >= 91, 1,0)) as d91plus_vips
from edw_prod.data_model.fact_registration fr
join edw_prod.data_model.fact_activation fa on fr.customer_id = fa.customer_id
join edw_prod.data_model.dim_customer dc on fa.customer_id = dc.customer_id
join reporting_media_base_prod.dbo.vw_med_hdyh_mapping hdyh on hdyh.hdyh_value = lower(replace(fr.how_did_you_hear_parent,' ',''))
join edw_prod.data_model.dim_store ds on fa.store_id = ds.store_id
where is_fake_retail_registration = false
    and is_secondary_registration = false
    and is_retail_registration = false
    and lower(hdyh.channel) != 'ignore'
    and lower(hdyh.channel) in (select distinct channel from reporting_media_prod.attribution.cac_by_lead_channel_daily)
    and to_date(activation_locaL_datetime) in (select * from _year_pop_dates)
group by 1,2,3,4,5,6,7
union all
select
    'daily' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  gender = 'M' then 'Fabletics Men'
    else store_brand end as store_brand_name,
    ds.store_region as region,
    ds.store_country as country,
    to_date(activation_locaL_datetime) as date,
    dateadd(year,-1,to_date(fa.activation_locaL_datetime)) + delta_ly_comp_this_year as date_pop,
    lower(hdyh.channel) as channel,
    sum(1) as hdyh_vips,
    sum(iff((datediff(day,fr.registration_local_datetime,activation_local_datetime) + 1) = 1,1,0)) as d1_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 2 and 7,1,0)) as d2to7_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 8 and 30, 1,0)) as d8to30_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 31 and 60, 1,0)) as d31to60_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 61 and 90, 1,0)) as d61to90_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) >= 91, 1,0)) as d91plus_vips
from edw_prod.data_model.fact_registration fr
join edw_prod.data_model.fact_activation fa on fr.customer_id = fa.customer_id
join edw_prod.data_model.dim_customer dc on fa.customer_id = dc.customer_id
join reporting_media_base_prod.dbo.vw_med_hdyh_mapping hdyh on hdyh.hdyh_value = lower(replace(fr.how_did_you_hear_parent,' ',''))
join edw_prod.data_model.dim_store ds on fa.store_id = ds.store_id
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,fa.activation_locaL_datetime) = th.year
where is_fake_retail_registration = false
    and is_secondary_registration = false
    and is_retail_registration = false
    and lower(hdyh.channel) != 'ignore'
    and lower(hdyh.channel) in (select distinct channel from reporting_media_prod.attribution.cac_by_lead_channel_daily)
    and to_date(activation_locaL_datetime) >= $start_date and to_date(activation_locaL_datetime) < current_date()
group by 1,2,3,4,5,6,7
union all
select
    'weekly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  gender = 'M' then 'Fabletics Men'
    else store_brand end as store_brand_name,
    ds.store_region as region,
    ds.store_country as country,
    date_trunc('week',to_date(activation_locaL_datetime)) as date,
    date_pop,
    lower(hdyh.channel) as channel,
    sum(1) as hdyh_vips,
    sum(iff((datediff(day,fr.registration_local_datetime,activation_local_datetime) + 1) = 1,1,0)) as d1_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 2 and 7,1,0)) as d2to7_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 8 and 30, 1,0)) as d8to30_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 31 and 60, 1,0)) as d31to60_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 61 and 90, 1,0)) as d61to90_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) >= 91, 1,0)) as d91plus_vips
from edw_prod.data_model.fact_registration fr
join edw_prod.data_model.fact_activation fa on fr.customer_id = fa.customer_id
join edw_prod.data_model.dim_customer dc on fa.customer_id = dc.customer_id
join reporting_media_base_prod.dbo.vw_med_hdyh_mapping hdyh on hdyh.hdyh_value = lower(replace(fr.how_did_you_hear_parent,' ',''))
join edw_prod.data_model.dim_store ds on fa.store_id = ds.store_id
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',to_date(fa.activation_locaL_datetime)))) = d.calendar_year and week(date_trunc('week',to_date(fa.activation_locaL_datetime))) = d.week_of_year
where is_fake_retail_registration = false
    and is_secondary_registration = false
    and is_retail_registration = false
    and lower(hdyh.channel) != 'ignore'
    and lower(hdyh.channel) in (select distinct channel from reporting_media_prod.attribution.cac_by_lead_channel_daily)
    and to_date(activation_locaL_datetime) >= $start_date and to_date(activation_locaL_datetime) < current_date()
group by 1,2,3,4,5,6,7
union all
select
    'monthly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  gender = 'M' then 'Fabletics Men'
    else store_brand end as store_brand_name,
    ds.store_region as region,
    ds.store_country as country,
    date_trunc('month',to_date(activation_locaL_datetime)) as date,
    dateadd(year,-1,date_trunc('month',to_date(activation_locaL_datetime))) as date_pop,
    lower(hdyh.channel) as channel,
    sum(1) as hdyh_vips,
    sum(iff((datediff(day,fr.registration_local_datetime,activation_local_datetime) + 1) = 1,1,0)) as d1_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 2 and 7,1,0)) as d2to7_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 8 and 30, 1,0)) as d8to30_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 31 and 60, 1,0)) as d31to60_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 61 and 90, 1,0)) as d61to90_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) >= 91, 1,0)) as d91plus_vips
from edw_prod.data_model.fact_registration fr
join edw_prod.data_model.fact_activation fa on fr.customer_id = fa.customer_id
join edw_prod.data_model.dim_customer dc on fa.customer_id = dc.customer_id
join reporting_media_base_prod.dbo.vw_med_hdyh_mapping hdyh on hdyh.hdyh_value = lower(replace(fr.how_did_you_hear_parent,' ',''))
join edw_prod.data_model.dim_store ds on fa.store_id = ds.store_id
where is_fake_retail_registration = false
    and is_secondary_registration = false
    and is_retail_registration = false
    and lower(hdyh.channel) != 'ignore'
    and lower(hdyh.channel) in (select distinct channel from reporting_media_prod.attribution.cac_by_lead_channel_daily)
    and to_date(activation_locaL_datetime) >= $start_date and to_date(activation_locaL_datetime) < current_date()
group by 1,2,3,4,5,6,7
union all
select
    'yearly' as date_segment,
    case when lower(store_brand) = 'fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs'
        when lower(store_brand) = 'fabletics' and  gender = 'M' then 'Fabletics Men'
    else store_brand end as store_brand_name,
    ds.store_region as region,
    ds.store_country as country,
    date_trunc('year',to_date(activation_locaL_datetime)) as date,
    dateadd(year,-1,date_trunc('year',to_date(activation_locaL_datetime))) as date_pop,
    lower(hdyh.channel) as channel,
    sum(1) as hdyh_vips,
    sum(iff((datediff(day,fr.registration_local_datetime,activation_local_datetime) + 1) = 1,1,0)) as d1_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 2 and 7,1,0)) as d2to7_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 8 and 30, 1,0)) as d8to30_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 31 and 60, 1,0)) as d31to60_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) between 61 and 90, 1,0)) as d61to90_vips,
    sum(iff(datediff(day,fr.registration_local_datetime,activation_local_datetime) >= 91, 1,0)) as d91plus_vips
from edw_prod.data_model.fact_registration fr
join edw_prod.data_model.fact_activation fa on fr.customer_id = fa.customer_id
join edw_prod.data_model.dim_customer dc on fa.customer_id = dc.customer_id
join reporting_media_base_prod.dbo.vw_med_hdyh_mapping hdyh on hdyh.hdyh_value = lower(replace(fr.how_did_you_hear_parent,' ',''))
join edw_prod.data_model.dim_store ds on fa.store_id = ds.store_id
where is_fake_retail_registration = false
    and is_secondary_registration = false
    and is_retail_registration = false
    and lower(hdyh.channel) != 'ignore'
    and lower(hdyh.channel) in (select distinct channel from reporting_media_prod.attribution.cac_by_lead_channel_daily)
    and to_date(activation_locaL_datetime) >= $start_date and to_date(activation_locaL_datetime) < current_date()
group by 1,2,3,4,5,6,7)

select
    hdyh.date_segment,
    hdyh.store_brand_name,
    hdyh.region,
    hdyh.country,
    hdyh.date,
    hdyh.date_pop,
    hdyh.channel as channel,
    row_number() over (partition by hdyh.date_segment, hdyh.store_brand_name, hdyh.region, hdyh.country, hdyh.channel order by hdyh.date desc) as latest_date_flag,
    media_spend,
    hdyh_vips,
    media_spend / iff(hdyh_vips=0,null,hdyh_vips) as hdyh_vip_cac,
    d1_vips,
    (d1_vips / hdyh_vips) * 100 as pct_d1_vips,
    media_spend / iff(d1_vips=0,null,d1_vips) as d1_hdyh_vip_cac,
    (d2to7_vips / hdyh_vips) * 100 as pct_d2to7_vips,
    (d8to30_vips / hdyh_vips) * 100 as pct_d8to30_vips,
    (d31to60_vips / hdyh_vips) * 100 as pct_d31to60_vips,
    (d61to90_vips / hdyh_vips) * 100 as pct_d61to90_vips,
    d91plus_vips,
    (d91plus_vips / hdyh_vips) * 100 as pct_d91plus_vips,
    media_spend / iff(d91plus_vips=0,null,d91plus_vips)  as d91_hdyh_vip_cac
from _hdyh_cac hdyh
left join _media_mix_calcs mmm on hdyh.date_segment = mmm.date_segment
and hdyh.store_brand_name = mmm.store_brand_name and hdyh.region = mmm.region and hdyh.date = mmm.date and hdyh.channel = mmm.channel and hdyh.country = mmm.country;

create or replace temporary table _hdyh as
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    to_varchar(t1.hdyh_vips, '999,999,999,999') as "HDYH VIPs on Date",
    to_varchar(((t1.hdyh_vips-t2.hdyh_vips)/iff(t2.hdyh_vips=0 or t2.hdyh_vips is null,1,t2.hdyh_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP HDYH VIPs on Date",
    to_varchar(((t1.hdyh_vips-t3.hdyh_vips)/iff(t3.hdyh_vips=0 or t3.hdyh_vips is null,1,t3.hdyh_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date HDYH VIPs on Date",
    to_varchar(((t1.hdyh_vips-t4.hdyh_vips)/iff(t4.hdyh_vips=0 or t4.hdyh_vips is null,1,t4.hdyh_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day HDYH VIPs on Date",
    '$' || to_varchar(t1.hdyh_vip_cac, '999,999,999,999.00') as "HDYH VIP CAC",
    to_varchar(((t1.hdyh_vip_cac-t2.hdyh_vip_cac)/iff(t2.hdyh_vip_cac=0 or t2.hdyh_vip_cac is null,1,t2.hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP HDYH VIP CAC",
    to_varchar(((t1.hdyh_vip_cac-t3.hdyh_vip_cac)/iff(t3.hdyh_vip_cac=0 or t3.hdyh_vip_cac is null,1,t3.hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date HDYH VIP CAC",
    to_varchar(((t1.hdyh_vip_cac-t4.hdyh_vip_cac)/iff(t4.hdyh_vip_cac=0 or t4.hdyh_vip_cac is null,1,t4.hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day HDYH VIP CAC",
    to_varchar(t1.d1_vips, '999,999,999,999') as "HDYH D1 VIPs",
    to_varchar(((t1.d1_vips-t2.d1_vips)/iff(t2.d1_vips=0 or t2.d1_vips is null,1,t2.d1_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP HDYH D1 VIPs",
    to_varchar(((t1.d1_vips-t3.d1_vips)/iff(t3.d1_vips=0 or t3.d1_vips is null,1,t3.d1_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date HDYH D1 VIPs",
    to_varchar(((t1.d1_vips-t4.d1_vips)/iff(t4.d1_vips=0 or t4.d1_vips is null,1,t4.d1_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day HDYH D1 VIPs",
    '$' || to_varchar(t1.d1_hdyh_vip_cac, '999,999,999,999.00') as "HDYH D1 VIP CAC",
    to_varchar(((t1.d1_hdyh_vip_cac-t2.d1_hdyh_vip_cac)/iff(t2.d1_hdyh_vip_cac=0 or t2.d1_hdyh_vip_cac is null,1,t2.d1_hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP HDYH D1 VIP CAC",
    to_varchar(((t1.d1_hdyh_vip_cac-t3.d1_hdyh_vip_cac)/iff(t3.d1_hdyh_vip_cac=0 or t3.d1_hdyh_vip_cac is null,1,t3.d1_hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date HDYH D1 VIP CAC",
    to_varchar(((t1.d1_hdyh_vip_cac-t4.d1_hdyh_vip_cac)/iff(t4.d1_hdyh_vip_cac=0 or t4.d1_hdyh_vip_cac is null,1,t4.d1_hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day HDYH D1 VIP CAC",
    to_varchar(t1.d91plus_vips, '999,999,999,999') as "HDYH D91+ VIPs",
    to_varchar(((t1.d91plus_vips-t2.d91plus_vips)/iff(t2.d91plus_vips=0 or t2.d91plus_vips is null,1,t2.d91plus_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP HDYH D91+ VIPs",
    to_varchar(((t1.d91plus_vips-t3.d91plus_vips)/iff(t3.d91plus_vips=0 or t3.d91plus_vips is null,1,t3.d91plus_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date HDYH D91+ VIPs",
    to_varchar(((t1.d91plus_vips-t4.d91plus_vips)/iff(t4.d91plus_vips=0 or t4.d91plus_vips is null,1,t4.d91plus_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day HDYH D91+ VIPs",
    '$' || to_varchar(t1.d91_hdyh_vip_cac, '999,999,999,999.00') as "HDYH D91+ VIP CAC",
    to_varchar(((t1.d91_hdyh_vip_cac-t2.d91_hdyh_vip_cac)/iff(t2.d91_hdyh_vip_cac=0 or t2.d91_hdyh_vip_cac is null,1,t2.d91_hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP HDYH D91+ VIP CAC",
    to_varchar(((t1.d91_hdyh_vip_cac-t3.d91_hdyh_vip_cac)/iff(t3.d91_hdyh_vip_cac=0 or t3.d91_hdyh_vip_cac is null,1,t3.d91_hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date HDYH D91+ VIP CAC",
    to_varchar(((t1.d91_hdyh_vip_cac-t4.d91_hdyh_vip_cac)/iff(t4.d91_hdyh_vip_cac=0 or t4.d91_hdyh_vip_cac is null,1,t4.d91_hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day HDYH D91+ VIP CAC",
    to_varchar(t1.pct_d1_vips, '999,999,999,999.0') || '%' as "% D1 HYDYH VIPs",
    to_varchar(t1.pct_d2to7_vips, '999,999,999,999.0') || '%' as "% D2-7 HYDYH VIPs",
    to_varchar(t1.pct_d8to30_vips, '999,999,999,999.0') || '%' as "% D8-30 HYDYH VIPs",
    to_varchar(t1.pct_d31to60_vips, '999,999,999,999.0') || '%' as "% D31-60 HYDYH VIPs",
    to_varchar(t1.pct_d61to90_vips, '999,999,999,999.0') || '%' as "% D61-90 HYDYH VIPs",
    to_varchar(t1.pct_d91plus_vips, '999,999,999,999.0') || '%' as "% D91+ HYDYH VIPs"
from _hdyh_calcs t1
left join _hdyh_calcs t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
                                    when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
                                    when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
                                    when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _hdyh_calcs t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
                                    when t1.date_segment = 'weekly' then t1.date_pop = t3.date
                                    when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _hdyh_calcs t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag > 1
union all
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    to_varchar(t1.hdyh_vips, '999,999,999,999') as "HDYH VIPs on Date",
    to_varchar(((t1.hdyh_vips-t2.hdyh_vips)/iff(t2.hdyh_vips=0 or t2.hdyh_vips is null,1,t2.hdyh_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP HDYH VIPs on Date",
    to_varchar(((t1.hdyh_vips-t3.hdyh_vips)/iff(t3.hdyh_vips=0 or t3.hdyh_vips is null,1,t3.hdyh_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date HDYH VIPs on Date",
    to_varchar(((t1.hdyh_vips-t4.hdyh_vips)/iff(t4.hdyh_vips=0 or t4.hdyh_vips is null,1,t4.hdyh_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day HDYH VIPs on Date",
    '$' || to_varchar(t1.hdyh_vip_cac, '999,999,999,999.00') as "HDYH VIP CAC",
    to_varchar(((t1.hdyh_vip_cac-t2.hdyh_vip_cac)/iff(t2.hdyh_vip_cac=0 or t2.hdyh_vip_cac is null,1,t2.hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP HDYH VIP CAC",
    to_varchar(((t1.hdyh_vip_cac-t3.hdyh_vip_cac)/iff(t3.hdyh_vip_cac=0 or t3.hdyh_vip_cac is null,1,t3.hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date HDYH VIP CAC",
    to_varchar(((t1.hdyh_vip_cac-t4.hdyh_vip_cac)/iff(t4.hdyh_vip_cac=0 or t4.hdyh_vip_cac is null,1,t4.hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day HDYH VIP CAC",
    to_varchar(t1.d1_vips, '999,999,999,999') as "HDYH D1 VIPs",
    to_varchar(((t1.d1_vips-t2.d1_vips)/iff(t2.d1_vips=0 or t2.d1_vips is null,1,t2.d1_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP HDYH D1 VIPs",
    to_varchar(((t1.d1_vips-t3.d1_vips)/iff(t3.d1_vips=0 or t3.d1_vips is null,1,t3.d1_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date HDYH D1 VIPs",
    to_varchar(((t1.d1_vips-t4.d1_vips)/iff(t4.d1_vips=0 or t4.d1_vips is null,1,t4.d1_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day HDYH D1 VIPs",
    '$' || to_varchar(t1.d1_hdyh_vip_cac, '999,999,999,999.00') as "HDYH D1 VIP CAC",
    to_varchar(((t1.d1_hdyh_vip_cac-t2.d1_hdyh_vip_cac)/iff(t2.d1_hdyh_vip_cac=0 or t2.d1_hdyh_vip_cac is null,1,t2.d1_hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP HDYH D1 VIP CAC",
    to_varchar(((t1.d1_hdyh_vip_cac-t3.d1_hdyh_vip_cac)/iff(t3.d1_hdyh_vip_cac=0 or t3.d1_hdyh_vip_cac is null,1,t3.d1_hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date HDYH D1 VIP CAC",
    to_varchar(((t1.d1_hdyh_vip_cac-t4.d1_hdyh_vip_cac)/iff(t4.d1_hdyh_vip_cac=0 or t4.d1_hdyh_vip_cac is null,1,t4.d1_hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day HDYH D1 VIP CAC",
    to_varchar(t1.d91plus_vips, '999,999,999,999') as "HDYH D91+ VIPs",
    to_varchar(((t1.d91plus_vips-t2.d91plus_vips)/iff(t2.d91plus_vips=0 or t2.d91plus_vips is null,1,t2.d91plus_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP HDYH D91+ VIPs",
    to_varchar(((t1.d91plus_vips-t3.d91plus_vips)/iff(t3.d91plus_vips=0 or t3.d91plus_vips is null,1,t3.d91plus_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date HDYH D91+ VIPs",
    to_varchar(((t1.d91plus_vips-t4.d91plus_vips)/iff(t4.d91plus_vips=0 or t4.d91plus_vips is null,1,t4.d91plus_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day HDYH D91+ VIPs",
    '$' || to_varchar(t1.d91_hdyh_vip_cac, '999,999,999,999.00') as "HDYH D91+ VIP CAC",
    to_varchar(((t1.d91_hdyh_vip_cac-t2.d91_hdyh_vip_cac)/iff(t2.d91_hdyh_vip_cac=0 or t2.d91_hdyh_vip_cac is null,1,t2.d91_hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP HDYH D91+ VIP CAC",
    to_varchar(((t1.d91_hdyh_vip_cac-t3.d91_hdyh_vip_cac)/iff(t3.d91_hdyh_vip_cac=0 or t3.d91_hdyh_vip_cac is null,1,t3.d91_hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date HDYH D91+ VIP CAC",
    to_varchar(((t1.d91_hdyh_vip_cac-t4.d91_hdyh_vip_cac)/iff(t4.d91_hdyh_vip_cac=0 or t4.d91_hdyh_vip_cac is null,1,t4.d91_hdyh_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day HDYH D91+ VIP CAC",
    to_varchar(t1.pct_d1_vips, '999,999,999,999.0') || '%' as "% D1 HYDYH VIPs",
    to_varchar(t1.pct_d2to7_vips, '999,999,999,999.0') || '%' as "% D2-7 HYDYH VIPs",
    to_varchar(t1.pct_d8to30_vips, '999,999,999,999.0') || '%' as "% D8-30 HYDYH VIPs",
    to_varchar(t1.pct_d31to60_vips, '999,999,999,999.0') || '%' as "% D31-60 HYDYH VIPs",
    to_varchar(t1.pct_d61to90_vips, '999,999,999,999.0') || '%' as "% D61-90 HYDYH VIPs",
    to_varchar(t1.pct_d91plus_vips, '999,999,999,999.0') || '%' as "% D91+ HYDYH VIPs"
from _hdyh_calcs t1
left join _hdyh_calcs t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
                                    when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
                                    when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
                                    when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _hdyh_calcs t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
                                    when t1.date_segment = 'weekly' then t1.date_pop = t3.date
                                    when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _hdyh_calcs t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag =1;

------------------------------------------------------------------------------------
-- cac by lead (cbl, supplemental cbl for flna, supplemental cbl for flna agg shopping)

create or replace temporary table _cbl_calcs as
with _raw_cbl as (
select 'daily' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date,
       dateadd(year,-1,cbl.date) + delta_ly_comp_this_year as date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily cbl
         join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,cbl.date) = th.year
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
group by 1,2,3,4,5,6,7,8
union all
select 'weekly' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date_trunc('week',date) as date,
       date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date_trunc('week',date) >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily cbl
         join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
              on year(dateadd(year, -1, date_trunc('week',cbl.date))) = d.calendar_year and week(date_trunc('week',cbl.date)) = d.week_of_year
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
group by 1,2,3,4,5,6,7,8
union all
select 'monthly' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date_trunc('month',date) as date,
       dateadd(year,-1,date_trunc('month',date)) as date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date_trunc('month',date) >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
group by 1,2,3,4,5,6,7,8
union all
select 'yearly' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date_trunc('year',date) as date,
       dateadd(year,-1,date_trunc('year',date)) as date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date_trunc('year',date) >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
group by 1,2,3,4,5,6,7,8
union all
select 'daily' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       country as country,
       date,
       dateadd(year,-1,cbl.date) + delta_ly_comp_this_year as date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily cbl
         join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,cbl.date) = th.year
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
group by 1,2,3,4,5,6,7,8
union all
select 'weekly' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       country as country,
       date_trunc('week',date) as date,
       date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date_trunc('week',date) >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily cbl
         join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
              on year(dateadd(year, -1, date_trunc('week',cbl.date))) = d.calendar_year and week(date_trunc('week',cbl.date)) = d.week_of_year
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
group by 1,2,3,4,5,6,7,8
union all
select 'monthly' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       country as country,
       date_trunc('month',date) as date,
       dateadd(year,-1,date_trunc('month',date)) as date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date_trunc('month',date) >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
group by 1,2,3,4,5,6,7,8
union all
select 'yearly' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       country as country,
       date_trunc('year',date) as date,
       dateadd(year,-1,date_trunc('year',date)) as date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date_trunc('year',date) >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
group by 1,2,3,4,5,6,7,8
)

, _remove_source as (
select date_segment,
       store_brand_name,
       region,
       country,
       date,
       date_pop,
       channel,
       sum(media_spend) as media_spend,
       sum(cbl_leads) as cbl_leads,
       sum(cbl_d1_vips_from_leads) as cbl_d1_vips_from_leads,
       sum(cbl_total_vips_on_date) as cbl_total_vips_on_date,
       sum(cbl_total_vips_on_date_from_click) as cbl_total_vips_on_date_from_click
from _raw_cbl
group by 1,2,3,4,5,6,7)

select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    media_spend,
    cbl_leads,
    cbl_d1_vips_from_leads,
    cbl_total_vips_on_date,
    media_spend / iff(cbl_leads = 0, null, cbl_leads) as cbl_cpl,
    media_spend / iff(cbl_d1_vips_from_leads = 0, null, cbl_d1_vips_from_leads) as cbl_d1_vips_from_leads_cac,
    media_spend / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_total_vip_cac,
    cbl_total_vips_on_date_from_click / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_pct_vips_from_click,
    1 - cbl_pct_vips_from_click as cbl_pct_vips_from_hdyh
from _remove_source;

create or replace temporary table _cbl_calcs_shopping_split as
with _raw_cbl_shopping_split as (
select 'daily' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date,
    dateadd(year,-1,cbl.date) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,cbl.date) = th.year
where retail_customer = false
    and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('week',date) as date,
    date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',cbl.date))) = d.calendar_year and week(date_trunc('week',cbl.date)) = d.week_of_year
where retail_customer = false
    and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
    and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
    and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country as country,
    date,
    dateadd(year,-1,cbl.date) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,cbl.date) = th.year
where retail_customer = false
    and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country as country,
    date_trunc('week',date) as date,
    date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',cbl.date))) = d.calendar_year and week(date_trunc('week',cbl.date)) = d.week_of_year
where retail_customer = false
    and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
    and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
    and date >= $start_date
group by 1,2,3,4,5,6,7)

select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    media_spend,
    cbl_leads,
    cbl_d1_vips_from_leads,
    cbl_total_vips_on_date,
    media_spend / iff(cbl_leads = 0, null, cbl_leads) as cbl_cpl,
    media_spend / iff(cbl_d1_vips_from_leads = 0, null, cbl_d1_vips_from_leads) as cbl_d1_vips_from_leads_cac,
    media_spend / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_total_vip_cac,
    cbl_total_vips_on_date_from_click / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_pct_vips_from_click,
    cbl_total_vips_on_date_from_hdyh / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_pct_vips_from_hdyh
from _raw_cbl_shopping_split;

create or replace temporary table _cbl_calcs_shopping_split_agg as
with _raw_cbl_shopping_split_agg as (
select 'daily' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date,
    dateadd(year,-1,cbl.date) + delta_ly_comp_this_year as date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,cbl.date) = th.year
where retail_customer = false
    and date >= $start_date
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('week',date) as date,
    date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',cbl.date))) = d.calendar_year and week(date_trunc('week',cbl.date)) = d.week_of_year
where retail_customer = false
    and date >= $start_date
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
    and date >= $start_date
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
    and date >= $start_date
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7
union all select 'daily' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country as country,
    date,
    dateadd(year,-1,cbl.date) + delta_ly_comp_this_year as date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,cbl.date) = th.year
where retail_customer = false
    and date >= $start_date
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country as country,
    date_trunc('week',date) as date,
    date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',cbl.date))) = d.calendar_year and week(date_trunc('week',cbl.date)) = d.week_of_year
where retail_customer = false
    and date >= $start_date
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
    and date >= $start_date
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
    and date >= $start_date
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7)

select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    media_spend,
    cbl_leads,
    cbl_d1_vips_from_leads,
    cbl_total_vips_on_date,
    media_spend / iff(cbl_leads = 0, null, cbl_leads) as cbl_cpl,
    media_spend / iff(cbl_d1_vips_from_leads = 0, null, cbl_d1_vips_from_leads) as cbl_d1_vips_from_leads_cac,
    media_spend / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_total_vip_cac,
    cbl_total_vips_on_date_from_click / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_pct_vips_from_click,
    cbl_total_vips_on_date_from_hdyh / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_pct_vips_from_hdyh
from _raw_cbl_shopping_split_agg;

create or replace temporary table _cbl_all_breakouts as
with _all_sources as (
select * from _cbl_calcs
union
select * from _cbl_calcs_shopping_split
union
select * from _cbl_calcs_shopping_split_agg)
select *, row_number() over (partition by date_segment, store_brand_name, region, country, channel order by date desc) as latest_date_flag
from _all_sources;

create or replace temporary table _cbl_calcs_pop as
with _raw_cbl as (
select 'daily' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date,
       dateadd(year,-1,cbl.date) + delta_ly_comp_this_year as date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily cbl
         join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,cbl.date) = th.year
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
group by 1,2,3,4,5,6,7,8
union all
select 'weekly' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date_trunc('week',date) as date,
       date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date_trunc('week',date) >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily cbl
         join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
              on year(dateadd(year, -1, date_trunc('week',cbl.date))) = d.calendar_year and week(date_trunc('week',cbl.date)) = d.week_of_year
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
    and date in (select date from _week_pop_dates)
group by 1,2,3,4,5,6,7,8
union all
select 'monthly' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date_trunc('month',date) as date,
       dateadd(year,-1,date_trunc('month',date)) as date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date_trunc('month',date) >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
    and date in (select date from _month_pop_dates)
group by 1,2,3,4,5,6,7,8
union all
select 'yearly' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date_trunc('year',date) as date,
       dateadd(year,-1,date_trunc('year',date)) as date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date_trunc('year',date) >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
    and date in (select date from _year_pop_dates)
group by 1,2,3,4,5,6,7,8
union all
select 'daily' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       country,
       date,
       dateadd(year,-1,cbl.date) + delta_ly_comp_this_year as date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily cbl
         join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,cbl.date) = th.year
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
group by 1,2,3,4,5,6,7,8
union all
select 'weekly' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       country,
       date_trunc('week',date) as date,
       date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date_trunc('week',date) >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily cbl
         join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
              on year(dateadd(year, -1, date_trunc('week',cbl.date))) = d.calendar_year and week(date_trunc('week',cbl.date)) = d.week_of_year
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
    and date in (select date from _week_pop_dates)
group by 1,2,3,4,5,6,7,8
union all
select 'monthly' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       country,
       date_trunc('month',date) as date,
       dateadd(year,-1,date_trunc('month',date)) as date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date_trunc('month',date) >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
    and date in (select date from _month_pop_dates)
group by 1,2,3,4,5,6,7,8
union all
select 'yearly' as date_segment,
       case when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
            when is_fl_mens_vip = true then 'Fabletics Men'
            else business_unit end as store_brand_name,
       region,
       country,
       date_trunc('year',date) as date,
       dateadd(year,-1,date_trunc('year',date)) as date_pop,
       channel,
       case when subchannel = 'hdyh' then 'hdyh' else 'click' end as credit_source,
       case when store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date_trunc('year',date) >= $cbl_start_date then 'remove' else 'keep' end as remove_flag,
       sum(spend_usd) as media_spend,
       sum(primary_leads) as cbl_leads,
       sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
       sum(total_vips_on_date) as cbl_total_vips_on_date,
       iff(credit_source = 'click', cbl_total_vips_on_date, 0) as cbl_total_vips_on_date_from_click
from reporting_media_prod.attribution.cac_by_lead_channel_daily
where retail_customer = false
    and channel not in ('direct traffic','physical partnerships','testing','reddit')
    and date >= $start_date
    and remove_flag = 'keep'
    and date in (select date from _year_pop_dates)
group by 1,2,3,4,5,6,7,8
)

, _remove_source as (
select date_segment,
       store_brand_name,
       region,
       country,
       date,
       date_pop,
       channel,
       sum(media_spend) as media_spend,
       sum(cbl_leads) as cbl_leads,
       sum(cbl_d1_vips_from_leads) as cbl_d1_vips_from_leads,
       sum(cbl_total_vips_on_date) as cbl_total_vips_on_date,
       sum(cbl_total_vips_on_date_from_click) as cbl_total_vips_on_date_from_click
from _raw_cbl
group by 1,2,3,4,5,6,7)

select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    media_spend,
    cbl_leads,
    cbl_d1_vips_from_leads,
    cbl_total_vips_on_date,
    media_spend / iff(cbl_leads = 0, null, cbl_leads) as cbl_cpl,
    media_spend / iff(cbl_d1_vips_from_leads = 0, null, cbl_d1_vips_from_leads) as cbl_d1_vips_from_leads_cac,
    media_spend / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_total_vip_cac,
    cbl_total_vips_on_date_from_click / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_pct_vips_from_click,
    1 - cbl_pct_vips_from_click as cbl_pct_vips_from_hdyh
from _remove_source;

create or replace temporary table _cbl_calcs_pop_shopping_split as
with _raw_cbl_pop_shopping_slit as (
select 'daily' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date,
    dateadd(year,-1,cbl.date) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
         join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,cbl.date) = th.year
where retail_customer = false
  and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('week',date) as date,
    date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
         join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
              on year(dateadd(year, -1, date_trunc('week',cbl.date))) = d.calendar_year and week(date_trunc('week',cbl.date)) = d.week_of_year
where retail_customer = false
  and date in (select * from _week_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
  and date in (select * from _month_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA'as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
  and date in (select * from _year_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    date,
    dateadd(year,-1,cbl.date) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
         join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,cbl.date) = th.year
where retail_customer = false
  and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    date_trunc('week',date) as date,
    date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
         join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
              on year(dateadd(year, -1, date_trunc('week',cbl.date))) = d.calendar_year and week(date_trunc('week',cbl.date)) = d.week_of_year
where retail_customer = false
  and date in (select * from _week_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
  and date in (select * from _month_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
  and date in (select * from _year_pop_dates)
group by 1,2,3,4,5,6,7)

select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    media_spend,
    cbl_leads,
    cbl_d1_vips_from_leads,
    cbl_total_vips_on_date,
    media_spend / iff(cbl_leads = 0, null, cbl_leads) as cbl_cpl,
    media_spend / iff(cbl_d1_vips_from_leads = 0, null, cbl_d1_vips_from_leads) as cbl_d1_vips_from_leads_cac,
    media_spend / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_total_vip_cac,
    cbl_total_vips_on_date_from_click / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_pct_vips_from_click,
    cbl_total_vips_on_date_from_hdyh / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_pct_vips_from_hdyh
from _raw_cbl_pop_shopping_slit;

create or replace temporary table _cbl_calcs_pop_shopping_split_agg as
with _raw_cbl_pop_shopping_slit_agg as (
select 'daily' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date,
    dateadd(year,-1,cbl.date) + delta_ly_comp_this_year as date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,cbl.date) = th.year
where retail_customer = false
    and date >= $start_date
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('week',date) as date,
    date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',cbl.date))) = d.calendar_year and week(date_trunc('week',cbl.date)) = d.week_of_year
where retail_customer = false
    and date in (select * from _week_pop_dates)
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
    and date in (select * from _month_pop_dates)
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    'US+CA' as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
    and date in (select * from _year_pop_dates)
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    date,
    dateadd(year,-1,cbl.date) + delta_ly_comp_this_year as date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,cbl.date) = th.year
where retail_customer = false
    and date >= $start_date
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    date_trunc('week',date) as date,
    date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google cbl
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',cbl.date))) = d.calendar_year and week(date_trunc('week',cbl.date)) = d.week_of_year
where retail_customer = false
    and date in (select * from _week_pop_dates)
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
    and date in (select * from _month_pop_dates)
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand as store_brand_name,
    'NA' as region,
    country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    'shopping' as channel,
    sum(spend) as media_spend,
    sum(primary_leads) as cbl_leads,
    sum(vips_from_leads_d1) as cbl_d1_vips_from_leads,
    sum(total_vips_on_date) as cbl_total_vips_on_date,
    sum(total_vips_on_date_click) as cbl_total_vips_on_date_from_click,
    sum(total_vips_on_date_hdyh) as cbl_total_vips_on_date_from_hdyh
from reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google
where retail_customer = false
    and date in (select * from _year_pop_dates)
    and channel in ('branded shopping','non branded shopping')
group by 1,2,3,4,5,6,7)

select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    media_spend,
    cbl_leads,
    cbl_d1_vips_from_leads,
    cbl_total_vips_on_date,
    media_spend / iff(cbl_leads = 0, null, cbl_leads) as cbl_cpl,
    media_spend / iff(cbl_d1_vips_from_leads = 0, null, cbl_d1_vips_from_leads) as cbl_d1_vips_from_leads_cac,
    media_spend / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_total_vip_cac,
    cbl_total_vips_on_date_from_click / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_pct_vips_from_click,
    cbl_total_vips_on_date_from_hdyh / iff(cbl_total_vips_on_date = 0, null, cbl_total_vips_on_date) as cbl_pct_vips_from_hdyh
from _raw_cbl_pop_shopping_slit_agg;

create or replace temporary table _cbl_pop_all_breakouts as
with _all_sources_pop as (
select * from _cbl_calcs_pop
union
select * from _cbl_calcs_pop_shopping_split
union
select * from _cbl_calcs_pop_shopping_split_agg)
select *, row_number() over (partition by date_segment, store_brand_name, region, country, channel order by date desc) as latest_date_flag
from _all_sources_pop;

create or replace temporary table _cbl as
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    '$' || to_varchar(t1.media_spend, '999,999,999,999') as "Media Spend",
    to_varchar(((t1.media_spend-t2.media_spend)/iff(t2.media_spend=0 or t2.media_spend is null,1,t2.media_spend)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Media Spend",
    to_varchar(((t1.media_spend-t3.media_spend)/iff(t3.media_spend=0 or t3.media_spend is null,1,t3.media_spend)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Media Spend",
    to_varchar(((t1.media_spend-t4.media_spend)/iff(t4.media_spend=0 or t4.media_spend is null,1,t4.media_spend)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Media Spend",
    to_varchar(t1.cbl_leads, '999,999,999,999') as "CBL Leads",
    '$' || to_varchar(t1.cbl_cpl, '999,999,999,999.00') as "CBL CPL",
    to_varchar(t1.cbl_d1_vips_from_leads, '999,999,999,999') as "CBL D1 VIPs from Leads",
    '$' || to_varchar(t1.cbl_d1_vips_from_leads_cac, '999,999,999,999.00') as "CBL D1 VIPs from Leads CAC",
    to_varchar(t1.cbl_total_vips_on_date, '999,999,999,999') as "CBL Total VIPs on Date",
    to_varchar(((t1.cbl_total_vips_on_date-t2.cbl_total_vips_on_date)/iff(t2.cbl_total_vips_on_date=0 or t2.cbl_total_vips_on_date is null,1,t2.cbl_total_vips_on_date)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP CBL Total VIPs on Date",
    to_varchar(((t1.cbl_total_vips_on_date-t3.cbl_total_vips_on_date)/iff(t3.cbl_total_vips_on_date=0 or t3.cbl_total_vips_on_date is null,1,t3.cbl_total_vips_on_date)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date CBL Total VIPS on Date",
    to_varchar(((t1.cbl_total_vips_on_date-t4.cbl_total_vips_on_date)/iff(t4.cbl_total_vips_on_date=0 or t4.cbl_total_vips_on_date is null,1,t4.cbl_total_vips_on_date)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day CBL Total VIPS on Date",
    '$' || to_varchar(t1.cbl_total_vip_cac, '999,999,999,999.00') as "CBL Total VIP CAC",
    to_varchar(((t1.cbl_total_vip_cac-t2.cbl_total_vip_cac)/iff(t2.cbl_total_vip_cac=0 or t2.cbl_total_vip_cac is null,1,t2.cbl_total_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP CBL Total VIP CAC",
    to_varchar(((t1.cbl_total_vip_cac-t3.cbl_total_vip_cac)/iff(t3.cbl_total_vip_cac=0 or t3.cbl_total_vip_cac is null,1,t3.cbl_total_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date CBL Total VIP CAC",
    to_varchar(((t1.cbl_total_vip_cac-t4.cbl_total_vip_cac)/iff(t4.cbl_total_vip_cac=0 or t4.cbl_total_vip_cac is null,1,t4.cbl_total_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day CBL Total VIP CAC",
    to_varchar(t1.cbl_pct_vips_from_click * 100, '999,999,999,999.0') || '%' as "CBL % VIPs from Click",
    to_varchar(t1.cbl_pct_vips_from_hdyh * 100, '999,999,999,999.0') || '%' as "CBL % VIPs from HDYH"
from _cbl_all_breakouts t1
         left join _cbl_all_breakouts t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
    and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
              when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
              when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
              when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
         left join _cbl_all_breakouts t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
    and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
              when t1.date_segment = 'weekly' then t1.date_pop = t3.date
              when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
         left join _cbl_all_breakouts t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
    and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag > 1
union all
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    '$' || to_varchar(t1.media_spend, '999,999,999,999') as "Media Spend",
    to_varchar(((t1.media_spend-t2.media_spend)/iff(t2.media_spend=0 or t2.media_spend is null,1,t2.media_spend)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Media Spend",
    to_varchar(((t1.media_spend-t3.media_spend)/iff(t3.media_spend=0 or t3.media_spend is null,1,t3.media_spend)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Media Spend",
    to_varchar(((t1.media_spend-t4.media_spend)/iff(t4.media_spend=0 or t4.media_spend is null,1,t4.media_spend)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Media Spend",
    to_varchar(t1.cbl_leads, '999,999,999,999') as "CBL Leads",
    '$' || to_varchar(t1.cbl_cpl, '999,999,999,999.00') as "CBL CPL",
    to_varchar(t1.cbl_d1_vips_from_leads, '999,999,999,999') as "CBL D1 VIPs on Date",
    '$' || to_varchar(t1.cbl_d1_vips_from_leads_cac, '999,999,999,999.00') as "CBL D1 VIP CAC",
    to_varchar(t1.cbl_total_vips_on_date, '999,999,999,999') as "CBL Total VIPs on Date",
    to_varchar(((t1.cbl_total_vips_on_date-t2.cbl_total_vips_on_date)/iff(t2.cbl_total_vips_on_date=0 or t2.cbl_total_vips_on_date is null,1,t2.cbl_total_vips_on_date)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP CBL Total VIPs on Date",
    to_varchar(((t1.cbl_total_vips_on_date-t3.cbl_total_vips_on_date)/iff(t3.cbl_total_vips_on_date=0 or t3.cbl_total_vips_on_date is null,1,t3.cbl_total_vips_on_date)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date CBL Total VIPS on Date",
    to_varchar(((t1.cbl_total_vips_on_date-t4.cbl_total_vips_on_date)/iff(t4.cbl_total_vips_on_date=0 or t4.cbl_total_vips_on_date is null,1,t4.cbl_total_vips_on_date)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day CBL Total VIPS on Date",
    '$' || to_varchar(t1.cbl_total_vip_cac, '999,999,999,999.00') as "CBL Total VIP CAC",
    to_varchar(((t1.cbl_total_vip_cac-t2.cbl_total_vip_cac)/iff(t2.cbl_total_vip_cac=0 or t2.cbl_total_vip_cac is null,1,t2.cbl_total_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP CBL Total VIP CAC",
    to_varchar(((t1.cbl_total_vip_cac-t3.cbl_total_vip_cac)/iff(t3.cbl_total_vip_cac=0 or t3.cbl_total_vip_cac is null,1,t3.cbl_total_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date CBL Total VIP CAC",
    to_varchar(((t1.cbl_total_vip_cac-t4.cbl_total_vip_cac)/iff(t4.cbl_total_vip_cac=0 or t4.cbl_total_vip_cac is null,1,t4.cbl_total_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day CBL Total VIP CAC",
    to_varchar(t1.cbl_pct_vips_from_click * 100, '999,999,999,999.0') || '%' as "CBL % VIPs from Click",
    to_varchar(t1.cbl_pct_vips_from_hdyh * 100, '999,999,999,999.0') || '%' as "CBL % VIPs from HDYH"
from _cbl_pop_all_breakouts t1
         left join _cbl_pop_all_breakouts t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
    and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
              when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
              when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
              when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
         left join _cbl_pop_all_breakouts t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
    and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
              when t1.date_segment = 'weekly' then t1.date_pop = t3.date
              when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
         left join _cbl_pop_all_breakouts t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
    and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag = 1;

------------------------------------------------------------------------------------
-- mmm

create or replace temporary table _mmm as
with _mmm_raw as (
select 'daily' as date_segment,
    store_brand_name,
    'NA' as region,
    'US' as country,
    date,
    dateadd(year,-1,td.date) + delta_ly_comp_this_year as date_pop,
    case when feature ilike '%fb+ig%' then 'fb+ig'
        when feature ilike '%discovery%' or feature ilike '%display%' then 'programmatic-gdn'
        when feature ilike '%streaming%' or feature ilike '%tv%' then 'tv+streaming'
        when feature ilike '%programmatic%' then 'programmatic'
        when feature ilike '%pmax%' then 'pmax'
        when feature ilike '%youtube%' then 'youtube'
        when feature ilike '%shopping>nonbrand%' then 'non branded shopping'
        when feature ilike '%shopping>brand%' then 'branded shopping'
    else feature end as channel,
    sum(spend) as spend,
    sum(feature_attributed_vips_without_base) as vips
from reporting_media_prod.attribution.mmm_outputs_and_target_setting td
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,td.date) = th.year
where feature_type = 'marketing'
and modeled_metric != 'not modeled'
and store_brand_name in ('Fabletics','Fabletics Men')
and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand_name,
    'NA' as region,
    'US' as country,
    date_trunc('week',date) as date,
    date_pop,
    case when feature ilike '%fb+ig%' then 'fb+ig'
        when feature ilike '%discovery%' or feature ilike '%display%' then 'programmatic-gdn'
        when feature ilike '%streaming%' or feature ilike '%tv%' then 'tv+streaming'
        when feature ilike '%programmatic%' then 'programmatic'
        when feature ilike '%pmax%' then 'pmax'
        when feature ilike '%youtube%' then 'youtube'
        when feature ilike '%shopping>nonbrand%' then 'non branded shopping'
        when feature ilike '%shopping>brand%' then 'branded shopping'
    else feature end as channel,
    sum(spend) as spend,
    sum(feature_attributed_vips_without_base) as vips
from reporting_media_prod.attribution.mmm_outputs_and_target_setting td
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',td.date))) = d.calendar_year and week(date_trunc('week',td.date)) = d.week_of_year
where feature_type = 'marketing'
and modeled_metric != 'not modeled'
and store_brand_name in ('Fabletics','Fabletics Men')
and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand_name,
    'NA' as region,
    'US' as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    case when feature ilike '%fb+ig%' then 'fb+ig'
        when feature ilike '%discovery%' or feature ilike '%display%' then 'programmatic-gdn'
        when feature ilike '%streaming%' or feature ilike '%tv%' then 'tv+streaming'
        when feature ilike '%programmatic%' then 'programmatic'
        when feature ilike '%pmax%' then 'pmax'
        when feature ilike '%youtube%' then 'youtube'
        when feature ilike '%shopping>nonbrand%' then 'non branded shopping'
        when feature ilike '%shopping>brand%' then 'branded shopping'
    else feature end as channel,
    sum(spend) as spend,
    sum(feature_attributed_vips_without_base) as vips
from reporting_media_prod.attribution.mmm_outputs_and_target_setting
where feature_type = 'marketing'
and modeled_metric != 'not modeled'
and store_brand_name in ('Fabletics','Fabletics Men')
and date >= $start_date
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand_name,
    'NA' as region,
    'US' as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    case when feature ilike '%fb+ig%' then 'fb+ig'
        when feature ilike '%discovery%' or feature ilike '%display%' then 'programmatic-gdn'
        when feature ilike '%streaming%' or feature ilike '%tv%' then 'tv+streaming'
        when feature ilike '%programmatic%' then 'programmatic'
        when feature ilike '%pmax%' then 'pmax'
        when feature ilike '%youtube%' then 'youtube'
        when feature ilike '%shopping>nonbrand%' then 'non branded shopping'
        when feature ilike '%shopping>brand%' then 'branded shopping'
    else feature end as channel,
    sum(spend) as spend,
    sum(feature_attributed_vips_without_base) as vips
from reporting_media_prod.attribution.mmm_outputs_and_target_setting
where feature_type = 'marketing'
and modeled_metric != 'not modeled'
and store_brand_name in ('Fabletics','Fabletics Men')
and date >= $start_date
group by 1,2,3,4,5,6,7)

select date_segment,
       store_brand_name,
       region,
       country,
       date,
       channel,
       to_varchar(vips, '999,999,999,999') as "MMM VIPs",
       '$' || to_varchar(spend / iff(vips=0,null,vips), '999,999,999,999.00') as "MMM CAC"
from _mmm_raw;

------------------------------------------------------------------------------------
-- session

create or replace temporary table _sessions_raw as
with _sessions as (
select 'daily' as date_segment,
    case when isscrubsgateway = true then 'Fabletics Scrubs'
        when ismalegateway = true then 'Fabletics Men'
        else storebrand end as store_brand_name,
    storeregion as region,
    iff(lower(region)='na','US+CA',storeregion) as country,
    sessionlocaldate as date,
    dateadd(year,-1,svm.sessionlocaldate) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(sessions) as total_sessions,
    sum(leads) as leads_from_session,
    sum(activatingorders24hrfromleads) as session_vips_from_leads_24h,
    sum(activatingorders) as same_session_vips,
    sum(quizstarts) as quiz_starts,
    sum(quizcompletes) as quiz_completes,
    (quiz_completes / iff(quiz_starts=0,null,quiz_starts)*100) as quiz_start_to_complete,
    (leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_lead,
    (session_vips_from_leads_24h / iff(leads_from_session = 0, null, leads_from_session) * 100) as session_lead_to_vip_24h,
    (same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_vip
from reporting_prod.shared.single_view_media svm
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,svm.sessionlocaldate) = th.year
where channel_type = 'Paid'
    and membershipstate = 'Prospect'
    and channel not in ('physical partnerships','testing','reddit')
    and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when isscrubsgateway = true then 'Fabletics Scrubs'
        when ismalegateway = true then 'Fabletics Men'
        else storebrand end as store_brand_name,
    storeregion as region,
    iff(lower(region)='na','US+CA',storeregion) as country,
    date_trunc('week',sessionlocaldate) as date,
    date_pop,
    channel,
    sum(sessions) as total_sessions,
    sum(leads) as leads_from_session,
    sum(activatingorders24hrfromleads) as session_vips_from_leads_24h,
    sum(activatingorders) as same_session_vips,
    sum(quizstarts) as quiz_starts,
    sum(quizcompletes) as quiz_completes,
    (quiz_completes / iff(quiz_starts=0,null,quiz_starts)*100) as quiz_start_to_complete,
    (leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_lead,
    (session_vips_from_leads_24h / iff(leads_from_session = 0, null, leads_from_session) * 100) as session_lead_to_vip_24h,
    (same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_vip
from reporting_prod.shared.single_view_media svm
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',svm.sessionlocaldate))) = d.calendar_year and week(date_trunc('week',svm.sessionlocaldate)) = d.week_of_year
where channel_type = 'Paid'
    and membershipstate = 'Prospect'
    and channel not in ('physical partnerships','testing','reddit')
    and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when isscrubsgateway = true then 'Fabletics Scrubs'
        when ismalegateway = true then 'Fabletics Men'
        else storebrand end as store_brand_name,
    storeregion as region,
    iff(lower(region)='na','US+CA',storeregion) as country,
    date_trunc('month',sessionlocaldate) as date,
    dateadd(year,-1,date_trunc('month',sessionlocaldate)) as date_pop,
    channel,
    sum(sessions) as total_sessions,
    sum(leads) as leads_from_session,
    sum(activatingorders24hrfromleads) as session_vips_from_leads_24h,
    sum(activatingorders) as same_session_vips,
    sum(quizstarts) as quiz_starts,
    sum(quizcompletes) as quiz_completes,
    (quiz_completes / iff(quiz_starts=0,null,quiz_starts)*100) as quiz_start_to_complete,
    (leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_lead,
    (session_vips_from_leads_24h / iff(leads_from_session = 0, null, leads_from_session) * 100) as session_lead_to_vip_24h,
    (same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_vip
from reporting_prod.shared.single_view_media svm
where channel_type = 'Paid'
    and membershipstate = 'Prospect'
    and channel not in ('physical partnerships','testing','reddit')
    and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    case when isscrubsgateway = true then 'Fabletics Scrubs'
        when ismalegateway = true then 'Fabletics Men'
        else storebrand end as store_brand_name,
    storeregion as region,
    storecountry as country,
    sessionlocaldate as date,
    dateadd(year,-1,svm.sessionlocaldate) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(sessions) as total_sessions,
    sum(leads) as leads_from_session,
    sum(activatingorders24hrfromleads) as session_vips_from_leads_24h,
    sum(activatingorders) as same_session_vips,
    sum(quizstarts) as quiz_starts,
    sum(quizcompletes) as quiz_completes,
    (quiz_completes / iff(quiz_starts=0,null,quiz_starts)*100) as quiz_start_to_complete,
    (leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_lead,
    (session_vips_from_leads_24h / iff(leads_from_session = 0, null, leads_from_session) * 100) as session_lead_to_vip_24h,
    (same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_vip
from reporting_prod.shared.single_view_media svm
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,svm.sessionlocaldate) = th.year
where channel_type = 'Paid'
    and membershipstate = 'Prospect'
    and channel not in ('physical partnerships','testing','reddit')
    and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when isscrubsgateway = true then 'Fabletics Scrubs'
        when ismalegateway = true then 'Fabletics Men'
        else storebrand end as store_brand_name,
    storeregion as region,
    storecountry as country,
    date_trunc('week',sessionlocaldate) as date,
    date_pop,
    channel,
    sum(sessions) as total_sessions,
    sum(leads) as leads_from_session,
    sum(activatingorders24hrfromleads) as session_vips_from_leads_24h,
    sum(activatingorders) as same_session_vips,
    sum(quizstarts) as quiz_starts,
    sum(quizcompletes) as quiz_completes,
    (quiz_completes / iff(quiz_starts=0,null,quiz_starts)*100) as quiz_start_to_complete,
    (leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_lead,
    (session_vips_from_leads_24h / iff(leads_from_session = 0, null, leads_from_session) * 100) as session_lead_to_vip_24h,
    (same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_vip
from reporting_prod.shared.single_view_media svm
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',svm.sessionlocaldate))) = d.calendar_year and week(date_trunc('week',svm.sessionlocaldate)) = d.week_of_year
where channel_type = 'Paid'
    and membershipstate = 'Prospect'
    and channel not in ('physical partnerships','testing','reddit')
    and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when isscrubsgateway = true then 'Fabletics Scrubs'
        when ismalegateway = true then 'Fabletics Men'
        else storebrand end as store_brand_name,
    storeregion as region,
    storecountry as country,
    date_trunc('month',sessionlocaldate) as date,
    dateadd(year,-1,date_trunc('month',sessionlocaldate)) as date_pop,
    channel,
    sum(sessions) as total_sessions,
    sum(leads) as leads_from_session,
    sum(activatingorders24hrfromleads) as session_vips_from_leads_24h,
    sum(activatingorders) as same_session_vips,
    sum(quizstarts) as quiz_starts,
    sum(quizcompletes) as quiz_completes,
    (quiz_completes / iff(quiz_starts=0,null,quiz_starts)*100) as quiz_start_to_complete,
    (leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_lead,
    (session_vips_from_leads_24h / iff(leads_from_session = 0, null, leads_from_session) * 100) as session_lead_to_vip_24h,
    (same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_vip
from reporting_prod.shared.single_view_media svm
where channel_type = 'Paid'
    and membershipstate = 'Prospect'
    and channel not in ('physical partnerships','testing','reddit')
    and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
group by 1,2,3,4,5,6,7
)
select
    *,
    row_number() over (partition by date_segment, store_brand_name, region, country, channel order by date desc) as latest_date_flag,
    sum(total_sessions) over (partition by date_segment, store_brand_name, region, country, date, date_pop) as overall_sessions,
    total_sessions / iff(overall_sessions=0,null,overall_sessions) as pct_sessions,
    sum(leads_from_session) over (partition by date_segment, store_brand_name, region, country, date, date_pop) as overall_leads_from_session,
    leads_from_session / iff(overall_leads_from_session=0,null,overall_leads_from_session) as pct_leads_from_session,
    sum(same_session_vips) over (partition by date_segment, store_brand_name, region, country, date, date_pop) as overall_same_session_vips,
    same_session_vips / iff(overall_same_session_vips=0,null,overall_same_session_vips) as pct_same_session_vips
from _sessions;

create or replace temporary table _sessions_raw_pop as
with _sessions_pop as (
select 'daily' as date_segment,
    case when isscrubsgateway = true then 'Fabletics Scrubs'
        when ismalegateway = true then 'Fabletics Men'
        else storebrand end as store_brand_name,
    storeregion as region,
    iff(lower(region)='na','US+CA',storeregion) as country,
    sessionlocaldate as date,
    dateadd(year,-1,svm.sessionlocaldate) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(sessions) as total_sessions,
    sum(leads) as leads_from_session,
    sum(activatingorders24hrfromleads) as session_vips_from_leads_24h,
    sum(activatingorders) as same_session_vips,
    sum(quizstarts) as quiz_starts,
    sum(quizcompletes) as quiz_completes,
    (quiz_completes / iff(quiz_starts=0,null,quiz_starts)*100) as quiz_start_to_complete,
    (leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_lead,
    (session_vips_from_leads_24h / iff(leads_from_session = 0, null, leads_from_session) * 100) as session_lead_to_vip_24h,
    (same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_vip
from reporting_prod.shared.single_view_media svm
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,svm.sessionlocaldate) = th.year
where channel_type = 'Paid'
    and membershipstate = 'Prospect'
    and channel not in ('physical partnerships','testing','reddit')
    and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when isscrubsgateway = true then 'Fabletics Scrubs'
        when ismalegateway = true then 'Fabletics Men'
        else storebrand end as store_brand_name,
    storeregion as region,
    iff(lower(region)='na','US+CA',storeregion) as country,
    date_trunc('week',sessionlocaldate) as date,
    date_pop,
    channel,
    sum(sessions) as total_sessions,
    sum(leads) as leads_from_session,
    sum(activatingorders24hrfromleads) as session_vips_from_leads_24h,
    sum(activatingorders) as same_session_vips,
    sum(quizstarts) as quiz_starts,
    sum(quizcompletes) as quiz_completes,
    (quiz_completes / iff(quiz_starts=0,null,quiz_starts)*100) as quiz_start_to_complete,
    (leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_lead,
    (session_vips_from_leads_24h / iff(leads_from_session = 0, null, leads_from_session) * 100) as session_lead_to_vip_24h,
    (same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_vip
from reporting_prod.shared.single_view_media svm
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',svm.sessionlocaldate))) = d.calendar_year and week(date_trunc('week',svm.sessionlocaldate)) = d.week_of_year
where sessionlocaldate in (select * from _week_pop_dates)
    and channel_type = 'Paid'
    and membershipstate = 'Prospect'
    and channel not in ('physical partnerships','testing','reddit')
    and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when isscrubsgateway = true then 'Fabletics Scrubs'
        when ismalegateway = true then 'Fabletics Men'
        else storebrand end as store_brand_name,
    storeregion as region,
    iff(lower(region)='na','US+CA',storeregion) as country,
    date_trunc('month',sessionlocaldate) as date,
    dateadd(year,-1,date_trunc('month',sessionlocaldate)) as date_pop,
    channel,
    sum(sessions) as total_sessions,
    sum(leads) as leads_from_session,
    sum(activatingorders24hrfromleads) as session_vips_from_leads_24h,
    sum(activatingorders) as same_session_vips,
    sum(quizstarts) as quiz_starts,
    sum(quizcompletes) as quiz_completes,
    (quiz_completes / iff(quiz_starts=0,null,quiz_starts)*100) as quiz_start_to_complete,
    (leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_lead,
    (session_vips_from_leads_24h / iff(leads_from_session = 0, null, leads_from_session) * 100) as session_lead_to_vip_24h,
    (same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_vip
from reporting_prod.shared.single_view_media svm
where sessionlocaldate in (select * from _month_pop_dates)
    and channel_type = 'Paid'
    and membershipstate = 'Prospect'
    and channel not in ('physical partnerships','testing','reddit')
    and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    case when isscrubsgateway = true then 'Fabletics Scrubs'
        when ismalegateway = true then 'Fabletics Men'
        else storebrand end as store_brand_name,
    storeregion as region,
    storecountry as country,
    sessionlocaldate as date,
    dateadd(year,-1,svm.sessionlocaldate) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(sessions) as total_sessions,
    sum(leads) as leads_from_session,
    sum(activatingorders24hrfromleads) as session_vips_from_leads_24h,
    sum(activatingorders) as same_session_vips,
    sum(quizstarts) as quiz_starts,
    sum(quizcompletes) as quiz_completes,
    (quiz_completes / iff(quiz_starts=0,null,quiz_starts)*100) as quiz_start_to_complete,
    (leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_lead,
    (session_vips_from_leads_24h / iff(leads_from_session = 0, null, leads_from_session) * 100) as session_lead_to_vip_24h,
    (same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_vip
from reporting_prod.shared.single_view_media svm
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,svm.sessionlocaldate) = th.year
where channel_type = 'Paid'
    and membershipstate = 'Prospect'
    and channel not in ('physical partnerships','testing','reddit')
    and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    case when isscrubsgateway = true then 'Fabletics Scrubs'
        when ismalegateway = true then 'Fabletics Men'
        else storebrand end as store_brand_name,
    storeregion as region,
    storecountry as country,
    date_trunc('week',sessionlocaldate) as date,
    date_pop,
    channel,
    sum(sessions) as total_sessions,
    sum(leads) as leads_from_session,
    sum(activatingorders24hrfromleads) as session_vips_from_leads_24h,
    sum(activatingorders) as same_session_vips,
    sum(quizstarts) as quiz_starts,
    sum(quizcompletes) as quiz_completes,
    (quiz_completes / iff(quiz_starts=0,null,quiz_starts)*100) as quiz_start_to_complete,
    (leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_lead,
    (session_vips_from_leads_24h / iff(leads_from_session = 0, null, leads_from_session) * 100) as session_lead_to_vip_24h,
    (same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_vip
from reporting_prod.shared.single_view_media svm
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',svm.sessionlocaldate))) = d.calendar_year and week(date_trunc('week',svm.sessionlocaldate)) = d.week_of_year
where sessionlocaldate in (select * from _week_pop_dates)
    and channel_type = 'Paid'
    and membershipstate = 'Prospect'
    and channel not in ('physical partnerships','testing','reddit')
    and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    case when isscrubsgateway = true then 'Fabletics Scrubs'
        when ismalegateway = true then 'Fabletics Men'
        else storebrand end as store_brand_name,
    storeregion as region,
    storecountry as country,
    date_trunc('month',sessionlocaldate) as date,
    dateadd(year,-1,date_trunc('month',sessionlocaldate)) as date_pop,
    channel,
    sum(sessions) as total_sessions,
    sum(leads) as leads_from_session,
    sum(activatingorders24hrfromleads) as session_vips_from_leads_24h,
    sum(activatingorders) as same_session_vips,
    sum(quizstarts) as quiz_starts,
    sum(quizcompletes) as quiz_completes,
    (quiz_completes / iff(quiz_starts=0,null,quiz_starts)*100) as quiz_start_to_complete,
    (leads_from_session / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_lead,
    (session_vips_from_leads_24h / iff(leads_from_session = 0, null, leads_from_session) * 100) as session_lead_to_vip_24h,
    (same_session_vips / iff(total_sessions = 0, null, total_sessions) * 100) as session_to_vip
from reporting_prod.shared.single_view_media svm
where sessionlocaldate in (select * from _month_pop_dates)
    and channel_type = 'Paid'
    and membershipstate = 'Prospect'
    and channel not in ('physical partnerships','testing','reddit')
    and sessionlocaldate >= $start_date and to_date(sessionlocaldate) < current_date()
group by 1,2,3,4,5,6,7
)
select
    *,
    row_number() over (partition by date_segment, store_brand_name, region, country, channel order by date desc) as latest_date_flag,
    sum(total_sessions) over (partition by date_segment, store_brand_name, region, country, date, date_pop) as overall_sessions,
    total_sessions / iff(overall_sessions=0,null,overall_sessions) as pct_sessions,
    sum(leads_from_session) over (partition by date_segment, store_brand_name, region, country, date, date_pop) as overall_leads_from_session,
    leads_from_session / iff(overall_leads_from_session=0,null,overall_leads_from_session) as pct_leads_from_session,
    sum(same_session_vips) over (partition by date_segment, store_brand_name, region, country, date, date_pop) as overall_same_session_vips,
    same_session_vips / iff(overall_same_session_vips=0,null,overall_same_session_vips) as pct_same_session_vips
from _sessions_pop;

create or replace temporary table _sessions as
select distinct
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    to_varchar(t1.total_sessions, '999,999,999,999') as "Sessions",
    to_varchar(((t1.total_sessions-t2.total_sessions)/iff(t2.total_sessions=0 or t2.total_sessions is null,1,t2.total_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Sessions",
    to_varchar(((t1.total_sessions-t3.total_sessions)/iff(t3.total_sessions=0 or t3.total_sessions is null,1,t3.total_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Sessions",
    to_varchar(((t1.total_sessions-t4.total_sessions)/iff(t4.total_sessions=0 or t4.total_sessions is null,1,t4.total_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Sessions",
    to_varchar(t1.pct_sessions * 100, '999,999,999,999') || '%' as "% Sessions",
    to_varchar(((t1.pct_sessions-t2.pct_sessions)/iff(t2.pct_sessions=0 or t2.pct_sessions is null,1,t2.pct_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP % Sessions",
    to_varchar(((t1.pct_sessions-t3.pct_sessions)/iff(t3.pct_sessions=0 or t3.pct_sessions is null,1,t3.pct_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date % Sessions",
    to_varchar(((t1.pct_sessions-t4.pct_sessions)/iff(t4.pct_sessions=0 or t4.pct_sessions is null,1,t4.pct_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day % Sessions",
    to_varchar(t1.leads_from_session, '999,999,999,999') as "Leads from Session",
    to_varchar(((t1.leads_from_session-t2.leads_from_session)/iff(t2.leads_from_session=0 or t2.leads_from_session is null,1,t2.leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Leads from Session",
    to_varchar(((t1.leads_from_session-t3.leads_from_session)/iff(t3.leads_from_session=0 or t3.leads_from_session is null,1,t3.leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Leads from Session",
    to_varchar(((t1.leads_from_session-t4.leads_from_session)/iff(t4.leads_from_session=0 or t4.leads_from_session is null,1,t4.leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Leads from Session",
    to_varchar(t1.session_vips_from_leads_24h, '999,999,999,999') as "Session VIPs from Leads 24H",
    to_varchar(((t1.session_vips_from_leads_24h-t2.session_vips_from_leads_24h)/iff(t2.session_vips_from_leads_24h=0 or t2.session_vips_from_leads_24h is null,1,t2.session_vips_from_leads_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Session VIPs from Leads 24H",
    to_varchar(((t1.session_vips_from_leads_24h-t3.session_vips_from_leads_24h)/iff(t3.session_vips_from_leads_24h=0 or t3.session_vips_from_leads_24h is null,1,t3.session_vips_from_leads_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Session VIPs from Leads 24H",
    to_varchar(((t1.session_vips_from_leads_24h-t4.session_vips_from_leads_24h)/iff(t4.session_vips_from_leads_24h=0 or t4.session_vips_from_leads_24h is null,1,t4.session_vips_from_leads_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Session VIPs from Leads 24H",
    to_varchar(t1.same_session_vips, '999,999,999,999') as "Same Session VIPs",
    to_varchar(((t1.same_session_vips-t2.same_session_vips)/iff(t2.same_session_vips=0 or t2.same_session_vips is null,1,t2.same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Same Session VIPs",
    to_varchar(((t1.same_session_vips-t3.same_session_vips)/iff(t3.same_session_vips=0 or t3.same_session_vips is null,1,t3.same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Same Session VIPs",
    to_varchar(((t1.same_session_vips-t4.same_session_vips)/iff(t4.same_session_vips=0 or t4.same_session_vips is null,1,t4.same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Same Session VIPs",
    to_varchar(t1.session_to_lead, '999,999,999,999.00') || '%' as "Session to Lead %",
    to_varchar(((t1.session_to_lead-t2.session_to_lead)/iff(t2.session_to_lead=0 or t2.session_to_lead is null,1,t2.session_to_lead)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Session to Lead %",
    to_varchar(((t1.session_to_lead-t3.session_to_lead)/iff(t3.session_to_lead=0 or t3.session_to_lead is null,1,t3.session_to_lead)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Session to Lead %",
    to_varchar(((t1.session_to_lead-t4.session_to_lead)/iff(t4.session_to_lead=0 or t4.session_to_lead is null,1,t4.session_to_lead)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Session to Lead %",
    to_varchar(t1.session_lead_to_vip_24h, '999,999,999,999.00') || '%' as "Session Lead to VIP 24H %",
    to_varchar(((t1.session_lead_to_vip_24h-t2.session_lead_to_vip_24h)/iff(t2.session_lead_to_vip_24h=0 or t2.session_lead_to_vip_24h is null,1,t2.session_lead_to_vip_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Session Lead to VIP 24H %",
    to_varchar(((t1.session_lead_to_vip_24h-t3.session_lead_to_vip_24h)/iff(t3.session_lead_to_vip_24h=0 or t3.session_lead_to_vip_24h is null,1,t3.session_lead_to_vip_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Session Lead to VIP 24H %",
    to_varchar(((t1.session_lead_to_vip_24h-t4.session_lead_to_vip_24h)/iff(t4.session_lead_to_vip_24h=0 or t4.session_lead_to_vip_24h is null,1,t4.session_lead_to_vip_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Session Lead to VIP 24H %",
    to_varchar(t1.session_to_vip, '999,999,999,999.00') || '%' as "Session to VIP %",
    to_varchar(((t1.session_to_vip-t2.session_to_vip)/iff(t2.session_to_vip=0 or t2.session_to_vip is null,1,t2.session_to_vip)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Session to VIP %",
    to_varchar(((t1.session_to_vip-t3.session_to_vip)/iff(t3.session_to_vip=0 or t3.session_to_vip is null,1,t3.session_to_vip)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Session to VIP %",
    to_varchar(((t1.session_to_vip-t4.session_to_vip)/iff(t4.session_to_vip=0 or t4.session_to_vip is null,1,t4.session_to_vip)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Session to VIP %",
    to_varchar(t1.pct_leads_from_session, '999,999,999,999.00') || '%' as "% Leads from Session",
    to_varchar(((t1.pct_leads_from_session-t2.pct_leads_from_session)/iff(t2.pct_leads_from_session=0 or t2.pct_leads_from_session is null,1,t2.pct_leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP % Leads from Session",
    to_varchar(((t1.pct_leads_from_session-t3.pct_leads_from_session)/iff(t3.pct_leads_from_session=0 or t3.pct_leads_from_session is null,1,t3.pct_leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date % Leads from Session",
    to_varchar(((t1.pct_leads_from_session-t4.pct_leads_from_session)/iff(t4.pct_leads_from_session=0 or t4.pct_leads_from_session is null,1,t4.pct_leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day % Leads from Session",
    to_varchar(t1.pct_same_session_vips, '999,999,999,999.00') || '%' as "% Same Session VIPs",
    to_varchar(((t1.pct_same_session_vips-t2.pct_same_session_vips)/iff(t2.pct_same_session_vips=0 or t2.pct_same_session_vips is null,1,t2.pct_same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP % Same Session VIPs",
    to_varchar(((t1.pct_same_session_vips-t3.pct_same_session_vips)/iff(t3.pct_same_session_vips=0 or t3.pct_same_session_vips is null,1,t3.pct_same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date % Same Session VIPs",
    to_varchar(((t1.pct_same_session_vips-t4.pct_same_session_vips)/iff(t4.pct_same_session_vips=0 or t4.pct_same_session_vips is null,1,t4.pct_same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day % Same Session VIPs",
    to_varchar(t1.quiz_starts, '999,999,999,999.00') as "Quiz Starts",
    to_varchar(((t1.quiz_starts-t2.quiz_starts)/iff(t2.quiz_starts=0 or t2.quiz_starts is null,1,t2.quiz_starts)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Quiz Starts",
    to_varchar(((t1.quiz_starts-t3.quiz_starts)/iff(t3.quiz_starts=0 or t3.quiz_starts is null,1,t3.quiz_starts)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Quiz Starts",
    to_varchar(((t1.quiz_starts-t4.quiz_starts)/iff(t4.quiz_starts=0 or t4.quiz_starts is null,1,t4.quiz_starts)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Quiz Starts",
    to_varchar(t1.quiz_completes, '999,999,999,999.00') as "Quiz Completes",
    to_varchar(((t1.quiz_completes-t2.quiz_completes)/iff(t2.quiz_completes=0 or t2.quiz_completes is null,1,t2.quiz_completes)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Quiz Completes",
    to_varchar(((t1.quiz_completes-t3.quiz_completes)/iff(t3.quiz_completes=0 or t3.quiz_completes is null,1,t3.quiz_completes)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Quiz Completes",
    to_varchar(((t1.quiz_completes-t4.quiz_completes)/iff(t4.quiz_completes=0 or t4.quiz_completes is null,1,t4.quiz_completes)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Quiz Completes",
    to_varchar(t1.quiz_start_to_complete, '999,999,999,999.00') || '%' as "Quiz Complete %",
    to_varchar(((t1.quiz_start_to_complete-t2.quiz_start_to_complete)/iff(t2.quiz_start_to_complete=0 or t2.quiz_start_to_complete is null,1,t2.quiz_start_to_complete)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Quiz Complete %",
    to_varchar(((t1.quiz_start_to_complete-t3.quiz_start_to_complete)/iff(t3.quiz_start_to_complete=0 or t3.quiz_start_to_complete is null,1,t3.quiz_start_to_complete)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Quiz Complete %",
    to_varchar(((t1.quiz_start_to_complete-t4.quiz_start_to_complete)/iff(t4.quiz_start_to_complete=0 or t4.quiz_start_to_complete is null,1,t4.quiz_start_to_complete)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Quiz Complete %"
from _sessions_raw t1
left join _sessions_raw t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.region = t2.region and t1.country = t2.country and t1.channel = t2.channel
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
          when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
          when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
          when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _sessions_raw t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.region = t3.region and t1.country = t3.country and t1.channel = t3.channel
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
          when t1.date_segment = 'weekly' then t1.date_pop = t3.date
          when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _sessions_raw t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.region = t4.region and t1.country = t4.country and t1.channel = t4.channel
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag > 1
union all
select distinct
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    to_varchar(t1.total_sessions, '999,999,999,999') as "Sessions",
    to_varchar(((t1.total_sessions-t2.total_sessions)/iff(t2.total_sessions=0 or t2.total_sessions is null,1,t2.total_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Sessions",
    to_varchar(((t1.total_sessions-t3.total_sessions)/iff(t3.total_sessions=0 or t3.total_sessions is null,1,t3.total_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Sessions",
    to_varchar(((t1.total_sessions-t4.total_sessions)/iff(t4.total_sessions=0 or t4.total_sessions is null,1,t4.total_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Sessions",
    to_varchar(t1.pct_sessions * 100, '999,999,999,999') || '%' as "% Sessions",
    to_varchar(((t1.pct_sessions-t2.pct_sessions)/iff(t2.pct_sessions=0 or t2.pct_sessions is null,1,t2.pct_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP % Sessions",
    to_varchar(((t1.pct_sessions-t3.pct_sessions)/iff(t3.pct_sessions=0 or t3.pct_sessions is null,1,t3.pct_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date % Sessions",
    to_varchar(((t1.pct_sessions-t4.pct_sessions)/iff(t4.pct_sessions=0 or t4.pct_sessions is null,1,t4.pct_sessions)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day % Sessions",
    to_varchar(t1.leads_from_session, '999,999,999,999') as "Leads from Session",
    to_varchar(((t1.leads_from_session-t2.leads_from_session)/iff(t2.leads_from_session=0 or t2.leads_from_session is null,1,t2.leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Leads from Session",
    to_varchar(((t1.leads_from_session-t3.leads_from_session)/iff(t3.leads_from_session=0 or t3.leads_from_session is null,1,t3.leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Leads from Session",
    to_varchar(((t1.leads_from_session-t4.leads_from_session)/iff(t4.leads_from_session=0 or t4.leads_from_session is null,1,t4.leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Leads from Session",
    to_varchar(t1.session_vips_from_leads_24h, '999,999,999,999') as "Session VIPs from Leads 24H",
    to_varchar(((t1.session_vips_from_leads_24h-t2.session_vips_from_leads_24h)/iff(t2.session_vips_from_leads_24h=0 or t2.session_vips_from_leads_24h is null,1,t2.session_vips_from_leads_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Session VIPs from Leads 24H",
    to_varchar(((t1.session_vips_from_leads_24h-t3.session_vips_from_leads_24h)/iff(t3.session_vips_from_leads_24h=0 or t3.session_vips_from_leads_24h is null,1,t3.session_vips_from_leads_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Session VIPs from Leads 24H",
    to_varchar(((t1.session_vips_from_leads_24h-t4.session_vips_from_leads_24h)/iff(t4.session_vips_from_leads_24h=0 or t4.session_vips_from_leads_24h is null,1,t4.session_vips_from_leads_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Session VIPs from Leads 24H",
    to_varchar(t1.same_session_vips, '999,999,999,999') as "Same Session VIPs",
    to_varchar(((t1.same_session_vips-t2.same_session_vips)/iff(t2.same_session_vips=0 or t2.same_session_vips is null,1,t2.same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Same Session VIPs",
    to_varchar(((t1.same_session_vips-t3.same_session_vips)/iff(t3.same_session_vips=0 or t3.same_session_vips is null,1,t3.same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Same Session VIPs",
    to_varchar(((t1.same_session_vips-t4.same_session_vips)/iff(t4.same_session_vips=0 or t4.same_session_vips is null,1,t4.same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Same Session VIPs",
    to_varchar(t1.session_to_lead, '999,999,999,999.00') || '%' as "Session to Lead %",
    to_varchar(((t1.session_to_lead-t2.session_to_lead)/iff(t2.session_to_lead=0 or t2.session_to_lead is null,1,t2.session_to_lead)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Session to Lead %",
    to_varchar(((t1.session_to_lead-t3.session_to_lead)/iff(t3.session_to_lead=0 or t3.session_to_lead is null,1,t3.session_to_lead)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Session to Lead %",
    to_varchar(((t1.session_to_lead-t4.session_to_lead)/iff(t4.session_to_lead=0 or t4.session_to_lead is null,1,t4.session_to_lead)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Session to Lead %",
    to_varchar(t1.session_lead_to_vip_24h, '999,999,999,999.00') || '%' as "Session Lead to VIP 24H %",
    to_varchar(((t1.session_lead_to_vip_24h-t2.session_lead_to_vip_24h)/iff(t2.session_lead_to_vip_24h=0 or t2.session_lead_to_vip_24h is null,1,t2.session_lead_to_vip_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Session Lead to VIP 24H %",
    to_varchar(((t1.session_lead_to_vip_24h-t3.session_lead_to_vip_24h)/iff(t3.session_lead_to_vip_24h=0 or t3.session_lead_to_vip_24h is null,1,t3.session_lead_to_vip_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Session Lead to VIP 24H %",
    to_varchar(((t1.session_lead_to_vip_24h-t4.session_lead_to_vip_24h)/iff(t4.session_lead_to_vip_24h=0 or t4.session_lead_to_vip_24h is null,1,t4.session_lead_to_vip_24h)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Session Lead to VIP 24H %",
    to_varchar(t1.session_to_vip, '999,999,999,999.00') || '%' as "Session to VIP %",
    to_varchar(((t1.session_to_vip-t2.session_to_vip)/iff(t2.session_to_vip=0 or t2.session_to_vip is null,1,t2.session_to_vip)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Session to VIP %",
    to_varchar(((t1.session_to_vip-t3.session_to_vip)/iff(t3.session_to_vip=0 or t3.session_to_vip is null,1,t3.session_to_vip)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Session to VIP %",
    to_varchar(((t1.session_to_vip-t4.session_to_vip)/iff(t4.session_to_vip=0 or t4.session_to_vip is null,1,t4.session_to_vip)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Session to VIP %",
    to_varchar(t1.pct_leads_from_session, '999,999,999,999.00') || '%' as "% Leads from Session",
    to_varchar(((t1.pct_leads_from_session-t2.pct_leads_from_session)/iff(t2.pct_leads_from_session=0 or t2.pct_leads_from_session is null,1,t2.pct_leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP % Leads from Session",
    to_varchar(((t1.pct_leads_from_session-t3.pct_leads_from_session)/iff(t3.pct_leads_from_session=0 or t3.pct_leads_from_session is null,1,t3.pct_leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date % Leads from Session",
    to_varchar(((t1.pct_leads_from_session-t4.pct_leads_from_session)/iff(t4.pct_leads_from_session=0 or t4.pct_leads_from_session is null,1,t4.pct_leads_from_session)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day % Leads from Session",
    to_varchar(t1.pct_same_session_vips, '999,999,999,999.00') || '%' as "% Same Session VIPs",
    to_varchar(((t1.pct_same_session_vips-t2.pct_same_session_vips)/iff(t2.pct_same_session_vips=0 or t2.pct_same_session_vips is null,1,t2.pct_same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP % Same Session VIPs",
    to_varchar(((t1.pct_same_session_vips-t3.pct_same_session_vips)/iff(t3.pct_same_session_vips=0 or t3.pct_same_session_vips is null,1,t3.pct_same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date % Same Session VIPs",
    to_varchar(((t1.pct_same_session_vips-t4.pct_same_session_vips)/iff(t4.pct_same_session_vips=0 or t4.pct_same_session_vips is null,1,t4.pct_same_session_vips)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day % Same Session VIPs",
    to_varchar(t1.quiz_starts, '999,999,999,999.00') as "Quiz Starts",
    to_varchar(((t1.quiz_starts-t2.quiz_starts)/iff(t2.quiz_starts=0 or t2.quiz_starts is null,1,t2.quiz_starts)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Quiz Starts",
    to_varchar(((t1.quiz_starts-t3.quiz_starts)/iff(t3.quiz_starts=0 or t3.quiz_starts is null,1,t3.quiz_starts)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Quiz Starts",
    to_varchar(((t1.quiz_starts-t4.quiz_starts)/iff(t4.quiz_starts=0 or t4.quiz_starts is null,1,t4.quiz_starts)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Quiz Starts",
    to_varchar(t1.quiz_completes, '999,999,999,999.00') as "Quiz Completes",
    to_varchar(((t1.quiz_completes-t2.quiz_completes)/iff(t2.quiz_completes=0 or t2.quiz_completes is null,1,t2.quiz_completes)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Quiz Completes",
    to_varchar(((t1.quiz_completes-t3.quiz_completes)/iff(t3.quiz_completes=0 or t3.quiz_completes is null,1,t3.quiz_completes)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Quiz Completes",
    to_varchar(((t1.quiz_completes-t4.quiz_completes)/iff(t4.quiz_completes=0 or t4.quiz_completes is null,1,t4.quiz_completes)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Quiz Completes",
    to_varchar(t1.quiz_start_to_complete, '999,999,999,999.00') || '%' as "Quiz Complete %",
    to_varchar(((t1.quiz_start_to_complete-t2.quiz_start_to_complete)/iff(t2.quiz_start_to_complete=0 or t2.quiz_start_to_complete is null,1,t2.quiz_start_to_complete)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Quiz Complete %",
    to_varchar(((t1.quiz_start_to_complete-t3.quiz_start_to_complete)/iff(t3.quiz_start_to_complete=0 or t3.quiz_start_to_complete is null,1,t3.quiz_start_to_complete)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Quiz Complete %",
    to_varchar(((t1.quiz_start_to_complete-t4.quiz_start_to_complete)/iff(t4.quiz_start_to_complete=0 or t4.quiz_start_to_complete is null,1,t4.quiz_start_to_complete)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Quiz Complete %"
from _sessions_raw_pop t1
left join _sessions_raw_pop t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.region = t2.region and t1.country = t2.country and t1.channel = t2.channel
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
          when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
          when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
          when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _sessions_raw_pop t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.region = t3.region and t1.country = t3.country and t1.channel = t3.channel
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
          when t1.date_segment = 'weekly' then t1.date_pop = t3.date
          when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _sessions_raw_pop t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.region = t4.region and t1.country = t4.country and t1.channel = t4.channel
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag = 1;

------------------------------------------------------------------------------------
-- fb+ig ui metrics

create or replace temporary table _fb_calcs as
with _fb_raw as (
select 'daily' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date,
    dateadd(year,-1,f.date) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
from reporting_media_prod.facebook.facebook_optimization_dataset_country f
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,f.date) = th.year
where  date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('week',date) as date,
    date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
from reporting_media_prod.facebook.facebook_optimization_dataset_country f
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',f.date))) = d.calendar_year and week(date_trunc('week',f.date)) = d.week_of_year
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
    from reporting_media_prod.facebook.facebook_optimization_dataset_country
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
    from reporting_media_prod.facebook.facebook_optimization_dataset_country
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    store_brand_name,
    region,
    country,
    date,
    dateadd(year,-1,f.date) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
from reporting_media_prod.facebook.facebook_optimization_dataset_country f
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,f.date) = th.year
where  date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('week',date) as date,
    date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
from reporting_media_prod.facebook.facebook_optimization_dataset_country f
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',f.date))) = d.calendar_year and week(date_trunc('week',f.date)) = d.week_of_year
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
    from reporting_media_prod.facebook.facebook_optimization_dataset_country
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
    from reporting_media_prod.facebook.facebook_optimization_dataset_country
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7)

select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    row_number() over (partition by date_segment, store_brand_name, region, country, channel order by date desc) as latest_date_flag,
    impressions,
    spend / iff(impressions=0,null,impressions) as cpm,
    clicks,
    spend / iff(clicks=0,null,clicks) as cpc,
    spend / iff(pixel_lead_1x1=0,null,pixel_lead_1x1) as ui_cpl_1x1,
    spend / iff(pixel_lead_7x1=0,null,pixel_lead_7x1) as ui_cpl_7x1,
    pixel_lead_1dc / iff(clicks=0,null,clicks) as click_to_lead_1d,
    pixel_lead_7dc / iff(clicks=0,null,clicks) as click_to_lead_7d,
    spend / iff(pixel_vip_1dc=0,null,pixel_vip_1dc) as ui_cac_1dc,
    spend / iff(pixel_vip_7dc=0,null,pixel_vip_7dc) as ui_cac_7dc,
    spend / iff(pixel_vip_1x1=0,null,pixel_vip_1x1) as ui_cac_1x1,
    spend / iff(pixel_vip_7x1=0,null,pixel_vip_7x1) as ui_cac_7x1,
    pixel_vip_1dc / iff(clicks=0,null,clicks) as click_to_vip_1d,
    pixel_vip_7dc / iff(clicks=0,null,clicks) as click_to_vip_7d
from _fb_raw;

create or replace temporary table _fb_calcs_pop as
with _fb_raw as (
select 'daily' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date,
    dateadd(year,-1,f.date) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
from reporting_media_prod.facebook.facebook_optimization_dataset_country f
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,f.date) = th.year
where  date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('week',date) as date,
    date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
from reporting_media_prod.facebook.facebook_optimization_dataset_country f
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',f.date))) = d.calendar_year and week(date_trunc('week',f.date)) = d.week_of_year
where date in (select * from _week_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
from reporting_media_prod.facebook.facebook_optimization_dataset_country
where date in (select * from _month_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
from reporting_media_prod.facebook.facebook_optimization_dataset_country
where date in (select * from _year_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    store_brand_name,
    region,
    country,
    date,
    dateadd(year,-1,f.date) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
from reporting_media_prod.facebook.facebook_optimization_dataset_country f
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,f.date) = th.year
where  date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('week',date) as date,
    date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
from reporting_media_prod.facebook.facebook_optimization_dataset_country f
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
    on year(dateadd(year, -1, date_trunc('week',f.date))) = d.calendar_year and week(date_trunc('week',f.date)) = d.week_of_year
where date in (select * from _week_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
from reporting_media_prod.facebook.facebook_optimization_dataset_country
where date in (select * from _month_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_lead_click_1d) as pixel_lead_1dc,
    sum(pixel_lead_click_7d) as pixel_lead_7dc,
    sum(pixel_lead_click_1d + pixel_lead_view_1d) as pixel_lead_1x1,
    sum(pixel_lead_click_7d + pixel_lead_view_1d) as pixel_lead_7x1,
    sum(pixel_vip_click_1d) as pixel_vip_1dc,
    sum(pixel_vip_click_7d) as pixel_vip_7dc,
    sum(pixel_vip_click_1d + pixel_vip_view_1d) as pixel_vip_1x1,
    sum(pixel_vip_click_7d + pixel_vip_view_1d) as pixel_vip_7x1
from reporting_media_prod.facebook.facebook_optimization_dataset_country
where date in (select * from _year_pop_dates)
group by 1,2,3,4,5,6,7)

select date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    row_number() over (partition by date_segment, store_brand_name, region, country, channel order by date desc) as latest_date_flag,
    impressions,
    spend / iff(impressions=0,null,impressions) as cpm,
    clicks,
    spend / iff(clicks=0,null,clicks) as cpc,
    spend / iff(pixel_lead_1x1=0,null,pixel_lead_1x1) as ui_cpl_1x1,
    spend / iff(pixel_lead_7x1=0,null,pixel_lead_7x1) as ui_cpl_7x1,
    pixel_lead_1dc / iff(clicks=0,null,clicks) as click_to_lead_1d,
    pixel_lead_7dc / iff(clicks=0,null,clicks) as click_to_lead_7d,
    spend / iff(pixel_vip_1dc=0,null,pixel_vip_1dc) as ui_cac_1dc,
    spend / iff(pixel_vip_7dc=0,null,pixel_vip_7dc) as ui_cac_7dc,
    spend / iff(pixel_vip_1x1=0,null,pixel_vip_1x1) as ui_cac_1x1,
    spend / iff(pixel_vip_7x1=0,null,pixel_vip_7x1) as ui_cac_7x1,
    pixel_vip_1dc / iff(clicks=0,null,clicks) as click_to_vip_1d,
    pixel_vip_7dc / iff(clicks=0,null,clicks) as click_to_vip_7d
from _fb_raw;

create or replace temporary table _fb_ui as
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    '$' || to_varchar(t1.ui_cpl_1x1,'999,999,999,999.00') as "UI CPL 1x1",
    to_varchar(((t1.ui_cpl_1x1-t2.ui_cpl_1x1)/iff(t2.ui_cpl_1x1=0 or t2.ui_cpl_1x1 is null,1,t2.ui_cpl_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CPL 1x1",
    to_varchar(((t1.ui_cpl_1x1-t3.ui_cpl_1x1)/iff(t3.ui_cpl_1x1=0 or t3.ui_cpl_1x1 is null,1,t3.ui_cpl_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CPL 1x1",
    to_varchar(((t1.ui_cpl_1x1-t4.ui_cpl_1x1)/iff(t4.ui_cpl_1x1=0 or t4.ui_cpl_1x1 is null,1,t4.ui_cpl_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CPL 1x1",
    '$' || to_varchar(t1.ui_cpl_7x1,'999,999,999,999.00') as "UI CPL 7x1",
    to_varchar(((t1.ui_cpl_7x1-t2.ui_cpl_7x1)/iff(t2.ui_cpl_7x1=0 or t2.ui_cpl_7x1 is null,1,t2.ui_cpl_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CPL 7x1",
    to_varchar(((t1.ui_cpl_7x1-t3.ui_cpl_7x1)/iff(t3.ui_cpl_7x1=0 or t3.ui_cpl_7x1 is null,1,t3.ui_cpl_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CPL 7x1",
    to_varchar(((t1.ui_cpl_7x1-t4.ui_cpl_7x1)/iff(t4.ui_cpl_7x1=0 or t4.ui_cpl_7x1 is null,1,t4.ui_cpl_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CPL 7x1",
    to_varchar(t1.click_to_lead_1d * 100,'999,999,999,999.00') || '%' as "UI Click to Lead % 1D",
    to_varchar(((t1.click_to_lead_1d-t2.click_to_lead_1d)/iff(t2.click_to_lead_1d=0 or t2.click_to_lead_1d is null,1,t2.click_to_lead_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to Lead % 1D",
    to_varchar(((t1.click_to_lead_1d-t3.click_to_lead_1d)/iff(t3.click_to_lead_1d=0 or t3.click_to_lead_1d is null,1,t3.click_to_lead_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to Lead % 1D",
    to_varchar(((t1.click_to_lead_1d-t4.click_to_lead_1d)/iff(t4.click_to_lead_1d=0 or t4.click_to_lead_1d is null,1,t4.click_to_lead_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to Lead % 1D",
    to_varchar(t1.click_to_lead_7d * 100,'999,999,999,999.00') || '%' as "UI Click to Lead % 7D",
    to_varchar(((t1.click_to_lead_7d-t2.click_to_lead_7d)/iff(t2.click_to_lead_7d=0 or t2.click_to_lead_7d is null,1,t2.click_to_lead_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to Lead % 7D",
    to_varchar(((t1.click_to_lead_7d-t3.click_to_lead_7d)/iff(t3.click_to_lead_7d=0 or t3.click_to_lead_7d is null,1,t3.click_to_lead_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to Lead % 7D",
    to_varchar(((t1.click_to_lead_7d-t4.click_to_lead_7d)/iff(t4.click_to_lead_7d=0 or t4.click_to_lead_7d is null,1,t4.click_to_lead_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to Lead % 7D",
    '$' || to_varchar(t1.ui_cac_1dc,'999,999,999,999.00') as "UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t2.ui_cac_1dc)/iff(t2.ui_cac_1dc=0 or t2.ui_cac_1dc is null,1,t2.ui_cac_1dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t3.ui_cac_1dc)/iff(t3.ui_cac_1dc=0 or t3.ui_cac_1dc is null,1,t3.ui_cac_1dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t4.ui_cac_1dc)/iff(t4.ui_cac_1dc=0 or t4.ui_cac_1dc is null,1,t4.ui_cac_1dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 1DC",
    '$' || to_varchar(t1.ui_cac_7dc,'999,999,999,999.00') as "UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t2.ui_cac_7dc)/iff(t2.ui_cac_7dc=0 or t2.ui_cac_7dc is null,1,t2.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t3.ui_cac_7dc)/iff(t3.ui_cac_7dc=0 or t3.ui_cac_7dc is null,1,t3.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t4.ui_cac_7dc)/iff(t4.ui_cac_7dc=0 or t4.ui_cac_7dc is null,1,t4.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 7DC",
    '$' || to_varchar(t1.ui_cac_1x1,'999,999,999,999.00') as "UI CAC 1x1",
    to_varchar(((t1.ui_cac_1x1-t2.ui_cac_1x1)/iff(t2.ui_cac_1x1=0 or t2.ui_cac_1x1 is null,1,t2.ui_cac_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 1x1",
    to_varchar(((t1.ui_cac_1x1-t3.ui_cac_1x1)/iff(t3.ui_cac_1x1=0 or t3.ui_cac_1x1 is null,1,t3.ui_cac_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 1x1",
    to_varchar(((t1.ui_cac_1x1-t4.ui_cac_1x1)/iff(t4.ui_cac_1x1=0 or t4.ui_cac_1x1 is null,1,t4.ui_cac_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 1x1",
    '$' || to_varchar(t1.ui_cac_7x1,'999,999,999,999.00') as "UI CAC 7x1",
    to_varchar(((t1.ui_cac_7x1-t2.ui_cac_7x1)/iff(t2.ui_cac_7x1=0 or t2.ui_cac_7x1 is null,1,t2.ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 7x1",
    to_varchar(((t1.ui_cac_7x1-t3.ui_cac_7x1)/iff(t3.ui_cac_7x1=0 or t3.ui_cac_7x1 is null,1,t3.ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 7x1",
    to_varchar(((t1.ui_cac_7x1-t4.ui_cac_7x1)/iff(t4.ui_cac_7x1=0 or t4.ui_cac_7x1 is null,1,t4.ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 7x1",
    to_varchar(t1.click_to_vip_1d * 100,'999,999,999,999.00') || '%' as "UI Click to VIP % 1D",
    to_varchar(((t1.click_to_vip_1d-t2.click_to_vip_1d)/iff(t2.click_to_vip_1d=0 or t2.click_to_vip_1d is null,1,t2.click_to_vip_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to VIP % 1D",
    to_varchar(((t1.click_to_vip_1d-t3.click_to_vip_1d)/iff(t3.click_to_vip_1d=0 or t3.click_to_vip_1d is null,1,t3.click_to_vip_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to VIP % 1D",
    to_varchar(((t1.click_to_vip_1d-t4.click_to_vip_1d)/iff(t4.click_to_vip_1d=0 or t4.click_to_vip_1d is null,1,t4.click_to_vip_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to VIP % 1D",
    to_varchar(t1.click_to_vip_7d * 100,'999,999,999,999.00') || '%' as "UI Click to VIP % 7D",
    to_varchar(((t1.click_to_vip_7d-t2.click_to_vip_7d)/iff(t2.click_to_vip_7d=0 or t2.click_to_vip_7d is null,1,t2.click_to_vip_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to VIP % 7D",
    to_varchar(((t1.click_to_vip_7d-t3.click_to_vip_7d)/iff(t3.click_to_vip_7d=0 or t3.click_to_vip_7d is null,1,t3.click_to_vip_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to VIP % 7D",
    to_varchar(((t1.click_to_vip_7d-t4.click_to_vip_7d)/iff(t4.click_to_vip_7d=0 or t4.click_to_vip_7d is null,1,t4.click_to_vip_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to VIP % 7D"
from _fb_calcs t1
left join _fb_calcs t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
                                    when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
                                    when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
                                    when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _fb_calcs t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
                                    when t1.date_segment = 'weekly' then t1.date_pop = t3.date
                                    when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _fb_calcs t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag > 1
union all
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    '$' || to_varchar(t1.ui_cpl_1x1,'999,999,999,999.00') as "UI CPL 1x1",
    to_varchar(((t1.ui_cpl_1x1-t2.ui_cpl_1x1)/iff(t2.ui_cpl_1x1=0 or t2.ui_cpl_1x1 is null,1,t2.ui_cpl_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CPL 1x1",
    to_varchar(((t1.ui_cpl_1x1-t3.ui_cpl_1x1)/iff(t3.ui_cpl_1x1=0 or t3.ui_cpl_1x1 is null,1,t3.ui_cpl_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CPL 1x1",
    to_varchar(((t1.ui_cpl_1x1-t4.ui_cpl_1x1)/iff(t4.ui_cpl_1x1=0 or t4.ui_cpl_1x1 is null,1,t4.ui_cpl_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CPL 1x1",
    '$' || to_varchar(t1.ui_cpl_7x1,'999,999,999,999.00') as "UI CPL 7x1",
    to_varchar(((t1.ui_cpl_7x1-t2.ui_cpl_7x1)/iff(t2.ui_cpl_7x1=0 or t2.ui_cpl_7x1 is null,1,t2.ui_cpl_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CPL 7x1",
    to_varchar(((t1.ui_cpl_7x1-t3.ui_cpl_7x1)/iff(t3.ui_cpl_7x1=0 or t3.ui_cpl_7x1 is null,1,t3.ui_cpl_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CPL 7x1",
    to_varchar(((t1.ui_cpl_7x1-t4.ui_cpl_7x1)/iff(t4.ui_cpl_7x1=0 or t4.ui_cpl_7x1 is null,1,t4.ui_cpl_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CPL 7x1",
    to_varchar(t1.click_to_lead_1d * 100,'999,999,999,999.00') || '%' as "UI Click to Lead % 1D",
    to_varchar(((t1.click_to_lead_1d-t2.click_to_lead_1d)/iff(t2.click_to_lead_1d=0 or t2.click_to_lead_1d is null,1,t2.click_to_lead_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to Lead % 1D",
    to_varchar(((t1.click_to_lead_1d-t3.click_to_lead_1d)/iff(t3.click_to_lead_1d=0 or t3.click_to_lead_1d is null,1,t3.click_to_lead_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to Lead % 1D",
    to_varchar(((t1.click_to_lead_1d-t4.click_to_lead_1d)/iff(t4.click_to_lead_1d=0 or t4.click_to_lead_1d is null,1,t4.click_to_lead_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to Lead % 1D",
    to_varchar(t1.click_to_lead_7d * 100,'999,999,999,999.00') || '%' as "UI Click to Lead % 7D",
    to_varchar(((t1.click_to_lead_7d-t2.click_to_lead_7d)/iff(t2.click_to_lead_7d=0 or t2.click_to_lead_7d is null,1,t2.click_to_lead_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to Lead % 7D",
    to_varchar(((t1.click_to_lead_7d-t3.click_to_lead_7d)/iff(t3.click_to_lead_7d=0 or t3.click_to_lead_7d is null,1,t3.click_to_lead_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to Lead % 7D",
    to_varchar(((t1.click_to_lead_7d-t4.click_to_lead_7d)/iff(t4.click_to_lead_7d=0 or t4.click_to_lead_7d is null,1,t4.click_to_lead_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to Lead % 7D",
    '$' || to_varchar(t1.ui_cac_1dc,'999,999,999,999.00') as "UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t2.ui_cac_1dc)/iff(t2.ui_cac_1dc=0 or t2.ui_cac_1dc is null,1,t2.ui_cac_1dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t3.ui_cac_1dc)/iff(t3.ui_cac_1dc=0 or t3.ui_cac_1dc is null,1,t3.ui_cac_1dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t4.ui_cac_1dc)/iff(t4.ui_cac_1dc=0 or t4.ui_cac_1dc is null,1,t4.ui_cac_1dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 1DC",
    '$' || to_varchar(t1.ui_cac_7dc,'999,999,999,999.00') as "UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t2.ui_cac_7dc)/iff(t2.ui_cac_7dc=0 or t2.ui_cac_7dc is null,1,t2.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t3.ui_cac_7dc)/iff(t3.ui_cac_7dc=0 or t3.ui_cac_7dc is null,1,t3.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t4.ui_cac_7dc)/iff(t4.ui_cac_7dc=0 or t4.ui_cac_7dc is null,1,t4.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 7DC",
    '$' || to_varchar(t1.ui_cac_1x1,'999,999,999,999.00') as "UI CAC 1x1",
    to_varchar(((t1.ui_cac_1x1-t2.ui_cac_1x1)/iff(t2.ui_cac_1x1=0 or t2.ui_cac_1x1 is null,1,t2.ui_cac_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 1x1",
    to_varchar(((t1.ui_cac_1x1-t3.ui_cac_1x1)/iff(t3.ui_cac_1x1=0 or t3.ui_cac_1x1 is null,1,t3.ui_cac_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 1x1",
    to_varchar(((t1.ui_cac_1x1-t4.ui_cac_1x1)/iff(t4.ui_cac_1x1=0 or t4.ui_cac_1x1 is null,1,t4.ui_cac_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 1x1",
    '$' || to_varchar(t1.ui_cac_7x1,'999,999,999,999.00') as "UI CAC 7x1",
    to_varchar(((t1.ui_cac_7x1-t2.ui_cac_7x1)/iff(t2.ui_cac_7x1=0 or t2.ui_cac_7x1 is null,1,t2.ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 7x1",
    to_varchar(((t1.ui_cac_7x1-t3.ui_cac_7x1)/iff(t3.ui_cac_7x1=0 or t3.ui_cac_7x1 is null,1,t3.ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 7x1",
    to_varchar(((t1.ui_cac_7x1-t4.ui_cac_7x1)/iff(t4.ui_cac_7x1=0 or t4.ui_cac_7x1 is null,1,t4.ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 7x1",
    to_varchar(t1.click_to_vip_1d * 100,'999,999,999,999.00') || '%' as "UI Click to VIP % 1D",
    to_varchar(((t1.click_to_vip_1d-t2.click_to_vip_1d)/iff(t2.click_to_vip_1d=0 or t2.click_to_vip_1d is null,1,t2.click_to_vip_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to VIP % 1D",
    to_varchar(((t1.click_to_vip_1d-t3.click_to_vip_1d)/iff(t3.click_to_vip_1d=0 or t3.click_to_vip_1d is null,1,t3.click_to_vip_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to VIP % 1D",
    to_varchar(((t1.click_to_vip_1d-t4.click_to_vip_1d)/iff(t4.click_to_vip_1d=0 or t4.click_to_vip_1d is null,1,t4.click_to_vip_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to VIP % 1D",
    to_varchar(t1.click_to_vip_7d * 100,'999,999,999,999.00') || '%' as "UI Click to VIP % 7D",
    to_varchar(((t1.click_to_vip_7d-t2.click_to_vip_7d)/iff(t2.click_to_vip_7d=0 or t2.click_to_vip_7d is null,1,t2.click_to_vip_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to VIP % 7D",
    to_varchar(((t1.click_to_vip_7d-t3.click_to_vip_7d)/iff(t3.click_to_vip_7d=0 or t3.click_to_vip_7d is null,1,t3.click_to_vip_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to VIP % 7D",
    to_varchar(((t1.click_to_vip_7d-t4.click_to_vip_7d)/iff(t4.click_to_vip_7d=0 or t4.click_to_vip_7d is null,1,t4.click_to_vip_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to VIP % 7D"
from _fb_calcs_pop t1
left join _fb_calcs_pop t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
                                    when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
                                    when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
                                    when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _fb_calcs_pop t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
                                    when t1.date_segment = 'weekly' then t1.date_pop = t3.date
                                    when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _fb_calcs_pop t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag = 1;

------------------------------------------------------------------------------------
-- google ads ui metrics (youtube, display, discovery)

create or replace temporary table _google_calcs as
with _google_ads_raw as (
select 'daily' as date_segment,
       store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date,
       dateadd(year,-1,g.date) + delta_ly_comp_this_year as date_pop,
       case when channel in ('display','discovery') then 'programmatic-gdn' else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
       sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
       sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset g
         join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,g.date) = th.year
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
       store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date_trunc('week',date) as date,
       date_pop,
       case when channel in ('display','discovery') then 'programmatic-gdn' else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
       sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
       sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset g
         join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
              on year(dateadd(year, -1, date_trunc('week',g.date))) = d.calendar_year and week(date_trunc('week',g.date)) = d.week_of_year
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
       store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date_trunc('month',date) as date,
       dateadd(year,-1,date_trunc('month',date)) as date_pop,
       case when channel in ('display','discovery') then 'programmatic-gdn' else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
       sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
       sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
       store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date_trunc('year',date) as date,
       dateadd(year,-1,date_trunc('year',date)) as date_pop,
       case when channel in ('display','discovery') then 'programmatic-gdn' else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
       sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
       sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
       store_brand_name,
       region,
       country,
       date,
       dateadd(year,-1,g.date) + delta_ly_comp_this_year as date_pop,
       case when channel in ('display','discovery') then 'programmatic-gdn' else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
       sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
       sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset g
         join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,g.date) = th.year
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
       store_brand_name,
       region,
       country,
       date_trunc('week',date) as date,
       date_pop,
       case when channel in ('display','discovery') then 'programmatic-gdn' else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
       sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
       sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset g
         join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
              on year(dateadd(year, -1, date_trunc('week',g.date))) = d.calendar_year and week(date_trunc('week',g.date)) = d.week_of_year
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
       store_brand_name,
       region,
       country,
       date_trunc('month',date) as date,
       dateadd(year,-1,date_trunc('month',date)) as date_pop,
       case when channel in ('display','discovery') then 'programmatic-gdn' else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
       sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
       sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
       store_brand_name,
       region,
       country,
       date_trunc('year',date) as date,
       dateadd(year,-1,date_trunc('year',date)) as date_pop,
       case when channel in ('display','discovery') then 'programmatic-gdn' else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
       sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
       sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
       sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
       sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
       sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7)

select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    row_number() over (partition by date_segment, store_brand_name, region, country, channel order by date desc) as latest_date_flag,
    impressions,
    spend / iff(impressions=0,null,impressions) as cpm,
    clicks,
    spend / iff(clicks=0,null,clicks) as cpc,
    spend / iff(ui_vip_7dc=0,null,ui_vip_7dc) as ui_cac_7dc,
    spend / iff(ui_vip_7x3x1=0,null,ui_vip_7x3x1) as ui_cac_7x3x1,
    spend / iff(ui_vip_30dc=0,null,ui_vip_30dc) as ui_cac_30dc,
    spend / iff(ui_vip_30x7x1=0,null,ui_vip_30x7x1) as ui_cac_30x7x1,
    spend / iff(discovery_ui_vip_7dc=0,null,discovery_ui_vip_7dc) as discovery_ui_cac_7dc,
    spend / iff(discovery_ui_vip_7x1=0,null,discovery_ui_vip_7x1) as discovery_ui_cac_7x1,
    spend / iff(gdn_ui_vip_7dc=0,null,gdn_ui_vip_7dc) as gdn_ui_cac_7dc,
    spend / iff(gdn_ui_vip_7x1=0,null,gdn_ui_vip_7x1) as gdn_ui_cac_7x1
from _google_ads_raw;

create or replace temporary table _google_calcs_pop as
with _google_ads_raw as (
select 'daily' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date,
    dateadd(year,-1,g.date) + delta_ly_comp_this_year as date_pop,
    case when channel in ('display','discovery') then 'programmatic-gdn'
        else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
    sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
    sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset g
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,g.date) = th.year
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('week',date) as date,
    date_pop,
    case when channel in ('display','discovery') then 'programmatic-gdn'
        else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
    sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
    sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset g
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',g.date))) = d.calendar_year and week(date_trunc('week',g.date)) = d.week_of_year
where date in (select * from _week_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    case when channel in ('display','discovery') then 'programmatic-gdn'
        else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
    sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
    sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset
where date in (select * from _month_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    case when channel in ('display','discovery') then 'programmatic-gdn'
        else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
    sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
    sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset
where date in (select * from _year_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    store_brand_name,
    region,
    country,
    date,
    dateadd(year,-1,g.date) + delta_ly_comp_this_year as date_pop,
    case when channel in ('display','discovery') then 'programmatic-gdn'
        else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
    sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
    sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset g
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,g.date) = th.year
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('week',date) as date,
    date_pop,
    case when channel in ('display','discovery') then 'programmatic-gdn'
        else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
    sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
    sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset g
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',g.date))) = d.calendar_year and week(date_trunc('week',g.date)) = d.week_of_year
where date in (select * from _week_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    case when channel in ('display','discovery') then 'programmatic-gdn'
        else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
    sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
    sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset
where date in (select * from _month_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    case when channel in ('display','discovery') then 'programmatic-gdn'
        else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click, null)) as ui_vip_7dc,
    sum(iff(channel = 'youtube', pixel_vip_7x3x1_click + pixel_vip_7x3x1_view, null)) as ui_vip_7x3x1,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click, null)) as ui_vip_30dc,
    sum(iff(channel = 'youtube', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view, null)) as ui_vip_30x7x1,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click, null)) as discovery_ui_vip_7dc,
    sum(iff(channel = 'discovery', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as discovery_ui_vip_7x1,
    sum(iff(channel = 'display', pixel_vip_7x1_click, null)) as gdn_ui_vip_7dc,
    sum(iff(channel = 'display', pixel_vip_7x1_click + pixel_vip_7x1_view, null)) as gdn_ui_vip_7x1
from reporting_media_prod.google_ads.google_ads_optimization_dataset
where date in (select * from _year_pop_dates)
group by 1,2,3,4,5,6,7)

select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    row_number() over (partition by date_segment, store_brand_name, region, country, channel order by date desc) as latest_date_flag,
    impressions,
    spend / iff(impressions=0,null,impressions) as cpm,
    clicks,
    spend / iff(clicks=0,null,clicks) as cpc,
    spend / iff(ui_vip_7dc=0,null,ui_vip_7dc) as ui_cac_7dc,
    spend / iff(ui_vip_7x3x1=0,null,ui_vip_7x3x1) as ui_cac_7x3x1,
    spend / iff(ui_vip_30dc=0,null,ui_vip_30dc) as ui_cac_30dc,
    spend / iff(ui_vip_30x7x1=0,null,ui_vip_30x7x1) as ui_cac_30x7x1,
    spend / iff(discovery_ui_vip_7dc=0,null,discovery_ui_vip_7dc) as discovery_ui_cac_7dc,
    spend / iff(discovery_ui_vip_7x1=0,null,discovery_ui_vip_7x1) as discovery_ui_cac_7x1,
    spend / iff(gdn_ui_vip_7dc=0,null,gdn_ui_vip_7dc) as gdn_ui_cac_7dc,
    spend / iff(gdn_ui_vip_7x1=0,null,gdn_ui_vip_7x1) as gdn_ui_cac_7x1
from _google_ads_raw;

create or replace temporary table _google_ads_ui as
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    '$' || to_varchar(t1.ui_cac_7dc,'999,999,999,999.00') as "UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t2.ui_cac_7dc)/iff(t2.ui_cac_7dc=0 or t2.ui_cac_7dc is null,1,t2.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t3.ui_cac_7dc)/iff(t3.ui_cac_7dc=0 or t3.ui_cac_7dc is null,1,t3.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t4.ui_cac_7dc)/iff(t4.ui_cac_7dc=0 or t4.ui_cac_7dc is null,1,t4.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 7DC",
    '$' || to_varchar(t1.ui_cac_7x3x1,'999,999,999,999.00') as "UI CAC 7x3x1",
    to_varchar(((t1.ui_cac_7x3x1-t2.ui_cac_7x3x1)/iff(t2.ui_cac_7x3x1=0 or t2.ui_cac_7x3x1 is null,1,t2.ui_cac_7x3x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 7x3x1",
    to_varchar(((t1.ui_cac_7x3x1-t3.ui_cac_7x3x1)/iff(t3.ui_cac_7x3x1=0 or t3.ui_cac_7x3x1 is null,1,t3.ui_cac_7x3x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 7x3x1",
    to_varchar(((t1.ui_cac_7x3x1-t4.ui_cac_7x3x1)/iff(t4.ui_cac_7x3x1=0 or t4.ui_cac_7x3x1 is null,1,t4.ui_cac_7x3x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 7x3x1",
    '$' || to_varchar(t1.ui_cac_30dc,'999,999,999,999.00') as "UI CAC 30DC",
    to_varchar(((t1.ui_cac_30dc-t2.ui_cac_30dc)/iff(t2.ui_cac_30dc=0 or t2.ui_cac_30dc is null,1,t2.ui_cac_30dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 30DC",
    to_varchar(((t1.ui_cac_30dc-t3.ui_cac_30dc)/iff(t3.ui_cac_30dc=0 or t3.ui_cac_30dc is null,1,t3.ui_cac_30dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 30DC",
    to_varchar(((t1.ui_cac_30dc-t4.ui_cac_30dc)/iff(t4.ui_cac_30dc=0 or t4.ui_cac_30dc is null,1,t4.ui_cac_30dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 30DC",
    '$' || to_varchar(t1.ui_cac_30x7x1,'999,999,999,999.00') as "UI CAC 30x7x1",
    to_varchar(((t1.ui_cac_30x7x1-t2.ui_cac_30x7x1)/iff(t2.ui_cac_30x7x1=0 or t2.ui_cac_30x7x1 is null,1,t2.ui_cac_30x7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 30x7x1",
    to_varchar(((t1.ui_cac_30x7x1-t3.ui_cac_30x7x1)/iff(t3.ui_cac_30x7x1=0 or t3.ui_cac_30x7x1 is null,1,t3.ui_cac_30x7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 30x7x1",
    to_varchar(((t1.ui_cac_30x7x1-t4.ui_cac_30x7x1)/iff(t4.ui_cac_30x7x1=0 or t4.ui_cac_30x7x1 is null,1,t4.ui_cac_30x7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 30x7x1",
    '$' || to_varchar(t1.discovery_ui_cac_7dc,'999,999,999,999.00') as "Discovery UI CAC 7DC",
    to_varchar(((t1.discovery_ui_cac_7dc-t2.discovery_ui_cac_7dc)/iff(t2.discovery_ui_cac_7dc=0 or t2.discovery_ui_cac_7dc is null,1,t2.discovery_ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Discovery UI CAC 7DC",
    to_varchar(((t1.discovery_ui_cac_7dc-t3.discovery_ui_cac_7dc)/iff(t3.discovery_ui_cac_7dc=0 or t3.discovery_ui_cac_7dc is null,1,t3.discovery_ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Discovery UI CAC 7DC",
    to_varchar(((t1.discovery_ui_cac_7dc-t4.discovery_ui_cac_7dc)/iff(t4.discovery_ui_cac_7dc=0 or t4.discovery_ui_cac_7dc is null,1,t4.discovery_ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Discovery UI CAC 7DC",
    '$' || to_varchar(t1.discovery_ui_cac_7x1,'999,999,999,999.00') as "Discovery UI CAC 7x1",
    to_varchar(((t1.discovery_ui_cac_7x1-t2.discovery_ui_cac_7x1)/iff(t2.discovery_ui_cac_7x1=0 or t2.discovery_ui_cac_7x1 is null,1,t2.discovery_ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Discovery UI CAC 7x1",
    to_varchar(((t1.discovery_ui_cac_7x1-t3.discovery_ui_cac_7x1)/iff(t3.discovery_ui_cac_7x1=0 or t3.discovery_ui_cac_7x1 is null,1,t3.discovery_ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Discovery UI CAC 7x1",
    to_varchar(((t1.discovery_ui_cac_7x1-t4.discovery_ui_cac_7x1)/iff(t4.discovery_ui_cac_7x1=0 or t4.discovery_ui_cac_7x1 is null,1,t4.discovery_ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Discovery UI CAC 7x1",
    '$' || to_varchar(t1.gdn_ui_cac_7dc,'999,999,999,999.00') as "GDN UI CAC 7DC",
    to_varchar(((t1.gdn_ui_cac_7dc-t2.gdn_ui_cac_7dc)/iff(t2.gdn_ui_cac_7dc=0 or t2.gdn_ui_cac_7dc is null,1,t2.gdn_ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP GDN UI CAC 7DC",
    to_varchar(((t1.gdn_ui_cac_7dc-t3.gdn_ui_cac_7dc)/iff(t3.gdn_ui_cac_7dc=0 or t3.gdn_ui_cac_7dc is null,1,t3.gdn_ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date GDN UI CAC 7DC",
    to_varchar(((t1.gdn_ui_cac_7dc-t4.gdn_ui_cac_7dc)/iff(t4.gdn_ui_cac_7dc=0 or t4.gdn_ui_cac_7dc is null,1,t4.gdn_ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day GDN UI CAC 7DC",
    '$' || to_varchar(t1.gdn_ui_cac_7x1, '999,999,999,999.00') as "GDN UI CAC 7x1",
    to_varchar(((t1.gdn_ui_cac_7x1-t2.gdn_ui_cac_7x1)/iff(t2.gdn_ui_cac_7x1=0 or t2.gdn_ui_cac_7x1 is null,1,t2.gdn_ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP GDN UI CAC 7x1",
    to_varchar(((t1.gdn_ui_cac_7x1-t3.gdn_ui_cac_7x1)/iff(t3.gdn_ui_cac_7x1=0 or t3.gdn_ui_cac_7x1 is null,1,t3.gdn_ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date GDN UI CAC 7x1",
    to_varchar(((t1.gdn_ui_cac_7x1-t4.gdn_ui_cac_7x1)/iff(t4.gdn_ui_cac_7x1=0 or t4.gdn_ui_cac_7x1 is null,1,t4.gdn_ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day GDN UI CAC 7x1"
from _google_calcs t1
left join _google_calcs t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
                                    when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
                                    when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
                                    when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _google_calcs t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
                                    when t1.date_segment = 'weekly' then t1.date_pop = t3.date
                                    when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _google_calcs t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag > 1
union all
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    '$' || to_varchar(t1.ui_cac_7dc,'999,999,999,999.00') as "UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t2.ui_cac_7dc)/iff(t2.ui_cac_7dc=0 or t2.ui_cac_7dc is null,1,t2.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t3.ui_cac_7dc)/iff(t3.ui_cac_7dc=0 or t3.ui_cac_7dc is null,1,t3.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t4.ui_cac_7dc)/iff(t4.ui_cac_7dc=0 or t4.ui_cac_7dc is null,1,t4.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 7DC",
    '$' || to_varchar(t1.ui_cac_7x3x1,'999,999,999,999.00') as "UI CAC 7x3x1",
    to_varchar(((t1.ui_cac_7x3x1-t2.ui_cac_7x3x1)/iff(t2.ui_cac_7x3x1=0 or t2.ui_cac_7x3x1 is null,1,t2.ui_cac_7x3x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 7x3x1",
    to_varchar(((t1.ui_cac_7x3x1-t3.ui_cac_7x3x1)/iff(t3.ui_cac_7x3x1=0 or t3.ui_cac_7x3x1 is null,1,t3.ui_cac_7x3x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 7x3x1",
    to_varchar(((t1.ui_cac_7x3x1-t4.ui_cac_7x3x1)/iff(t4.ui_cac_7x3x1=0 or t4.ui_cac_7x3x1 is null,1,t4.ui_cac_7x3x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 7x3x1",
    '$' || to_varchar(t1.ui_cac_30dc,'999,999,999,999.00') as "UI CAC 30DC",
    to_varchar(((t1.ui_cac_30dc-t2.ui_cac_30dc)/iff(t2.ui_cac_30dc=0 or t2.ui_cac_30dc is null,1,t2.ui_cac_30dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 30DC",
    to_varchar(((t1.ui_cac_30dc-t3.ui_cac_30dc)/iff(t3.ui_cac_30dc=0 or t3.ui_cac_30dc is null,1,t3.ui_cac_30dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 30DC",
    to_varchar(((t1.ui_cac_30dc-t4.ui_cac_30dc)/iff(t4.ui_cac_30dc=0 or t4.ui_cac_30dc is null,1,t4.ui_cac_30dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 30DC",
    '$' || to_varchar(t1.ui_cac_30x7x1,'999,999,999,999.00') as "UI CAC 30x7x1",
    to_varchar(((t1.ui_cac_30x7x1-t2.ui_cac_30x7x1)/iff(t2.ui_cac_30x7x1=0 or t2.ui_cac_30x7x1 is null,1,t2.ui_cac_30x7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 30x7x1",
    to_varchar(((t1.ui_cac_30x7x1-t3.ui_cac_30x7x1)/iff(t3.ui_cac_30x7x1=0 or t3.ui_cac_30x7x1 is null,1,t3.ui_cac_30x7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 30x7x1",
    to_varchar(((t1.ui_cac_30x7x1-t4.ui_cac_30x7x1)/iff(t4.ui_cac_30x7x1=0 or t4.ui_cac_30x7x1 is null,1,t4.ui_cac_30x7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 30x7x1",
    '$' || to_varchar(t1.discovery_ui_cac_7dc,'999,999,999,999.00') as "Discovery UI CAC 7DC",
    to_varchar(((t1.discovery_ui_cac_7dc-t2.discovery_ui_cac_7dc)/iff(t2.discovery_ui_cac_7dc=0 or t2.discovery_ui_cac_7dc is null,1,t2.discovery_ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Discovery UI CAC 7DC",
    to_varchar(((t1.discovery_ui_cac_7dc-t3.discovery_ui_cac_7dc)/iff(t3.discovery_ui_cac_7dc=0 or t3.discovery_ui_cac_7dc is null,1,t3.discovery_ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Discovery UI CAC 7DC",
    to_varchar(((t1.discovery_ui_cac_7dc-t4.discovery_ui_cac_7dc)/iff(t4.discovery_ui_cac_7dc=0 or t4.discovery_ui_cac_7dc is null,1,t4.discovery_ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Discovery UI CAC 7DC",
    '$' || to_varchar(t1.discovery_ui_cac_7x1,'999,999,999,999.00') as "Discovery UI CAC 7x1",
    to_varchar(((t1.discovery_ui_cac_7x1-t2.discovery_ui_cac_7x1)/iff(t2.discovery_ui_cac_7x1=0 or t2.discovery_ui_cac_7x1 is null,1,t2.discovery_ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Discovery UI CAC 7x1",
    to_varchar(((t1.discovery_ui_cac_7x1-t3.discovery_ui_cac_7x1)/iff(t3.discovery_ui_cac_7x1=0 or t3.discovery_ui_cac_7x1 is null,1,t3.discovery_ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Discovery UI CAC 7x1",
    to_varchar(((t1.discovery_ui_cac_7x1-t4.discovery_ui_cac_7x1)/iff(t4.discovery_ui_cac_7x1=0 or t4.discovery_ui_cac_7x1 is null,1,t4.discovery_ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Discovery UI CAC 7x1",
    '$' || to_varchar(t1.gdn_ui_cac_7dc,'999,999,999,999.00') as "GDN UI CAC 7DC",
    to_varchar(((t1.gdn_ui_cac_7dc-t2.gdn_ui_cac_7dc)/iff(t2.gdn_ui_cac_7dc=0 or t2.gdn_ui_cac_7dc is null,1,t2.gdn_ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP GDN UI CAC 7DC",
    to_varchar(((t1.gdn_ui_cac_7dc-t3.gdn_ui_cac_7dc)/iff(t3.gdn_ui_cac_7dc=0 or t3.gdn_ui_cac_7dc is null,1,t3.gdn_ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date GDN UI CAC 7DC",
    to_varchar(((t1.gdn_ui_cac_7dc-t4.gdn_ui_cac_7dc)/iff(t4.gdn_ui_cac_7dc=0 or t4.gdn_ui_cac_7dc is null,1,t4.gdn_ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day GDN UI CAC 7DC",
    '$' || to_varchar(t1.gdn_ui_cac_7x1, '999,999,999,999.00') as "GDN UI CAC 7x1",
    to_varchar(((t1.gdn_ui_cac_7x1-t2.gdn_ui_cac_7x1)/iff(t2.gdn_ui_cac_7x1=0 or t2.gdn_ui_cac_7x1 is null,1,t2.gdn_ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP GDN UI CAC 7x1",
    to_varchar(((t1.gdn_ui_cac_7x1-t3.gdn_ui_cac_7x1)/iff(t3.gdn_ui_cac_7x1=0 or t3.gdn_ui_cac_7x1 is null,1,t3.gdn_ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date GDN UI CAC 7x1",
    to_varchar(((t1.gdn_ui_cac_7x1-t4.gdn_ui_cac_7x1)/iff(t4.gdn_ui_cac_7x1=0 or t4.gdn_ui_cac_7x1 is null,1,t4.gdn_ui_cac_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day GDN UI CAC 7x1"
from _google_calcs_pop t1
left join _google_calcs_pop t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
                                    when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
                                    when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
                                    when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _google_calcs_pop t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
                                    when t1.date_segment = 'weekly' then t1.date_pop = t3.date
                                    when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _google_calcs_pop t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag = 1;

------------------------------------------------------------------------------------
-- google ads ui metrics (shopping, nonbrand, brand search)

create or replace temporary table _google_search_calcs as
with _google_search_ads_raw as (
select 'daily' as date_segment,
       store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date,
       dateadd(year,-1,g.date) + delta_ly_comp_this_year as date_pop,
       case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
           when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
        else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(pixel_vip_1dc_click) as ui_vip_1dc,
       sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
       sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
       sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
       sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from reporting_media_prod.google_ads.search_optimization_dataset g
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,g.date) = th.year
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
       store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date_trunc('week',date) as date,
       date_pop,
       case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
           when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
        else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(pixel_vip_1dc_click) as ui_vip_1dc,
       sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
       sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
       sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
       sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from reporting_media_prod.google_ads.search_optimization_dataset g
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
              on year(dateadd(year, -1, date_trunc('week',g.date))) = d.calendar_year and week(date_trunc('week',g.date)) = d.week_of_year
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
       store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date_trunc('month',date) as date,
       dateadd(year,-1,date_trunc('month',date)) as date_pop,
        case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
           when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
        else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(pixel_vip_1dc_click) as ui_vip_1dc,
       sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
       sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
       sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
       sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from  reporting_media_prod.google_ads.search_optimization_dataset
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
       store_brand_name,
       region,
       iff(lower(region)='na','US+CA',region) as country,
       date_trunc('year',date) as date,
       dateadd(year,-1,date_trunc('year',date)) as date_pop,
       case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
           when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
        else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(pixel_vip_1dc_click) as ui_vip_1dc,
       sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
       sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
       sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
       sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from  reporting_media_prod.google_ads.search_optimization_dataset
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
       store_brand_name,
       region,
       country,
       date,
       dateadd(year,-1,g.date) + delta_ly_comp_this_year as date_pop,
       case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
           when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
        else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(pixel_vip_1dc_click) as ui_vip_1dc,
       sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
       sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
       sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
       sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from reporting_media_prod.google_ads.search_optimization_dataset g
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,g.date) = th.year
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
       store_brand_name,
       region,
       country,
       date_trunc('week',date) as date,
       date_pop,
       case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
           when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
        else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(pixel_vip_1dc_click) as ui_vip_1dc,
       sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
       sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
       sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
       sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from reporting_media_prod.google_ads.search_optimization_dataset g
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
              on year(dateadd(year, -1, date_trunc('week',g.date))) = d.calendar_year and week(date_trunc('week',g.date)) = d.week_of_year
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
       store_brand_name,
       region,
       country,
       date_trunc('month',date) as date,
       dateadd(year,-1,date_trunc('month',date)) as date_pop,
        case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
           when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
        else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(pixel_vip_1dc_click) as ui_vip_1dc,
       sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
       sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
       sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
       sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from  reporting_media_prod.google_ads.search_optimization_dataset
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
       store_brand_name,
       region,
       country,
       date_trunc('year',date) as date,
       dateadd(year,-1,date_trunc('year',date)) as date_pop,
       case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
           when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
        else channel end as channel,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(pixel_vip_1dc_click) as ui_vip_1dc,
       sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
       sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
       sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
       sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from  reporting_media_prod.google_ads.search_optimization_dataset
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7)

select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    row_number() over (partition by date_segment, store_brand_name, region, country, channel order by date desc) as latest_date_flag,
    impressions,
    spend / iff(impressions=0,null,impressions) as cpm,
    clicks,
    spend / iff(clicks=0,null,clicks) as cpc,
    spend / iff(ui_vip_1dc=0,null,ui_vip_1dc) as ui_cac_1dc,
    spend / iff(ui_vip_7dc=0,null,ui_vip_7dc) as ui_cac_7dc,
    spend / iff(ui_vip_7x3x1=0,null,ui_vip_7x3x1) as ui_cac_7x3x1,
    spend / iff(ui_vip_30dc=0,null,ui_vip_30dc) as ui_cac_30dc,
    spend / iff(ui_vip_30x7x1=0,null,ui_vip_30x7x1) as ui_cac_30x7x1
from _google_search_ads_raw;



create or replace temporary table _google_search_calcs_pop as
with _google_search_ads_raw as (
select 'daily' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date,
    dateadd(year,-1,g.date) + delta_ly_comp_this_year as date_pop,
    case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
       when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
    else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_vip_1dc_click) as ui_vip_1dc,
    sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
    sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
    sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
    sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from reporting_media_prod.google_ads.search_optimization_dataset g
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,g.date) = th.year
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('week',date) as date,
    date_pop,
    case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
       when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
    else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_vip_1dc_click) as ui_vip_1dc,
    sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
    sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
    sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
    sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from reporting_media_prod.google_ads.search_optimization_dataset g
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',g.date))) = d.calendar_year and week(date_trunc('week',g.date)) = d.week_of_year
where date in (select * from _week_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
       when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
    else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_vip_1dc_click) as ui_vip_1dc,
    sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
    sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
    sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
    sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from reporting_media_prod.google_ads.search_optimization_dataset g
where date in (select * from _month_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
       when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
    else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_vip_1dc_click) as ui_vip_1dc,
    sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
    sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
    sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
    sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from reporting_media_prod.google_ads.search_optimization_dataset g
where date in (select * from _year_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    store_brand_name,
    region,
    country,
    date,
    dateadd(year,-1,g.date) + delta_ly_comp_this_year as date_pop,
    case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
       when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
    else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_vip_1dc_click) as ui_vip_1dc,
    sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
    sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
    sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
    sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from reporting_media_prod.google_ads.search_optimization_dataset g
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,g.date) = th.year
where date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('week',date) as date,
    date_pop,
    case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
       when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
    else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_vip_1dc_click) as ui_vip_1dc,
    sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
    sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
    sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
    sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from reporting_media_prod.google_ads.search_optimization_dataset g
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',g.date))) = d.calendar_year and week(date_trunc('week',g.date)) = d.week_of_year
where date in (select * from _week_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'monthly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
       when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
    else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_vip_1dc_click) as ui_vip_1dc,
    sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
    sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
    sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
    sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from reporting_media_prod.google_ads.search_optimization_dataset g
where date in (select * from _month_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    case when channel = 'shopping' and lower(campaign_name) ilike '%_brand_%' then 'branded shopping'
       when channel = 'shopping' and lower(campaign_name) ilike '%_nb_%' then 'non branded shopping'
    else channel end as channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(pixel_vip_1dc_click) as ui_vip_1dc,
    sum(pixel_vip_7x3x1_click) as ui_vip_7dc,
    sum(pixel_vip_7x3x1_click + pixel_vip_7x3x1_view) as ui_vip_7x3x1,
    sum(coalesce(pixel_vip_30dc_click, pixel_vip_30x7x1_click)) as ui_vip_30dc,
    sum(pixel_vip_30x7x1_click + pixel_vip_30x7x1_view) as ui_vip_30x7x1
from reporting_media_prod.google_ads.search_optimization_dataset g
where date in (select * from _year_pop_dates)
group by 1,2,3,4,5,6,7)

select
    date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    channel,
    row_number() over (partition by date_segment, store_brand_name, region, country, channel order by date desc) as latest_date_flag,
    impressions,
    spend / iff(impressions=0,null,impressions) as cpm,
    clicks,
    spend / iff(clicks=0,null,clicks) as cpc,
    spend / iff(ui_vip_1dc=0,null,ui_vip_1dc) as ui_cac_1dc,
    spend / iff(ui_vip_7dc=0,null,ui_vip_7dc) as ui_cac_7dc,
    spend / iff(ui_vip_7x3x1=0,null,ui_vip_7x3x1) as ui_cac_7x3x1,
    spend / iff(ui_vip_30dc=0,null,ui_vip_30dc) as ui_cac_30dc,
    spend / iff(ui_vip_30x7x1=0,null,ui_vip_30x7x1) as ui_cac_30x7x1
from _google_search_ads_raw;

create or replace temporary table _google_search_ads_ui as
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    '$' || to_varchar(t1.ui_cac_1dc,'999,999,999,999.00') as "UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t2.ui_cac_1dc)/iff(t2.ui_cac_1dc=0 or t2.ui_cac_1dc is null,1,t2.ui_cac_1dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t3.ui_cac_1dc)/iff(t3.ui_cac_1dc=0 or t3.ui_cac_1dc is null,1,t3.ui_cac_1dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t4.ui_cac_1dc)/iff(t4.ui_cac_1dc=0 or t4.ui_cac_1dc is null,1,t4.ui_cac_1dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 1DC",
    '$' || to_varchar(t1.ui_cac_7dc,'999,999,999,999.00') as "UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t2.ui_cac_7dc)/iff(t2.ui_cac_7dc=0 or t2.ui_cac_7dc is null,1,t2.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t3.ui_cac_7dc)/iff(t3.ui_cac_7dc=0 or t3.ui_cac_7dc is null,1,t3.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t4.ui_cac_7dc)/iff(t4.ui_cac_7dc=0 or t4.ui_cac_7dc is null,1,t4.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 7DC",
    '$' || to_varchar(t1.ui_cac_7x3x1,'999,999,999,999.00') as "UI CAC 7x3x1",
    to_varchar(((t1.ui_cac_7x3x1-t2.ui_cac_7x3x1)/iff(t2.ui_cac_7x3x1=0 or t2.ui_cac_7x3x1 is null,1,t2.ui_cac_7x3x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 7x3x1",
    to_varchar(((t1.ui_cac_7x3x1-t3.ui_cac_7x3x1)/iff(t3.ui_cac_7x3x1=0 or t3.ui_cac_7x3x1 is null,1,t3.ui_cac_7x3x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 7x3x1",
    to_varchar(((t1.ui_cac_7x3x1-t4.ui_cac_7x3x1)/iff(t4.ui_cac_7x3x1=0 or t4.ui_cac_7x3x1 is null,1,t4.ui_cac_7x3x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 7x3x1",
    '$' || to_varchar(t1.ui_cac_30dc,'999,999,999,999.00') as "UI CAC 30DC",
    to_varchar(((t1.ui_cac_30dc-t2.ui_cac_30dc)/iff(t2.ui_cac_30dc=0 or t2.ui_cac_30dc is null,1,t2.ui_cac_30dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 30DC",
    to_varchar(((t1.ui_cac_30dc-t3.ui_cac_30dc)/iff(t3.ui_cac_30dc=0 or t3.ui_cac_30dc is null,1,t3.ui_cac_30dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 30DC",
    to_varchar(((t1.ui_cac_30dc-t4.ui_cac_30dc)/iff(t4.ui_cac_30dc=0 or t4.ui_cac_30dc is null,1,t4.ui_cac_30dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 30DC",
    '$' || to_varchar(t1.ui_cac_30x7x1,'999,999,999,999.00') as "UI CAC 30x7x1",
    to_varchar(((t1.ui_cac_30x7x1-t2.ui_cac_30x7x1)/iff(t2.ui_cac_30x7x1=0 or t2.ui_cac_30x7x1 is null,1,t2.ui_cac_30x7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 30x7x1",
    to_varchar(((t1.ui_cac_30x7x1-t3.ui_cac_30x7x1)/iff(t3.ui_cac_30x7x1=0 or t3.ui_cac_30x7x1 is null,1,t3.ui_cac_30x7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 30x7x1",
    to_varchar(((t1.ui_cac_30x7x1-t4.ui_cac_30x7x1)/iff(t4.ui_cac_30x7x1=0 or t4.ui_cac_30x7x1 is null,1,t4.ui_cac_30x7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 30x7x1"
from _google_search_calcs t1
left join _google_search_calcs t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
                                    when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
                                    when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
                                    when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _google_search_calcs t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
                                    when t1.date_segment = 'weekly' then t1.date_pop = t3.date
                                    when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _google_search_calcs t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag > 1
union all
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    '$' || to_varchar(t1.ui_cac_1dc,'999,999,999,999.00') as "UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t2.ui_cac_1dc)/iff(t2.ui_cac_1dc=0 or t2.ui_cac_1dc is null,1,t2.ui_cac_1dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t3.ui_cac_1dc)/iff(t3.ui_cac_1dc=0 or t3.ui_cac_1dc is null,1,t3.ui_cac_1dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t4.ui_cac_1dc)/iff(t4.ui_cac_1dc=0 or t4.ui_cac_1dc is null,1,t4.ui_cac_1dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 1DC",
    '$' || to_varchar(t1.ui_cac_7dc,'999,999,999,999.00') as "UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t2.ui_cac_7dc)/iff(t2.ui_cac_7dc=0 or t2.ui_cac_7dc is null,1,t2.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t3.ui_cac_7dc)/iff(t3.ui_cac_7dc=0 or t3.ui_cac_7dc is null,1,t3.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t4.ui_cac_7dc)/iff(t4.ui_cac_7dc=0 or t4.ui_cac_7dc is null,1,t4.ui_cac_7dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 7DC",
    '$' || to_varchar(t1.ui_cac_7x3x1,'999,999,999,999.00') as "UI CAC 7x3x1",
    to_varchar(((t1.ui_cac_7x3x1-t2.ui_cac_7x3x1)/iff(t2.ui_cac_7x3x1=0 or t2.ui_cac_7x3x1 is null,1,t2.ui_cac_7x3x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 7x3x1",
    to_varchar(((t1.ui_cac_7x3x1-t3.ui_cac_7x3x1)/iff(t3.ui_cac_7x3x1=0 or t3.ui_cac_7x3x1 is null,1,t3.ui_cac_7x3x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 7x3x1",
    to_varchar(((t1.ui_cac_7x3x1-t4.ui_cac_7x3x1)/iff(t4.ui_cac_7x3x1=0 or t4.ui_cac_7x3x1 is null,1,t4.ui_cac_7x3x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 7x3x1",
    '$' || to_varchar(t1.ui_cac_30dc,'999,999,999,999.00') as "UI CAC 30DC",
    to_varchar(((t1.ui_cac_30dc-t2.ui_cac_30dc)/iff(t2.ui_cac_30dc=0 or t2.ui_cac_30dc is null,1,t2.ui_cac_30dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 30DC",
    to_varchar(((t1.ui_cac_30dc-t3.ui_cac_30dc)/iff(t3.ui_cac_30dc=0 or t3.ui_cac_30dc is null,1,t3.ui_cac_30dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 30DC",
    to_varchar(((t1.ui_cac_30dc-t4.ui_cac_30dc)/iff(t4.ui_cac_30dc=0 or t4.ui_cac_30dc is null,1,t4.ui_cac_30dc)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 30DC",
    '$' || to_varchar(t1.ui_cac_30x7x1,'999,999,999,999.00') as "UI CAC 30x7x1",
    to_varchar(((t1.ui_cac_30x7x1-t2.ui_cac_30x7x1)/iff(t2.ui_cac_30x7x1=0 or t2.ui_cac_30x7x1 is null,1,t2.ui_cac_30x7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CAC 30x7x1",
    to_varchar(((t1.ui_cac_30x7x1-t3.ui_cac_30x7x1)/iff(t3.ui_cac_30x7x1=0 or t3.ui_cac_30x7x1 is null,1,t3.ui_cac_30x7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CAC 30x7x1",
    to_varchar(((t1.ui_cac_30x7x1-t4.ui_cac_30x7x1)/iff(t4.ui_cac_30x7x1=0 or t4.ui_cac_30x7x1 is null,1,t4.ui_cac_30x7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CAC 30x7x1"
from _google_search_calcs_pop t1
left join _google_search_calcs_pop t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
                                    when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
                                    when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
                                    when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _google_search_calcs_pop t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
                                    when t1.date_segment = 'weekly' then t1.date_pop = t3.date
                                    when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _google_search_calcs_pop t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag = 1;

------------------------------------------------------------------------------------
-- other ui metrics

create or replace temporary table _ui_calcs as
with _other_ui_raw as (
select 'daily' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date,
    dateadd(year,-1,op.date) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv
from reporting_media_prod.dbo.all_channel_optimization op
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,op.date) = th.year
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('week',date) as date,
    date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv
from reporting_media_prod.dbo.all_channel_optimization op
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',op.date))) = d.calendar_year and week(date_trunc('week',op.date)) = d.week_of_year
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all select 'monthly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv

from reporting_media_prod.dbo.all_channel_optimization
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all select 'yearly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv

from reporting_media_prod.dbo.all_channel_optimization
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    store_brand_name,
    region,
    country,
    date,
    dateadd(year,-1,op.date) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv
from reporting_media_prod.dbo.all_channel_optimization op
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,op.date) = th.year
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('week',date) as date,
    date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv
from reporting_media_prod.dbo.all_channel_optimization op
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',op.date))) = d.calendar_year and week(date_trunc('week',op.date)) = d.week_of_year
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all select 'monthly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv

from reporting_media_prod.dbo.all_channel_optimization
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all select 'yearly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv

from reporting_media_prod.dbo.all_channel_optimization
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7)

select date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    row_number() over (partition by date_segment, store_brand_name, region, channel order by date desc) as latest_date_flag,
    channel,
    impressions,
    spend / iff(impressions=0,null,impressions) as cpm,
    clicks,
    spend / iff(clicks=0,null,clicks) as cpc,
    spend / iff(pixel_lead_1x1=0,null,pixel_lead_1x1) as ui_cpl_1x1,
    spend / iff(pixel_lead_7x1=0,null,pixel_lead_7x1) as ui_cpl_7x1,
    pixel_lead_1dc / iff(clicks=0,null,clicks) as click_to_lead_1d,
    pixel_lead_7dc / iff(clicks=0,null,clicks) as click_to_lead_7d,
    spend / iff(pixel_vip_1dc=0,null,pixel_vip_1dc) as ui_cac_1dc,
    spend / iff(pixel_vip_7dc=0,null,pixel_vip_7dc) as ui_cac_7dc,
    spend / iff(pixel_vip_1x1=0,null,pixel_vip_1x1) as ui_cac_1x1,
    spend / iff(pixel_vip_7x1=0,null,pixel_vip_7x1) as ui_cac_7x1,
    pixel_vip_1dc / iff(clicks=0,null,clicks) as click_to_vip_1d,
    pixel_vip_7dc / iff(clicks=0,null,clicks) as click_to_vip_7d,
    tatari_tv_spend / iff(tatari_tv_pixel_vip_incremental=0,null,tatari_tv_pixel_vip_incremental) as tatari_tv_ui_cac_incremental,
    tatari_streaming_spend / iff(tatari_streaming_pixel_vip_incremental=0,null,tatari_streaming_pixel_vip_incremental) as tatari_streaming_ui_cac_incremental,
    tatari_streaming_spend / iff(tatari_streaming_pixel_vip_1dv=0,null,tatari_streaming_pixel_vip_1dv) as tatari_streaming_ui_cac_1dv,
    tatari_streaming_spend / iff(tatari_streaming_pixel_vip_7dv=0,null,tatari_streaming_pixel_vip_7dv) as tatari_streaming_ui_cac_7dv,
    bpm_streaming_spend / iff(bpm_streaming_pixel_vip_3dv=0,null,bpm_streaming_pixel_vip_3dv) as bpm_streaming_ui_cac_3dv,
    bpm_streaming_spend / iff(bpm_streaming_pixel_vip_7dv=0,null,bpm_streaming_pixel_vip_7dv) as bpm_streaming_ui_cac_7dv
from _other_ui_raw;

create or replace temporary table _ui_calcs_pop as
with _other_ui_raw as (
select 'daily' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date,
    dateadd(year,-1,op.date) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv
from reporting_media_prod.dbo.all_channel_optimization op
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,op.date) = th.year
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('week',date) as date,
    date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv
from reporting_media_prod.dbo.all_channel_optimization op
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',op.date))) = d.calendar_year and week(date_trunc('week',op.date)) = d.week_of_year
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date in (select * from _week_pop_dates)
group by 1,2,3,4,5,6,7
union all select 'monthly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv

from reporting_media_prod.dbo.all_channel_optimization
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date in (select * from _month_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand_name,
    region,
    iff(lower(region)='na','US+CA',region) as country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv

from reporting_media_prod.dbo.all_channel_optimization
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date in (select * from _year_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'daily' as date_segment,
    store_brand_name,
    region,
    country,
    date,
    dateadd(year,-1,op.date) + delta_ly_comp_this_year as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv
from reporting_media_prod.dbo.all_channel_optimization op
join reporting_media_base_prod.dbo.static_thanksgiving_dates th on date_part(year,op.date) = th.year
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date >= $start_date and date < current_date()
group by 1,2,3,4,5,6,7
union all
select 'weekly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('week',date) as date,
    date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv
from reporting_media_prod.dbo.all_channel_optimization op
join (select distinct calendar_year, week_of_year, date_trunc('week',full_date) as date_pop from _dim_date) d
        on year(dateadd(year, -1, date_trunc('week',op.date))) = d.calendar_year and week(date_trunc('week',op.date)) = d.week_of_year
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date in (select * from _week_pop_dates)
group by 1,2,3,4,5,6,7
union all select 'monthly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('month',date) as date,
    dateadd(year,-1,date_trunc('month',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv

from reporting_media_prod.dbo.all_channel_optimization
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date in (select * from _month_pop_dates)
group by 1,2,3,4,5,6,7
union all
select 'yearly' as date_segment,
    store_brand_name,
    region,
    country,
    date_trunc('year',date) as date,
    dateadd(year,-1,date_trunc('year',date)) as date_pop,
    channel,
    sum(spend_usd) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    -- all channels outside of tv+streaming
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d, 0)) as pixel_lead_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_lead_click_7d, 0)) as pixel_lead_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_1d + pixel_lead_view_1d, 0)) as pixel_lead_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_lead_click_7d + pixel_lead_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_lead_7x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d, 0)) as pixel_vip_1dc,
    sum(iff(channel not in ('tv+streaming','twitter'), pixel_vip_click_7d, 0)) as pixel_vip_7dc,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_1d + pixel_vip_view_1d, 0)) as pixel_vip_1x1,
    sum(iff(channel not in ('tv+streaming','twitter','tiktok'), pixel_vip_click_7d + pixel_vip_view_1d, iff(channel in ('tiktok','twitter'),pixel_vip_7x1,0))) as pixel_vip_7x1,
    -- tatari
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', spend_usd, 0)) as tatari_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', spend_usd, 0)) as tatari_tv_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_click_1d, 0)) as tatari_streaming_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari tv', pixel_vip_click_1d, 0)) as tatari_tv_pixel_vip_incremental,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_1d, 0)) as tatari_streaming_pixel_vip_1dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'tatari streaming', pixel_vip_view_7d, 0)) as tatari_streaming_pixel_vip_7dv,
    -- bpm
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', spend_usd, 0)) as bpm_streaming_spend,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_3dv,
    sum(iff(channel = 'tv+streaming' and subchannel = 'bpm streaming', pixel_vip_view_3d, 0)) as bpm_streaming_pixel_vip_7dv

from reporting_media_prod.dbo.all_channel_optimization
where channel in ('tiktok','pinterest','snapchat','tv+streaming','twitter')
    and date in (select * from _year_pop_dates)
group by 1,2,3,4,5,6,7)

select date_segment,
    store_brand_name,
    region,
    country,
    date,
    date_pop,
    row_number() over (partition by date_segment, store_brand_name, region, channel order by date desc) as latest_date_flag,
    channel,
    impressions,
    spend / iff(impressions=0,null,impressions) as cpm,
    clicks,
    spend / iff(clicks=0,null,clicks) as cpc,
    spend / iff(pixel_lead_1x1=0,null,pixel_lead_1x1) as ui_cpl_1x1,
    spend / iff(pixel_lead_7x1=0,null,pixel_lead_7x1) as ui_cpl_7x1,
    pixel_lead_1dc / iff(clicks=0,null,clicks) as click_to_lead_1d,
    pixel_lead_7dc / iff(clicks=0,null,clicks) as click_to_lead_7d,
    spend / iff(pixel_vip_1dc=0,null,pixel_vip_1dc) as ui_cac_1dc,
    spend / iff(pixel_vip_7dc=0,null,pixel_vip_7dc) as ui_cac_7dc,
    spend / iff(pixel_vip_1x1=0,null,pixel_vip_1x1) as ui_cac_1x1,
    spend / iff(pixel_vip_7x1=0,null,pixel_vip_7x1) as ui_cac_7x1,
    pixel_vip_1dc / iff(clicks=0,null,clicks) as click_to_vip_1d,
    pixel_vip_7dc / iff(clicks=0,null,clicks) as click_to_vip_7d,
    tatari_tv_spend / iff(tatari_tv_pixel_vip_incremental=0,null,tatari_tv_pixel_vip_incremental) as tatari_tv_ui_cac_incremental,
    tatari_streaming_spend / iff(tatari_streaming_pixel_vip_incremental=0,null,tatari_streaming_pixel_vip_incremental) as tatari_streaming_ui_cac_incremental,
    tatari_streaming_spend / iff(tatari_streaming_pixel_vip_1dv=0,null,tatari_streaming_pixel_vip_1dv) as tatari_streaming_ui_cac_1dv,
    tatari_streaming_spend / iff(tatari_streaming_pixel_vip_7dv=0,null,tatari_streaming_pixel_vip_7dv) as tatari_streaming_ui_cac_7dv,
    bpm_streaming_spend / iff(bpm_streaming_pixel_vip_3dv=0,null,bpm_streaming_pixel_vip_3dv) as bpm_streaming_ui_cac_3dv,
    bpm_streaming_spend / iff(bpm_streaming_pixel_vip_7dv=0,null,bpm_streaming_pixel_vip_7dv) as bpm_streaming_ui_cac_7dv
from _other_ui_raw;

create or replace temporary table _other_ui as
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    '$' || to_varchar(t1.ui_cpl_1x1,'999,999,999,999.00') as "UI CPL 1x1",
    to_varchar(((t1.ui_cpl_1x1-t2.ui_cpl_1x1)/iff(t2.ui_cpl_1x1=0 or t2.ui_cpl_1x1 is null,1,t2.ui_cpl_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CPL 1x1",
    to_varchar(((t1.ui_cpl_1x1-t3.ui_cpl_1x1)/iff(t3.ui_cpl_1x1=0 or t3.ui_cpl_1x1 is null,1,t3.ui_cpl_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CPL 1x1",
    to_varchar(((t1.ui_cpl_1x1-t4.ui_cpl_1x1)/iff(t4.ui_cpl_1x1=0 or t4.ui_cpl_1x1 is null,1,t4.ui_cpl_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CPL 1x1",
    '$' || to_varchar(t1.ui_cpl_7x1,'999,999,999,999.00') as "UI CPL 7x1",
    to_varchar(((t1.ui_cpl_7x1-t2.ui_cpl_7x1)/iff(t2.ui_cpl_7x1=0 or t2.ui_cpl_7x1 is null,1,t2.ui_cpl_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CPL 7x1",
    to_varchar(((t1.ui_cpl_7x1-t3.ui_cpl_7x1)/iff(t3.ui_cpl_7x1=0 or t3.ui_cpl_7x1 is null,1,t3.ui_cpl_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CPL 7x1",
    to_varchar(((t1.ui_cpl_7x1-t4.ui_cpl_7x1)/iff(t4.ui_cpl_7x1=0 or t4.ui_cpl_7x1 is null,1,t4.ui_cpl_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CPL 7x1",
    to_varchar(t1.click_to_lead_1d * 100,'999,999,999,999.00') || '%' as "UI Click to Lead % 1D",
    to_varchar(((t1.click_to_lead_1d-t2.click_to_lead_1d)/iff(t2.click_to_lead_1d=0 or t2.click_to_lead_1d is null,1,t2.click_to_lead_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to Lead % 1D",
    to_varchar(((t1.click_to_lead_1d-t3.click_to_lead_1d)/iff(t3.click_to_lead_1d=0 or t3.click_to_lead_1d is null,1,t3.click_to_lead_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to Lead % 1D",
    to_varchar(((t1.click_to_lead_1d-t4.click_to_lead_1d)/iff(t4.click_to_lead_1d=0 or t4.click_to_lead_1d is null,1,t4.click_to_lead_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to Lead % 1D",
    to_varchar(t1.click_to_lead_7d * 100,'999,999,999,999.00') || '%' as "UI Click to Lead % 7D",
    to_varchar(((t1.click_to_lead_7d-t2.click_to_lead_7d)/iff(t2.click_to_lead_7d=0 or t2.click_to_lead_7d is null,1,t2.click_to_lead_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to Lead % 7D",
    to_varchar(((t1.click_to_lead_7d-t3.click_to_lead_7d)/iff(t3.click_to_lead_7d=0 or t3.click_to_lead_7d is null,1,t3.click_to_lead_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to Lead % 7D",
    to_varchar(((t1.click_to_lead_7d-t4.click_to_lead_7d)/iff(t4.click_to_lead_7d=0 or t4.click_to_lead_7d is null,1,t4.click_to_lead_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to Lead % 7D",
    '$' || to_varchar(t1.ui_cac_1dc,'999,999,999,999.00') as "UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t2.ui_cac_1dc)/iff(t2.ui_cac_1dc=0 or t2.ui_cac_1dc is null,1,t2.ui_cac_1dc)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t3.ui_cac_1dc)/iff(t3.ui_cac_1dc=0 or t3.ui_cac_1dc is null,1,t3.ui_cac_1dc)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t4.ui_cac_1dc)/iff(t4.ui_cac_1dc=0 or t4.ui_cac_1dc is null,1,t4.ui_cac_1dc)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day UI CAC 1DC",
    '$' || to_varchar(t1.ui_cac_7dc,'999,999,999,999.00') as "UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t2.ui_cac_7dc)/iff(t2.ui_cac_7dc=0 or t2.ui_cac_7dc is null,1,t2.ui_cac_7dc)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t3.ui_cac_7dc)/iff(t3.ui_cac_7dc=0 or t3.ui_cac_7dc is null,1,t3.ui_cac_7dc)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t4.ui_cac_7dc)/iff(t4.ui_cac_7dc=0 or t4.ui_cac_7dc is null,1,t4.ui_cac_7dc)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day UI CAC 7DC",
    '$' || to_varchar(t1.ui_cac_1x1,'999,999,999,999.00') as "UI CAC 1x1",
    to_varchar(((t1.ui_cac_1x1-t2.ui_cac_1x1)/iff(t2.ui_cac_1x1=0 or t2.ui_cac_1x1 is null,1,t2.ui_cac_1x1)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP UI CAC 1x1",
    to_varchar(((t1.ui_cac_1x1-t3.ui_cac_1x1)/iff(t3.ui_cac_1x1=0 or t3.ui_cac_1x1 is null,1,t3.ui_cac_1x1)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date UI CAC 1x1",
    to_varchar(((t1.ui_cac_1x1-t4.ui_cac_1x1)/iff(t4.ui_cac_1x1=0 or t4.ui_cac_1x1 is null,1,t4.ui_cac_1x1)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day UI CAC 1x1",
    '$' || to_varchar(t1.ui_cac_7x1,'999,999,999,999.00') as "UI CAC 7x1",
    to_varchar(((t1.ui_cac_7x1-t2.ui_cac_7x1)/iff(t2.ui_cac_7x1=0 or t2.ui_cac_7x1 is null,1,t2.ui_cac_7x1)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP UI CAC 7x1",
    to_varchar(((t1.ui_cac_7x1-t3.ui_cac_7x1)/iff(t3.ui_cac_7x1=0 or t3.ui_cac_7x1 is null,1,t3.ui_cac_7x1)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date UI CAC 7x1",
    to_varchar(((t1.ui_cac_7x1-t4.ui_cac_7x1)/iff(t4.ui_cac_7x1=0 or t4.ui_cac_7x1 is null,1,t4.ui_cac_7x1)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day UI CAC 7x1",
    to_varchar(t1.click_to_vip_1d * 100,'999,999,999,999.00') || '%' as "UI Click to VIP % 1D",
    to_varchar(((t1.click_to_vip_1d-t2.click_to_vip_1d)/iff(t2.click_to_vip_1d=0 or t2.click_to_vip_1d is null,1,t2.click_to_vip_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to VIP % 1D",
    to_varchar(((t1.click_to_vip_1d-t3.click_to_vip_1d)/iff(t3.click_to_vip_1d=0 or t3.click_to_vip_1d is null,1,t3.click_to_vip_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to VIP % 1D",
    to_varchar(((t1.click_to_vip_1d-t4.click_to_vip_1d)/iff(t4.click_to_vip_1d=0 or t4.click_to_vip_1d is null,1,t4.click_to_vip_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to VIP % 1D",
    to_varchar(t1.click_to_vip_7d * 100,'999,999,999,999.00') || '%' as "UI Click to VIP % 7D",
    to_varchar(((t1.click_to_vip_7d-t2.click_to_vip_7d)/iff(t2.click_to_vip_7d=0 or t2.click_to_vip_7d is null,1,t2.click_to_vip_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to VIP % 7D",
    to_varchar(((t1.click_to_vip_7d-t3.click_to_vip_7d)/iff(t3.click_to_vip_7d=0 or t3.click_to_vip_7d is null,1,t3.click_to_vip_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to VIP % 7D",
    to_varchar(((t1.click_to_vip_7d-t4.click_to_vip_7d)/iff(t4.click_to_vip_7d=0 or t4.click_to_vip_7d is null,1,t4.click_to_vip_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to VIP % 7D",
    '$' || to_varchar(t1.tatari_tv_ui_cac_incremental,'999,999,999,999.00') as "Tatari TV UI CAC Incremental",
    to_varchar(((t1.tatari_tv_ui_cac_incremental-t2.tatari_tv_ui_cac_incremental)/iff(t2.tatari_tv_ui_cac_incremental=0 or t2.tatari_tv_ui_cac_incremental is null,1,t2.tatari_tv_ui_cac_incremental)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP Tatari TV UI CAC Incremental",
    to_varchar(((t1.tatari_tv_ui_cac_incremental-t3.tatari_tv_ui_cac_incremental)/iff(t3.tatari_tv_ui_cac_incremental=0 or t3.tatari_tv_ui_cac_incremental is null,1,t3.tatari_tv_ui_cac_incremental)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date Tatari TV UI CAC Incremental",
    to_varchar(((t1.tatari_tv_ui_cac_incremental-t4.tatari_tv_ui_cac_incremental)/iff(t4.tatari_tv_ui_cac_incremental=0 or t4.tatari_tv_ui_cac_incremental is null,1,t4.tatari_tv_ui_cac_incremental)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day Tatari TV UI CAC Incremental",
    '$' || to_varchar(t1.tatari_streaming_ui_cac_incremental,'999,999,999,999.00') as "Tatari Streaming UI CAC Incremental",
    to_varchar(((t1.tatari_streaming_ui_cac_incremental-t2.tatari_streaming_ui_cac_incremental)/iff(t2.tatari_streaming_ui_cac_incremental=0 or t2.tatari_streaming_ui_cac_incremental is null,1,t2.tatari_streaming_ui_cac_incremental)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP Tatari Streaming UI CAC Incremental",
    to_varchar(((t1.tatari_streaming_ui_cac_incremental-t3.tatari_streaming_ui_cac_incremental)/iff(t3.tatari_streaming_ui_cac_incremental=0 or t3.tatari_streaming_ui_cac_incremental is null,1,t3.tatari_streaming_ui_cac_incremental)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date Tatari Streaming UI CAC Incremental",
    to_varchar(((t1.tatari_streaming_ui_cac_incremental-t4.tatari_streaming_ui_cac_incremental)/iff(t4.tatari_streaming_ui_cac_incremental=0 or t4.tatari_streaming_ui_cac_incremental is null,1,t4.tatari_streaming_ui_cac_incremental)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day Tatari Streaming UI CAC Incremental",
    '$' || to_varchar(t1.tatari_streaming_ui_cac_1dv,'999,999,999,999.00') as "Tatari Streaming UI CAC 1DV",
    to_varchar(((t1.tatari_streaming_ui_cac_1dv-t2.tatari_streaming_ui_cac_1dv)/iff(t2.tatari_streaming_ui_cac_1dv=0 or t2.tatari_streaming_ui_cac_1dv is null,1,t2.tatari_streaming_ui_cac_1dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP Tatari Streaming UI CAC 1DV",
    to_varchar(((t1.tatari_streaming_ui_cac_1dv-t3.tatari_streaming_ui_cac_1dv)/iff(t3.tatari_streaming_ui_cac_1dv=0 or t3.tatari_streaming_ui_cac_1dv is null,1,t3.tatari_streaming_ui_cac_1dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date Tatari Streaming UI CAC 1DV",
    to_varchar(((t1.tatari_streaming_ui_cac_1dv-t4.tatari_streaming_ui_cac_1dv)/iff(t4.tatari_streaming_ui_cac_1dv=0 or t4.tatari_streaming_ui_cac_1dv is null,1,t4.tatari_streaming_ui_cac_1dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day Tatari Streaming UI CAC 1DV",
    '$' || to_varchar(t1.tatari_streaming_ui_cac_7dv,'999,999,999,999.00') as "Tatari Streaming UI CAC 7DV",
    to_varchar(((t1.tatari_streaming_ui_cac_7dv-t2.tatari_streaming_ui_cac_7dv)/iff(t2.tatari_streaming_ui_cac_7dv=0 or t2.tatari_streaming_ui_cac_7dv is null,1,t2.tatari_streaming_ui_cac_7dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP Tatari Streaming UI CAC 7DV",
    to_varchar(((t1.tatari_streaming_ui_cac_7dv-t3.tatari_streaming_ui_cac_7dv)/iff(t3.tatari_streaming_ui_cac_7dv=0 or t3.tatari_streaming_ui_cac_7dv is null,1,t3.tatari_streaming_ui_cac_7dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date Tatari Streaming UI CAC 7DV",
    to_varchar(((t1.tatari_streaming_ui_cac_7dv-t4.tatari_streaming_ui_cac_7dv)/iff(t4.tatari_streaming_ui_cac_7dv=0 or t4.tatari_streaming_ui_cac_7dv is null,1,t4.tatari_streaming_ui_cac_7dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day Tatari Streaming UI CAC 7DV",
    '$' || to_varchar(t1.bpm_streaming_ui_cac_3dv,'999,999,999,999.00') as "BPM Streaming UI CAC 3DV",
    to_varchar(((t1.bpm_streaming_ui_cac_3dv-t2.bpm_streaming_ui_cac_3dv)/iff(t2.bpm_streaming_ui_cac_3dv=0 or t2.bpm_streaming_ui_cac_3dv is null,1,t2.bpm_streaming_ui_cac_3dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP BPM Streaming UI CAC 3DV",
    to_varchar(((t1.bpm_streaming_ui_cac_3dv-t3.bpm_streaming_ui_cac_3dv)/iff(t3.bpm_streaming_ui_cac_3dv=0 or t3.bpm_streaming_ui_cac_3dv is null,1,t3.bpm_streaming_ui_cac_3dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date BPM Streaming UI CAC 3DV",
    to_varchar(((t1.bpm_streaming_ui_cac_3dv-t4.bpm_streaming_ui_cac_3dv)/iff(t4.bpm_streaming_ui_cac_3dv=0 or t4.bpm_streaming_ui_cac_3dv is null,1,t4.bpm_streaming_ui_cac_3dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day BPM Streaming UI CAC 3DV",
    '$' || to_varchar(t1.bpm_streaming_ui_cac_7dv,'999,999,999,999.00') as "BPM Streaming UI CAC 7DV",
    to_varchar(((t1.bpm_streaming_ui_cac_7dv-t2.bpm_streaming_ui_cac_7dv)/iff(t2.bpm_streaming_ui_cac_7dv=0 or t2.bpm_streaming_ui_cac_7dv is null,1,t2.bpm_streaming_ui_cac_7dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP BPM Streaming UI CAC 7DV",
    to_varchar(((t1.bpm_streaming_ui_cac_7dv-t3.bpm_streaming_ui_cac_7dv)/iff(t3.bpm_streaming_ui_cac_7dv=0 or t3.bpm_streaming_ui_cac_7dv is null,1,t3.bpm_streaming_ui_cac_7dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date BPM Streaming UI CAC 7DV",
    to_varchar(((t1.bpm_streaming_ui_cac_7dv-t4.bpm_streaming_ui_cac_7dv)/iff(t4.bpm_streaming_ui_cac_7dv=0 or t4.bpm_streaming_ui_cac_7dv is null,1,t4.bpm_streaming_ui_cac_7dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day BPM Streaming UI CAC 7DV"
from _ui_calcs t1
left join _ui_calcs t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
                                    when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
                                    when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
                                    when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _ui_calcs t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
                                    when t1.date_segment = 'weekly' then t1.date_pop = t3.date
                                    when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _ui_calcs t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag > 1
union all
select
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    '$' || to_varchar(t1.ui_cpl_1x1,'999,999,999,999.00') as "UI CPL 1x1",
    to_varchar(((t1.ui_cpl_1x1-t2.ui_cpl_1x1)/iff(t2.ui_cpl_1x1=0 or t2.ui_cpl_1x1 is null,1,t2.ui_cpl_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CPL 1x1",
    to_varchar(((t1.ui_cpl_1x1-t3.ui_cpl_1x1)/iff(t3.ui_cpl_1x1=0 or t3.ui_cpl_1x1 is null,1,t3.ui_cpl_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CPL 1x1",
    to_varchar(((t1.ui_cpl_1x1-t4.ui_cpl_1x1)/iff(t4.ui_cpl_1x1=0 or t4.ui_cpl_1x1 is null,1,t4.ui_cpl_1x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CPL 1x1",
    '$' || to_varchar(t1.ui_cpl_7x1,'999,999,999,999.00') as "UI CPL 7x1",
    to_varchar(((t1.ui_cpl_7x1-t2.ui_cpl_7x1)/iff(t2.ui_cpl_7x1=0 or t2.ui_cpl_7x1 is null,1,t2.ui_cpl_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI CPL 7x1",
    to_varchar(((t1.ui_cpl_7x1-t3.ui_cpl_7x1)/iff(t3.ui_cpl_7x1=0 or t3.ui_cpl_7x1 is null,1,t3.ui_cpl_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI CPL 7x1",
    to_varchar(((t1.ui_cpl_7x1-t4.ui_cpl_7x1)/iff(t4.ui_cpl_7x1=0 or t4.ui_cpl_7x1 is null,1,t4.ui_cpl_7x1)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI CPL 7x1",
    to_varchar(t1.click_to_lead_1d * 100,'999,999,999,999.00') || '%' as "UI Click to Lead % 1D",
    to_varchar(((t1.click_to_lead_1d-t2.click_to_lead_1d)/iff(t2.click_to_lead_1d=0 or t2.click_to_lead_1d is null,1,t2.click_to_lead_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to Lead % 1D",
    to_varchar(((t1.click_to_lead_1d-t3.click_to_lead_1d)/iff(t3.click_to_lead_1d=0 or t3.click_to_lead_1d is null,1,t3.click_to_lead_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to Lead % 1D",
    to_varchar(((t1.click_to_lead_1d-t4.click_to_lead_1d)/iff(t4.click_to_lead_1d=0 or t4.click_to_lead_1d is null,1,t4.click_to_lead_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to Lead % 1D",
    to_varchar(t1.click_to_lead_7d * 100,'999,999,999,999.00') || '%' as "UI Click to Lead % 7D",
    to_varchar(((t1.click_to_lead_7d-t2.click_to_lead_7d)/iff(t2.click_to_lead_7d=0 or t2.click_to_lead_7d is null,1,t2.click_to_lead_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to Lead % 7D",
    to_varchar(((t1.click_to_lead_7d-t3.click_to_lead_7d)/iff(t3.click_to_lead_7d=0 or t3.click_to_lead_7d is null,1,t3.click_to_lead_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to Lead % 7D",
    to_varchar(((t1.click_to_lead_7d-t4.click_to_lead_7d)/iff(t4.click_to_lead_7d=0 or t4.click_to_lead_7d is null,1,t4.click_to_lead_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to Lead % 7D",
    '$' || to_varchar(t1.ui_cac_1dc,'999,999,999,999.00') as "UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t2.ui_cac_1dc)/iff(t2.ui_cac_1dc=0 or t2.ui_cac_1dc is null,1,t2.ui_cac_1dc)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t3.ui_cac_1dc)/iff(t3.ui_cac_1dc=0 or t3.ui_cac_1dc is null,1,t3.ui_cac_1dc)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date UI CAC 1DC",
    to_varchar(((t1.ui_cac_1dc-t4.ui_cac_1dc)/iff(t4.ui_cac_1dc=0 or t4.ui_cac_1dc is null,1,t4.ui_cac_1dc)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day UI CAC 1DC",
    '$' || to_varchar(t1.ui_cac_7dc,'999,999,999,999.00') as "UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t2.ui_cac_7dc)/iff(t2.ui_cac_7dc=0 or t2.ui_cac_7dc is null,1,t2.ui_cac_7dc)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t3.ui_cac_7dc)/iff(t3.ui_cac_7dc=0 or t3.ui_cac_7dc is null,1,t3.ui_cac_7dc)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date UI CAC 7DC",
    to_varchar(((t1.ui_cac_7dc-t4.ui_cac_7dc)/iff(t4.ui_cac_7dc=0 or t4.ui_cac_7dc is null,1,t4.ui_cac_7dc)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day UI CAC 7DC",
    '$' || to_varchar(t1.ui_cac_1x1,'999,999,999,999.00') as "UI CAC 1x1",
    to_varchar(((t1.ui_cac_1x1-t2.ui_cac_1x1)/iff(t2.ui_cac_1x1=0 or t2.ui_cac_1x1 is null,1,t2.ui_cac_1x1)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP UI CAC 1x1",
    to_varchar(((t1.ui_cac_1x1-t3.ui_cac_1x1)/iff(t3.ui_cac_1x1=0 or t3.ui_cac_1x1 is null,1,t3.ui_cac_1x1)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date UI CAC 1x1",
    to_varchar(((t1.ui_cac_1x1-t4.ui_cac_1x1)/iff(t4.ui_cac_1x1=0 or t4.ui_cac_1x1 is null,1,t4.ui_cac_1x1)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day UI CAC 1x1",
    '$' || to_varchar(t1.ui_cac_7x1,'999,999,999,999.00') as "UI CAC 7x1",
    to_varchar(((t1.ui_cac_7x1-t2.ui_cac_7x1)/iff(t2.ui_cac_7x1=0 or t2.ui_cac_7x1 is null,1,t2.ui_cac_7x1)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP UI CAC 7x1",
    to_varchar(((t1.ui_cac_7x1-t3.ui_cac_7x1)/iff(t3.ui_cac_7x1=0 or t3.ui_cac_7x1 is null,1,t3.ui_cac_7x1)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date UI CAC 7x1",
    to_varchar(((t1.ui_cac_7x1-t4.ui_cac_7x1)/iff(t4.ui_cac_7x1=0 or t4.ui_cac_7x1 is null,1,t4.ui_cac_7x1)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day UI CAC 7x1",
    to_varchar(t1.click_to_vip_1d * 100,'999,999,999,999.00') || '%' as "UI Click to VIP % 1D",
    to_varchar(((t1.click_to_vip_1d-t2.click_to_vip_1d)/iff(t2.click_to_vip_1d=0 or t2.click_to_vip_1d is null,1,t2.click_to_vip_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to VIP % 1D",
    to_varchar(((t1.click_to_vip_1d-t3.click_to_vip_1d)/iff(t3.click_to_vip_1d=0 or t3.click_to_vip_1d is null,1,t3.click_to_vip_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to VIP % 1D",
    to_varchar(((t1.click_to_vip_1d-t4.click_to_vip_1d)/iff(t4.click_to_vip_1d=0 or t4.click_to_vip_1d is null,1,t4.click_to_vip_1d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to VIP % 1D",
    to_varchar(t1.click_to_vip_7d * 100,'999,999,999,999.00') || '%' as "UI Click to VIP % 7D",
    to_varchar(((t1.click_to_vip_7d-t2.click_to_vip_7d)/iff(t2.click_to_vip_7d=0 or t2.click_to_vip_7d is null,1,t2.click_to_vip_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP UI Click to VIP % 7D",
    to_varchar(((t1.click_to_vip_7d-t3.click_to_vip_7d)/iff(t3.click_to_vip_7d=0 or t3.click_to_vip_7d is null,1,t3.click_to_vip_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date UI Click to VIP % 7D",
    to_varchar(((t1.click_to_vip_7d-t4.click_to_vip_7d)/iff(t4.click_to_vip_7d=0 or t4.click_to_vip_7d is null,1,t4.click_to_vip_7d)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day UI Click to VIP % 7D",
    '$' || to_varchar(t1.tatari_tv_ui_cac_incremental,'999,999,999,999.00') as "Tatari TV UI CAC Incremental",
    to_varchar(((t1.tatari_tv_ui_cac_incremental-t2.tatari_tv_ui_cac_incremental)/iff(t2.tatari_tv_ui_cac_incremental=0 or t2.tatari_tv_ui_cac_incremental is null,1,t2.tatari_tv_ui_cac_incremental)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP Tatari TV UI CAC Incremental",
    to_varchar(((t1.tatari_tv_ui_cac_incremental-t3.tatari_tv_ui_cac_incremental)/iff(t3.tatari_tv_ui_cac_incremental=0 or t3.tatari_tv_ui_cac_incremental is null,1,t3.tatari_tv_ui_cac_incremental)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date Tatari TV UI CAC Incremental",
    to_varchar(((t1.tatari_tv_ui_cac_incremental-t4.tatari_tv_ui_cac_incremental)/iff(t4.tatari_tv_ui_cac_incremental=0 or t4.tatari_tv_ui_cac_incremental is null,1,t4.tatari_tv_ui_cac_incremental)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day Tatari TV UI CAC Incremental",
    '$' || to_varchar(t1.tatari_streaming_ui_cac_incremental,'999,999,999,999.00') as "Tatari Streaming UI CAC Incremental",
    to_varchar(((t1.tatari_streaming_ui_cac_incremental-t2.tatari_streaming_ui_cac_incremental)/iff(t2.tatari_streaming_ui_cac_incremental=0 or t2.tatari_streaming_ui_cac_incremental is null,1,t2.tatari_streaming_ui_cac_incremental)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP Tatari Streaming UI CAC Incremental",
    to_varchar(((t1.tatari_streaming_ui_cac_incremental-t3.tatari_streaming_ui_cac_incremental)/iff(t3.tatari_streaming_ui_cac_incremental=0 or t3.tatari_streaming_ui_cac_incremental is null,1,t3.tatari_streaming_ui_cac_incremental)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date Tatari Streaming UI CAC Incremental",
    to_varchar(((t1.tatari_streaming_ui_cac_incremental-t4.tatari_streaming_ui_cac_incremental)/iff(t4.tatari_streaming_ui_cac_incremental=0 or t4.tatari_streaming_ui_cac_incremental is null,1,t4.tatari_streaming_ui_cac_incremental)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day Tatari Streaming UI CAC Incremental",
    '$' || to_varchar(t1.tatari_streaming_ui_cac_1dv,'999,999,999,999.00') as "Tatari Streaming UI CAC 1DV",
    to_varchar(((t1.tatari_streaming_ui_cac_1dv-t2.tatari_streaming_ui_cac_1dv)/iff(t2.tatari_streaming_ui_cac_1dv=0 or t2.tatari_streaming_ui_cac_1dv is null,1,t2.tatari_streaming_ui_cac_1dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP Tatari Streaming UI CAC 1DV",
    to_varchar(((t1.tatari_streaming_ui_cac_1dv-t3.tatari_streaming_ui_cac_1dv)/iff(t3.tatari_streaming_ui_cac_1dv=0 or t3.tatari_streaming_ui_cac_1dv is null,1,t3.tatari_streaming_ui_cac_1dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date Tatari Streaming UI CAC 1DV",
    to_varchar(((t1.tatari_streaming_ui_cac_1dv-t4.tatari_streaming_ui_cac_1dv)/iff(t4.tatari_streaming_ui_cac_1dv=0 or t4.tatari_streaming_ui_cac_1dv is null,1,t4.tatari_streaming_ui_cac_1dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day Tatari Streaming UI CAC 1DV",
    '$' || to_varchar(t1.tatari_streaming_ui_cac_7dv,'999,999,999,999.00') as "Tatari Streaming UI CAC 7DV",
    to_varchar(((t1.tatari_streaming_ui_cac_7dv-t2.tatari_streaming_ui_cac_7dv)/iff(t2.tatari_streaming_ui_cac_7dv=0 or t2.tatari_streaming_ui_cac_7dv is null,1,t2.tatari_streaming_ui_cac_7dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP Tatari Streaming UI CAC 7DV",
    to_varchar(((t1.tatari_streaming_ui_cac_7dv-t3.tatari_streaming_ui_cac_7dv)/iff(t3.tatari_streaming_ui_cac_7dv=0 or t3.tatari_streaming_ui_cac_7dv is null,1,t3.tatari_streaming_ui_cac_7dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date Tatari Streaming UI CAC 7DV",
    to_varchar(((t1.tatari_streaming_ui_cac_7dv-t4.tatari_streaming_ui_cac_7dv)/iff(t4.tatari_streaming_ui_cac_7dv=0 or t4.tatari_streaming_ui_cac_7dv is null,1,t4.tatari_streaming_ui_cac_7dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day Tatari Streaming UI CAC 7DV",
    '$' || to_varchar(t1.bpm_streaming_ui_cac_3dv,'999,999,999,999.00') as "BPM Streaming UI CAC 3DV",
    to_varchar(((t1.bpm_streaming_ui_cac_3dv-t2.bpm_streaming_ui_cac_3dv)/iff(t2.bpm_streaming_ui_cac_3dv=0 or t2.bpm_streaming_ui_cac_3dv is null,1,t2.bpm_streaming_ui_cac_3dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP BPM Streaming UI CAC 3DV",
    to_varchar(((t1.bpm_streaming_ui_cac_3dv-t3.bpm_streaming_ui_cac_3dv)/iff(t3.bpm_streaming_ui_cac_3dv=0 or t3.bpm_streaming_ui_cac_3dv is null,1,t3.bpm_streaming_ui_cac_3dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date BPM Streaming UI CAC 3DV",
    to_varchar(((t1.bpm_streaming_ui_cac_3dv-t4.bpm_streaming_ui_cac_3dv)/iff(t4.bpm_streaming_ui_cac_3dv=0 or t4.bpm_streaming_ui_cac_3dv is null,1,t4.bpm_streaming_ui_cac_3dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day BPM Streaming UI CAC 3DV",
    '$' || to_varchar(t1.bpm_streaming_ui_cac_7dv,'999,999,999,999.00') as "BPM Streaming UI CAC 7DV",
    to_varchar(((t1.bpm_streaming_ui_cac_7dv-t2.bpm_streaming_ui_cac_7dv)/iff(t2.bpm_streaming_ui_cac_7dv=0 or t2.bpm_streaming_ui_cac_7dv is null,1,t2.bpm_streaming_ui_cac_7dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ PoP BPM Streaming UI CAC 7DV",
    to_varchar(((t1.bpm_streaming_ui_cac_7dv-t3.bpm_streaming_ui_cac_7dv)/iff(t3.bpm_streaming_ui_cac_7dv=0 or t3.bpm_streaming_ui_cac_7dv is null,1,t3.bpm_streaming_ui_cac_7dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Date BPM Streaming UI CAC 7DV",
    to_varchar(((t1.bpm_streaming_ui_cac_7dv-t4.bpm_streaming_ui_cac_7dv)/iff(t4.bpm_streaming_ui_cac_7dv=0 or t4.bpm_streaming_ui_cac_7dv is null,1,t4.bpm_streaming_ui_cac_7dv)) * 100, '999,999,999,999.00') || '%' as "% ∆ YoY, Same Day BPM Streaming UI CAC 7DV"
from _ui_calcs_pop t1
left join _ui_calcs_pop t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.channel = t2.channel and t1.region = t2.region and t1.country = t2.country
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
                                    when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
                                    when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
                                    when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _ui_calcs_pop t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.channel = t3.channel and t1.region = t3.region and t1.country = t3.country
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
                                    when t1.date_segment = 'weekly' then t1.date_pop = t3.date
                                    when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _ui_calcs_pop t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.channel = t4.channel and t1.region = t4.region and t1.country = t4.country
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag = 1;

------------------------------------------------------------------------------------
-- additional calculations

create or replace temporary table _additional_calcs_raw as
with _scaffold as (
select distinct date_segment, store_brand_name, region, country, date, date_pop, channel, latest_date_flag from _sessions_raw
union
select distinct date_segment, store_brand_name, region, country, date, date_pop, channel, latest_date_flag from _media_mix_final where  channel not in ('non branded shopping', 'branded shopping')
)
select
    c.*,
    spend as media_spend,
    impressions,
    total_sessions,
    spend / iff(total_sessions = 0, null, total_sessions) as cps,
    total_sessions / iff((impressions/1000) = 0, null, (impressions/1000)) as imp_to_session_rate,
    spend / iff(same_session_vips=0,null,same_session_vips) as same_session_vip_cac,
    spend / iff(session_vips_from_leads_24h=0,null,session_vips_from_leads_24h) as session_vips_from_leads_24h_cac
from _scaffold c
left join _media_mix_final mmm on c.date_segment = mmm.date_segment and c.store_brand_name = mmm.store_brand_name and c.region = mmm.region and c.date = mmm.date and c.channel = mmm.channel and c.country = mmm.country
left join _sessions_raw s on c.date_segment = s.date_segment and c.store_brand_name = s.store_brand_name and c.region = s.region and c.date = s.date and c.channel = s.channel and c.country = s.country;

create or replace temporary table _additional_calcs_raw_pop as
with _scaffold_pop as (
select distinct date_segment, store_brand_name, region, country, date, date_pop, channel, latest_date_flag from _sessions_raw_pop
union
select distinct date_segment, store_brand_name, region, country, date, date_pop, channel, latest_date_flag from _media_mix_final_pop where channel not in ('non branded shopping', 'branded shopping')
)
select
    c.*,
    spend as media_spend,
    impressions,
    total_sessions,
    spend / iff(total_sessions = 0, null, total_sessions) as cps,
    total_sessions / iff((impressions/1000) = 0, null, (impressions/1000)) as imp_to_session_rate,
    spend / iff(same_session_vips=0,null,same_session_vips) as same_session_vip_cac,
    spend / iff(session_vips_from_leads_24h=0,null,session_vips_from_leads_24h) as session_vips_from_leads_24h_cac
from _scaffold_pop c
left join _media_mix_final_pop mmm on c.date_segment = mmm.date_segment and c.store_brand_name = mmm.store_brand_name and c.region = mmm.region and c.date = mmm.date and c.channel = mmm.channel and c.country = mmm.country
left join _sessions_raw_pop s on c.date_segment = s.date_segment and c.store_brand_name = s.store_brand_name and c.region = s.region and c.date = s.date and c.channel = s.channel and c.country = s.country;

create or replace temporary table _additional_calcs as
select distinct
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    to_varchar(t1.imp_to_session_rate, '999,999,999,999.00') || '%' as "Impression to Session %",
    to_varchar(((t1.imp_to_session_rate-t2.imp_to_session_rate)/iff(t2.imp_to_session_rate=0 or t2.imp_to_session_rate is null,1,t2.imp_to_session_rate)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Impression to Session %",
    to_varchar(((t1.imp_to_session_rate-t3.imp_to_session_rate)/iff(t3.imp_to_session_rate=0 or t3.imp_to_session_rate is null,1,t3.imp_to_session_rate)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Impression to Session %",
    to_varchar(((t1.imp_to_session_rate-t4.imp_to_session_rate)/iff(t4.imp_to_session_rate=0 or t4.imp_to_session_rate is null,1,t4.imp_to_session_rate)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Impression to Session %",
    '$' || to_varchar(t1.cps, '999,999,999,999.00') as "CPS",
    to_varchar(((t1.cps-t2.cps)/iff(t2.cps=0 or t2.cps is null,1,t2.cps)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP CPS",
    to_varchar(((t1.cps-t3.cps)/iff(t3.cps=0 or t3.cps is null,1,t3.cps)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date CPS",
    to_varchar(((t1.cps-t4.cps)/iff(t4.cps=0 or t4.cps is null,1,t4.cps)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day CPS",
    '$' || to_varchar(t1.same_session_vip_cac, '999,999,999,999.00') as "Same Session VIP CAC",
    to_varchar(((t1.same_session_vip_cac-t2.same_session_vip_cac)/iff(t2.same_session_vip_cac=0 or t2.same_session_vip_cac is null,1,t2.same_session_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Same Session VIP CAC",
    to_varchar(((t1.same_session_vip_cac-t3.same_session_vip_cac)/iff(t3.same_session_vip_cac=0 or t3.same_session_vip_cac is null,1,t3.same_session_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Same Session VIP CAC",
    to_varchar(((t1.same_session_vip_cac-t4.same_session_vip_cac)/iff(t4.same_session_vip_cac=0 or t4.same_session_vip_cac is null,1,t4.same_session_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Same Session VIP CAC",
    '$' || to_varchar(t1.session_vips_from_leads_24h_cac, '999,999,999,999.00') as "Session VIPs from Leads 24H CAC",
    to_varchar(((t1.session_vips_from_leads_24h_cac-t2.session_vips_from_leads_24h_cac)/iff(t2.session_vips_from_leads_24h_cac=0 or t2.session_vips_from_leads_24h_cac is null,1,t2.session_vips_from_leads_24h_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Session VIPs from Leads 24H CAC",
    to_varchar(((t1.session_vips_from_leads_24h_cac-t3.session_vips_from_leads_24h_cac)/iff(t3.session_vips_from_leads_24h_cac=0 or t3.session_vips_from_leads_24h_cac is null,1,t3.session_vips_from_leads_24h_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Session VIPs from Leads 24H CAC",
    to_varchar(((t1.session_vips_from_leads_24h_cac-t4.session_vips_from_leads_24h_cac)/iff(t4.session_vips_from_leads_24h_cac=0 or t4.session_vips_from_leads_24h_cac is null,1,t4.session_vips_from_leads_24h_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Session VIPs from Leads 24H CAC"
from _additional_calcs_raw t1
left join _additional_calcs_raw t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.region = t2.region and t1.country = t2.country and t1.channel = t2.channel
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
          when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
          when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
          when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _additional_calcs_raw t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.region = t3.region and t1.country = t3.country and t1.channel = t3.channel
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
          when t1.date_segment = 'weekly' then t1.date_pop = t3.date
          when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _additional_calcs_raw t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.region = t4.region and t1.country = t4.country and t1.channel = t4.channel
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag > 1
union all
select distinct
    t1.date_segment,
    t1.store_brand_name,
    t1.region,
    t1.country,
    t1.date,
    t1.channel,
    to_varchar(t1.imp_to_session_rate, '999,999,999,999.00') || '%' as "Impression to Session %",
    to_varchar(((t1.imp_to_session_rate-t2.imp_to_session_rate)/iff(t2.imp_to_session_rate=0 or t2.imp_to_session_rate is null,1,t2.imp_to_session_rate)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Impression to Session %",
    to_varchar(((t1.imp_to_session_rate-t3.imp_to_session_rate)/iff(t3.imp_to_session_rate=0 or t3.imp_to_session_rate is null,1,t3.imp_to_session_rate)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Impression to Session %",
    to_varchar(((t1.imp_to_session_rate-t4.imp_to_session_rate)/iff(t4.imp_to_session_rate=0 or t4.imp_to_session_rate is null,1,t4.imp_to_session_rate)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Impression to Session %",
    '$' || to_varchar(t1.cps, '999,999,999,999.00') as "CPS",
    to_varchar(((t1.cps-t2.cps)/iff(t2.cps=0 or t2.cps is null,1,t2.cps)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP CPS",
    to_varchar(((t1.cps-t3.cps)/iff(t3.cps=0 or t3.cps is null,1,t3.cps)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date CPS",
    to_varchar(((t1.cps-t4.cps)/iff(t4.cps=0 or t4.cps is null,1,t4.cps)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day CPS",
    '$' || to_varchar(t1.same_session_vip_cac, '999,999,999,999.00') as "Same Session VIP CAC",
    to_varchar(((t1.same_session_vip_cac-t2.same_session_vip_cac)/iff(t2.same_session_vip_cac=0 or t2.same_session_vip_cac is null,1,t2.same_session_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Same Session VIP CAC",
    to_varchar(((t1.same_session_vip_cac-t3.same_session_vip_cac)/iff(t3.same_session_vip_cac=0 or t3.same_session_vip_cac is null,1,t3.same_session_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Same Session VIP CAC",
    to_varchar(((t1.same_session_vip_cac-t4.same_session_vip_cac)/iff(t4.same_session_vip_cac=0 or t4.same_session_vip_cac is null,1,t4.same_session_vip_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Same Session VIP CAC",
    '$' || to_varchar(t1.session_vips_from_leads_24h_cac, '999,999,999,999.00') as "Session VIPs from Leads 24H CAC",
    to_varchar(((t1.session_vips_from_leads_24h_cac-t2.session_vips_from_leads_24h_cac)/iff(t2.session_vips_from_leads_24h_cac=0 or t2.session_vips_from_leads_24h_cac is null,1,t2.session_vips_from_leads_24h_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ PoP Session VIPs from Leads 24H CAC",
    to_varchar(((t1.session_vips_from_leads_24h_cac-t3.session_vips_from_leads_24h_cac)/iff(t3.session_vips_from_leads_24h_cac=0 or t3.session_vips_from_leads_24h_cac is null,1,t3.session_vips_from_leads_24h_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Date Session VIPs from Leads 24H CAC",
    to_varchar(((t1.session_vips_from_leads_24h_cac-t4.session_vips_from_leads_24h_cac)/iff(t4.session_vips_from_leads_24h_cac=0 or t4.session_vips_from_leads_24h_cac is null,1,t4.session_vips_from_leads_24h_cac)) * 100, '999,999,999,999.0') || '%' as "% ∆ YoY, Same Day Session VIPs from Leads 24H CAC"
from _additional_calcs_raw_pop t1
left join _additional_calcs_raw_pop t2 on t1.date_segment = t2.date_segment and t1.store_brand_name = t2.store_brand_name and t1.region = t2.region and t1.country = t2.country and t1.channel = t2.channel
and (case when t1.date_segment = 'daily' then t1.date = (t2.date + 1)
          when t1.date_segment = 'weekly' then t1.date = dateadd(week,+1,t2.date)
          when t1.date_segment = 'monthly' then t1.date = dateadd(month,+1,t2.date)
          when t1.date_segment = 'yearly' then t1.date = dateadd(year,+1,t2.date) end)
left join _additional_calcs_raw_pop t3 on t1.date_segment = t3.date_segment and t1.store_brand_name = t3.store_brand_name and t1.region = t3.region and t1.country = t3.country and t1.channel = t3.channel
and (case when t1.date_segment = 'daily' then t1.date = dateadd(year,+1,t3.date)
          when t1.date_segment = 'weekly' then t1.date_pop = t3.date
          when t1.date_segment = 'monthly' then t1.date_pop = t3.date end)
left join _additional_calcs_raw_pop t4 on t1.date_segment = t4.date_segment and t1.store_brand_name = t4.store_brand_name and t1.region = t4.region and t1.country = t4.country and t1.channel = t4.channel
and (case when t1.date_segment = 'daily' then t1.date_pop = t4.date end)
where t1.latest_date_flag = 1;

------------------------------------------------------------------------------------
---- union all sources for final

create or replace temporary table _final as
select * from _total
unpivot (value for measure in ("Total Media Spend", "% ∆ PoP Total Media Spend", "% ∆ YoY, Same Date Total Media Spend", "% ∆ YoY, Same Day Total Media Spend",
                                "Total Leads", "% ∆ PoP Total Leads", "% ∆ YoY, Same Date Total Leads", "% ∆ YoY, Same Day Total Leads",
                                "Total VIPs", "% ∆ PoP Total VIPs", "% ∆ YoY, Same Date Total VIPs", "% ∆ YoY, Same Day Total VIPs",
                                "Total CPL", "% ∆ PoP Total CPL", "% ∆ YoY, Same Date Total CPL", "% ∆ YoY, Same Day Total CPL",
                                "Total VIP CAC", "% ∆ PoP Total VIP CAC", "% ∆ YoY, Same Date Total VIP CAC", "% ∆ YoY, Same Day Total VIP CAC"))

union all
select * from _total_online_only
unpivot (value for measure in ("Total Leads - Online Only", "% ∆ PoP Total Leads - Online Only", "% ∆ YoY, Same Date Total Leads - Online Only", "% ∆ YoY, Same Day Total Leads - Online Only",
                                "Total VIPs - Online Only", "% ∆ PoP Total VIPs - Online Only", "% ∆ YoY, Same Date Total VIPs - Online Only", "% ∆ YoY, Same Day Total VIPs - Online Only",
                                "Total CPL - Online Only", "% ∆ PoP Total CPL - Online Only", "% ∆ YoY, Same Date Total CPL - Online Only", "% ∆ YoY, Same Day Total CPL - Online Only",
                                "Total VIP CAC - Online Only", "% ∆ PoP Total VIP CAC - Online Only", "% ∆ YoY, Same Date Total VIP CAC - Online Only", "% ∆ YoY, Same Day Total VIP CAC - Online Only"))
union all
select * from _total_sessions
unpivot (value for measure in ("Total Sessions", "% ∆ PoP Total Sessions", "% ∆ YoY, Same Date Total Sessions", "% ∆ YoY, Same Day Total Sessions",
                                "Total Leads from Session", "% ∆ PoP Total Leads from Session", "% ∆ YoY, Same Date Total Leads from Session", "% ∆ YoY, Same Day Total Leads from Session",
                                "Total Session VIPs from Leads 24H", "% ∆ PoP Total Session VIPs from Leads 24H", "% ∆ YoY, Same Date Total Session VIPs from Leads 24H", "% ∆ YoY, Same Day Total Session VIPs from Leads 24H",
                                "Total Same Session VIPs", "% ∆ PoP Total Same Session VIPs", "% ∆ YoY, Same Date Total Same Session VIPs", "% ∆ YoY, Same Day Total Same Session VIPs",
                                "Total Session to Lead %", "% ∆ PoP Total Session to Lead %", "% ∆ YoY, Same Date Total Session to Lead %", "% ∆ YoY, Same Day Total Session to Lead %",
                                "Total Session Lead to VIP 24H %", "% ∆ PoP Total Session Lead to VIP 24H %", "% ∆ YoY, Same Date Total Session Lead to VIP 24H %", "% ∆ YoY, Same Day Total Session Lead to VIP 24H %",
                                "Total Session to VIP %", "% ∆ PoP Total Session to VIP %", "% ∆ YoY, Same Date Total Session to VIP %", "% ∆ YoY, Same Day Total Session to VIP %",
                                "Total Quiz Starts", "% ∆ PoP Total Quiz Starts", "% ∆ YoY, Same Date Total Quiz Starts","% ∆ YoY, Same Day Total Quiz Starts",
                                "Total Quiz Completes", "% ∆ PoP Total Quiz Completes", "% ∆ YoY, Same Date Total Quiz Completes", "% ∆ YoY, Same Day Total Quiz Completes",
                                "Total Quiz Complete %", "% ∆ PoP Total Quiz Complete %", "% ∆ YoY, Same Date Total Quiz Complete %", "% ∆ YoY, Same Day Total Quiz Complete %"))

union all
select * from _media_mix
unpivot (value for measure in ("% Media Mix", "% ∆ PoP % Media Mix","% ∆ YoY, Same Date % Media Mix","% ∆ YoY, Same Day  % Media Mix"))

union all
select * from _media_eng
unpivot (value for measure in ("Impressions", "% ∆ PoP Impressions","% ∆ YoY, Same Date Impressions", "% ∆ YoY, Same Day  Impressions", "% Impressions",
                                "CPM", "% ∆ PoP CPM","% ∆ YoY, Same Date CPM", "% ∆ YoY, Same Day  CPM",
                                "Clicks", "% Clicks", "CPC", "% ∆ PoP CPC","% ∆ YoY, Same Date CPC", "% ∆ YoY, Same Day  CPC"))

union all
select * from _sessions
unpivot (value for measure in ("Sessions", "% ∆ PoP Sessions", "% ∆ YoY, Same Date Sessions", "% ∆ YoY, Same Day Sessions",
                                "% Sessions", "% ∆ PoP % Sessions", "% ∆ YoY, Same Date % Sessions", "% ∆ YoY, Same Day % Sessions",
                                "Leads from Session", "% ∆ PoP Leads from Session", "% ∆ YoY, Same Date Leads from Session", "% ∆ YoY, Same Day Leads from Session",
                                "Session VIPs from Leads 24H", "% ∆ PoP Session VIPs from Leads 24H", "% ∆ YoY, Same Date Session VIPs from Leads 24H", "% ∆ YoY, Same Day Session VIPs from Leads 24H",
                                "Same Session VIPs", "% ∆ PoP Same Session VIPs", "% ∆ YoY, Same Date Same Session VIPs", "% ∆ YoY, Same Day Same Session VIPs",
                                "Session to Lead %", "% ∆ PoP Session to Lead %", "% ∆ YoY, Same Date Session to Lead %", "% ∆ YoY, Same Day Session to Lead %",
                                "Session Lead to VIP 24H %", "% ∆ PoP Session Lead to VIP 24H %", "% ∆ YoY, Same Date Session Lead to VIP 24H %", "% ∆ YoY, Same Day Session Lead to VIP 24H %",
                                "Session to VIP %", "% ∆ PoP Session to VIP %", "% ∆ YoY, Same Date Session to VIP %", "% ∆ YoY, Same Day Session to VIP %",
                                "% Leads from Session", "% ∆ PoP % Leads from Session", "% ∆ YoY, Same Date % Leads from Session", "% ∆ YoY, Same Day % Leads from Session",
                                "% Same Session VIPs", "% ∆ PoP % Same Session VIPs", "% ∆ YoY, Same Date % Same Session VIPs", "% ∆ YoY, Same Day % Same Session VIPs",
                                "Quiz Starts", "% ∆ PoP Quiz Starts", "% ∆ YoY, Same Date Quiz Starts","% ∆ YoY, Same Day Quiz Starts",
                                "Quiz Completes", "% ∆ PoP Quiz Completes", "% ∆ YoY, Same Date Quiz Completes", "% ∆ YoY, Same Day Quiz Completes",
                                "Quiz Complete %", "% ∆ PoP Quiz Complete %", "% ∆ YoY, Same Date Quiz Complete %", "% ∆ YoY, Same Day Quiz Complete %"))

union all
select * from _cbl
unpivot (value for measure in ("Media Spend", "% ∆ PoP Media Spend", "% ∆ YoY, Same Date Media Spend", "% ∆ YoY, Same Day Media Spend",
                                "CBL Leads", "CBL CPL", "CBL D1 VIPs from Leads", "CBL D1 VIPs from Leads CAC",
                                "CBL Total VIPs on Date", "% ∆ PoP CBL Total VIPs on Date", "% ∆ YoY, Same Date CBL Total VIPS on Date", "% ∆ YoY, Same Day CBL Total VIPS on Date",
                                "CBL Total VIP CAC", "% ∆ PoP CBL Total VIP CAC", "% ∆ YoY, Same Date CBL Total VIP CAC", "% ∆ YoY, Same Day CBL Total VIP CAC",
                                "CBL % VIPs from Click", "CBL % VIPs from HDYH"))

union all
select * from _hdyh
unpivot (value for measure in ("HDYH VIPs on Date", "% ∆ PoP HDYH VIPs on Date", "% ∆ YoY, Same Date HDYH VIPs on Date", "% ∆ YoY, Same Day HDYH VIPs on Date",
                               "HDYH VIP CAC", "% ∆ PoP HDYH VIP CAC", "% ∆ YoY, Same Date HDYH VIP CAC", "% ∆ YoY, Same Day HDYH VIP CAC",
                               "HDYH D91+ VIPs", "% ∆ PoP HDYH D91+ VIPs", "% ∆ YoY, Same Date HDYH D91+ VIPs", "% ∆ YoY, Same Day HDYH D91+ VIPs",
                               "HDYH D91+ VIP CAC", "% ∆ PoP HDYH D91+ VIP CAC", "% ∆ YoY, Same Date HDYH D91+ VIP CAC", "% ∆ YoY, Same Day HDYH D91+ VIP CAC",
                               "HDYH D1 VIPs", "% ∆ PoP HDYH D1 VIPs", "% ∆ YoY, Same Date HDYH D1 VIPs", "% ∆ YoY, Same Day HDYH D1 VIPs",
                               "HDYH D1 VIP CAC", "% ∆ PoP HDYH D1 VIP CAC", "% ∆ YoY, Same Date HDYH D1 VIP CAC", "% ∆ YoY, Same Day HDYH D1 VIP CAC",
                               "% D1 HYDYH VIPs", "% D2-7 HYDYH VIPs","% D8-30 HYDYH VIPs","% D31-60 HYDYH VIPs","% D61-90 HYDYH VIPs","% D91+ HYDYH VIPs"))
union all
select * from _mmm
unpivot (value for measure in ("MMM VIPs", "MMM CAC"))

union all
select * from _fb_ui
unpivot (value for measure in ("UI CPL 1x1", "% ∆ PoP UI CPL 1x1", "% ∆ YoY, Same Date UI CPL 1x1", "% ∆ YoY, Same Day UI CPL 1x1",
                                "UI CPL 7x1", "% ∆ PoP UI CPL 7x1", "% ∆ YoY, Same Date UI CPL 7x1", "% ∆ YoY, Same Day UI CPL 7x1",
                                "UI CAC 1DC", "% ∆ PoP UI CAC 1DC", "% ∆ YoY, Same Date UI CAC 1DC", "% ∆ YoY, Same Day UI CAC 1DC",
                                "UI CAC 7DC", "% ∆ PoP UI CAC 7DC", "% ∆ YoY, Same Date UI CAC 7DC", "% ∆ YoY, Same Day UI CAC 7DC",
                                "UI CAC 1x1", "% ∆ PoP UI CAC 1x1", "% ∆ YoY, Same Date UI CAC 1x1", "% ∆ YoY, Same Day UI CAC 1x1",
                                "UI CAC 7x1", "% ∆ PoP UI CAC 7x1", "% ∆ YoY, Same Date UI CAC 7x1", "% ∆ YoY, Same Day UI CAC 7x1",
                                "UI Click to Lead % 1D", "% ∆ PoP UI Click to Lead % 1D", "% ∆ YoY, Same Date UI Click to Lead % 1D", "% ∆ YoY, Same Day UI Click to Lead % 1D",
                                "UI Click to Lead % 7D", "% ∆ PoP UI Click to Lead % 7D", "% ∆ YoY, Same Date UI Click to Lead % 7D", "% ∆ YoY, Same Day UI Click to Lead % 7D",
                                "UI Click to VIP % 1D", "% ∆ PoP UI Click to VIP % 1D", "% ∆ YoY, Same Date UI Click to VIP % 1D", "% ∆ YoY, Same Day UI Click to VIP % 1D",
                                "UI Click to VIP % 7D", "% ∆ PoP UI Click to VIP % 7D", "% ∆ YoY, Same Date UI Click to VIP % 7D", "% ∆ YoY, Same Day UI Click to VIP % 7D"))

union all
select * from _google_ads_ui
unpivot (value for measure in ("UI CAC 7DC", "% ∆ PoP UI CAC 7DC", "% ∆ YoY, Same Date UI CAC 7DC", "% ∆ YoY, Same Day UI CAC 7DC",
                                "UI CAC 7x3x1", "% ∆ PoP UI CAC 7x3x1", "% ∆ YoY, Same Date UI CAC 7x3x1", "% ∆ YoY, Same Day UI CAC 7x3x1",
                                "UI CAC 30DC", "% ∆ PoP UI CAC 30DC", "% ∆ YoY, Same Date UI CAC 30DC", "% ∆ YoY, Same Day UI CAC 30DC",
                                "UI CAC 30x7x1", "% ∆ PoP UI CAC 30x7x1", "% ∆ YoY, Same Date UI CAC 30x7x1", "% ∆ YoY, Same Day UI CAC 30x7x1",
                                "Discovery UI CAC 7DC", "% ∆ PoP Discovery UI CAC 7DC", "% ∆ YoY, Same Date Discovery UI CAC 7DC", "% ∆ YoY, Same Day Discovery UI CAC 7DC",
                                "Discovery UI CAC 7x1", "% ∆ PoP Discovery UI CAC 7x1", "% ∆ YoY, Same Date Discovery UI CAC 7x1", "% ∆ YoY, Same Day Discovery UI CAC 7x1",
                                "GDN UI CAC 7DC", "% ∆ PoP GDN UI CAC 7DC", "% ∆ YoY, Same Date GDN UI CAC 7DC", "% ∆ YoY, Same Day GDN UI CAC 7DC",
                                "GDN UI CAC 7x1", "% ∆ PoP GDN UI CAC 7x1", "% ∆ YoY, Same Date GDN UI CAC 7x1", "% ∆ YoY, Same Day GDN UI CAC 7x1"))

union all
select * from _google_search_ads_ui
unpivot (value for measure in ("UI CAC 1DC", "% ∆ PoP UI CAC 1DC", "% ∆ YoY, Same Date UI CAC 1DC", "% ∆ YoY, Same Day UI CAC 1DC",
                                "UI CAC 7DC", "% ∆ PoP UI CAC 7DC", "% ∆ YoY, Same Date UI CAC 7DC", "% ∆ YoY, Same Day UI CAC 7DC",
                                "UI CAC 7x3x1", "% ∆ PoP UI CAC 7x3x1", "% ∆ YoY, Same Date UI CAC 7x3x1", "% ∆ YoY, Same Day UI CAC 7x3x1",
                                "UI CAC 30DC", "% ∆ PoP UI CAC 30DC", "% ∆ YoY, Same Date UI CAC 30DC", "% ∆ YoY, Same Day UI CAC 30DC",
                                "UI CAC 30x7x1", "% ∆ PoP UI CAC 30x7x1", "% ∆ YoY, Same Date UI CAC 30x7x1", "% ∆ YoY, Same Day UI CAC 30x7x1"))

union all
select * from _other_ui
unpivot (value for measure in ("UI CPL 1x1", "% ∆ PoP UI CPL 1x1", "% ∆ YoY, Same Date UI CPL 1x1", "% ∆ YoY, Same Day UI CPL 1x1",
                                "UI CPL 7x1", "% ∆ PoP UI CPL 7x1", "% ∆ YoY, Same Date UI CPL 7x1", "% ∆ YoY, Same Day UI CPL 7x1",
                                "UI CAC 1DC", "% ∆ PoP UI CAC 1DC", "% ∆ YoY, Same Date UI CAC 1DC", "% ∆ YoY, Same Day UI CAC 1DC",
                                "UI CAC 7DC", "% ∆ PoP UI CAC 7DC", "% ∆ YoY, Same Date UI CAC 7DC", "% ∆ YoY, Same Day UI CAC 7DC",
                                "UI CAC 1x1", "% ∆ PoP UI CAC 1x1", "% ∆ YoY, Same Date UI CAC 1x1", "% ∆ YoY, Same Day UI CAC 1x1",
                                "UI CAC 7x1", "% ∆ PoP UI CAC 7x1", "% ∆ YoY, Same Date UI CAC 7x1", "% ∆ YoY, Same Day UI CAC 7x1",
                                "UI Click to Lead % 1D", "% ∆ PoP UI Click to Lead % 1D", "% ∆ YoY, Same Date UI Click to Lead % 1D", "% ∆ YoY, Same Day UI Click to Lead % 1D",
                                "UI Click to Lead % 7D", "% ∆ PoP UI Click to Lead % 7D", "% ∆ YoY, Same Date UI Click to Lead % 7D", "% ∆ YoY, Same Day UI Click to Lead % 7D",
                                "UI Click to VIP % 1D", "% ∆ PoP UI Click to VIP % 1D", "% ∆ YoY, Same Date UI Click to VIP % 1D", "% ∆ YoY, Same Day UI Click to VIP % 1D",
                                "UI Click to VIP % 7D", "% ∆ PoP UI Click to VIP % 7D", "% ∆ YoY, Same Date UI Click to VIP % 7D", "% ∆ YoY, Same Day UI Click to VIP % 7D",
                                "Tatari TV UI CAC Incremental", "% ∆ PoP Tatari TV UI CAC Incremental", "% ∆ YoY, Same Date Tatari TV UI CAC Incremental", "% ∆ YoY, Same Day Tatari TV UI CAC Incremental",
                                "Tatari Streaming UI CAC Incremental", "% ∆ PoP Tatari Streaming UI CAC Incremental", "% ∆ YoY, Same Date Tatari Streaming UI CAC Incremental", "% ∆ YoY, Same Day Tatari Streaming UI CAC Incremental",
                                "Tatari Streaming UI CAC 1DV", "% ∆ PoP Tatari Streaming UI CAC 1DV", "% ∆ YoY, Same Date Tatari Streaming UI CAC 1DV", "% ∆ YoY, Same Day Tatari Streaming UI CAC 1DV",
                                "Tatari Streaming UI CAC 7DV", "% ∆ PoP Tatari Streaming UI CAC 7DV", "% ∆ YoY, Same Date Tatari Streaming UI CAC 7DV", "% ∆ YoY, Same Day Tatari Streaming UI CAC 7DV",
                                "BPM Streaming UI CAC 3DV", "% ∆ PoP BPM Streaming UI CAC 3DV", "% ∆ YoY, Same Date BPM Streaming UI CAC 3DV", "% ∆ YoY, Same Day BPM Streaming UI CAC 3DV",
                                "BPM Streaming UI CAC 7DV", "% ∆ PoP BPM Streaming UI CAC 7DV", "% ∆ YoY, Same Date BPM Streaming UI CAC 7DV", "% ∆ YoY, Same Day BPM Streaming UI CAC 7DV"))

union all
select * from _additional_calcs
unpivot (value for measure in ("CPS", "% ∆ PoP CPS", "% ∆ YoY, Same Date CPS", "% ∆ YoY, Same Day CPS",
                                "Impression to Session %", "% ∆ PoP Impression to Session %", "% ∆ YoY, Same Date Impression to Session %", "% ∆ YoY, Same Day Impression to Session %",
                                "Same Session VIP CAC", "% ∆ PoP Same Session VIP CAC", "% ∆ YoY, Same Date Same Session VIP CAC", "% ∆ YoY, Same Day Same Session VIP CAC",
                                "Session VIPs from Leads 24H CAC", "% ∆ PoP Session VIPs from Leads 24H CAC", "% ∆ YoY, Same Date Session VIPs from Leads 24H CAC", "% ∆ YoY, Same Day Session VIPs from Leads 24H CAC"));

update _final
set value = replace(value,' ','');

create or replace transient table reporting_media_prod.attribution.total_attribution as
select *, current_timestamp()::timestamp_ltz as process_meta_update_datetime
from _final
where date < current_date()
and store_brand_name is not null
and value != 'remove';
