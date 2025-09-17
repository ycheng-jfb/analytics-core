
--get impressions grouped by operating system, total impressions, and operating system as percent of total impressions
create or replace temporary table _osdelivery as
select ad_id,
       date as date,
       case when impression_device in ('iphone','ipod','ipad') then 'ios'
           when impression_device ilike '%android%' then 'android'
           else 'other' end as operating_system,
       sum(impressions) as os_impressions,
       sum(os_impressions) over(partition by ad_id, date) as total_impressions,
       os_impressions/nullifzero(total_impressions) as os_percent_total_impressions
from LAKE_VIEW.FACEBOOK.AD_INSIGHTS_BY_PLATFORM
where date >= '2018-01-01'
group by 1,2,3;

--get ios percent of total impressions using ios filter
create or replace temporary table _osdeliveryios as
select ad_id, date, operating_system, os_percent_total_impressions as ios_percentimpressions
from _osdelivery
where operating_system = 'ios';

--get android percent of total impressions using android filter
create or replace temporary table _osdeliveryandroid as
select ad_id, date, operating_system, os_percent_total_impressions as android_percentimpressions
from _osdelivery
where operating_system = 'android';

--combine total impressions, ios percent impressions, and android percent impressions
create or replace temporary table _oscombined as
select t.*,
       i.ios_percentimpressions,
       a.android_percentimpressions
from _osdelivery t
left join _osdeliveryios i on t.ad_id = i.ad_id
    and t.date = i.date
    and t.operating_system = i.operating_system
left join _osdeliveryandroid a on t.ad_id=a.ad_id
    and t.date = a.date
    and t.operating_system = a.operating_system;

--pivot table so ios percent and android percent impressions are on one row
create or replace transient table reporting_media_base_prod.facebook.operating_system_percent_daily_impressions as
select ad_id,
       date,
       max(ios_percentimpressions) as ios_percentimpressions,
       max(android_percentimpressions) as android_percentimpressions
from _oscombined
group by 1,2;
