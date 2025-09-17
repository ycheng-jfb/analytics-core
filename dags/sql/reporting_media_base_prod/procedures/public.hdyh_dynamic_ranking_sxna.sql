use schema reporting_media_base_prod.public;
set n = 7; --number of days to consider for post recency
set number_of_influencers_in_hdyh = 5;
set calibration_date_range = 90;
set ranking_hours_exponent = 2;


-- create temp mapping table
create or replace TEMPORARY table _sxna_influencer_mapping AS
select distinct *
from lake_view.sharepoint.med_influencer_mapping
where lower(store_name_abbr) = 'sx'
and media_partner_id is not null
and CIQ_PUBLISHER_ID is not null;

-- get hdyh selections
create or replace TEMPORARY table _infl_hdyh_leads AS
select
    STORE_ID,
    SESSION_ID,
    CUSTOMER_ID,
    REGISTRATION_LOCAL_DATETIME,
    HOW_DID_YOU_HEAR as HOWDIDYOUHEAR
from edw_prod.data_model.fact_registration l
--join lake_view.sharepoint.hdyh_legacy_mapping h on lower(l.HOWDIDYOUHEAR) = lower(h.HDYH_VALUE)
join _sxna_influencer_mapping h on h.INFLUENCER_CLEANED_NAME = l.HOW_DID_YOU_HEAR
where STORE_ID = 115 -- sxf
    and REGISTRATION_LOCAL_DATETIME >= current_date() - $calibration_date_range;

create or replace TEMPORARY table _infl_hdyh_selections AS
select
    hdyh_no_space as howdidyouhear_cleaned,
    to_varchar(MEDIA_PARTNER_ID) as media_partner_id,
    to_date(REGISTRATION_LOCAL_DATETIME) as REGISTRATION_LOCAL_DATE,
    count(CUSTOMER_ID) as count_hdyh_selections
from
    (select l.*,
           m.MEDIA_PARTNER_ID,
           ACTIVATION_LOCAL_DATETIME,
           to_date(registration_local_datetime) as registration_local_date,
           trim(HOWDIDYOUHEAR) as hdyh_trim,
            replace(hdyh_trim, ' ', '') as hdyh_no_space
    from _infl_hdyh_leads l
    join edw_prod.data_model.fact_activation v on v.CUSTOMER_ID = l.CUSTOMER_ID
        and datediff(day,REGISTRATION_LOCAL_DATETIME,ACTIVATION_LOCAL_DATETIME) < 30
    --LEFT JOIN LAKE_VIEW.SHAREPOINT.MED_SELECTOR_INFLUENCER_PAID_SPEND_MAPPINGS m on lower(m.HDYH) = lower(l.HOWDIDYOUHEAR)
    left join _sxna_influencer_mapping m on m.INFLUENCER_CLEANED_NAME = l.HOWDIDYOUHEAR
    where howdidyouhear != 'Influencer/Blogger' )
group by 1,2,3;

--------- get the reallocated hdyh selections + clicks ----------
create or replace TEMPORARY table _infl_hdyh_selections_reallocated AS
with
     t1 as
    (select *
    from _infl_hdyh_selections),

     t2 as (
        select
            split_part(MEDIA_PARTNER_ID, '|', 0) as media_partner_id_split,
            to_date(event_datetime) as event_date,
            count(CUSTOMER_ID) as hdyh_selections_reallocated
        from reporting_media_base_prod.influencers.click_conversion_counts
        where lower(event_type) = 'vip'
        and media_store_id = 121
        and media_partner_id_split in (select distinct media_partner_id from _sxna_influencer_mapping)
        group by 1,2
     )

    select t1.*,
           t2.hdyh_selections_reallocated,
           coalesce(hdyh_selections_reallocated, count_hdyh_selections) as hdyh_selections_final
    from t1
    left join t2 on t2.media_partner_id_split = lower(t1.media_partner_id)
    and t2.event_date = t1.REGISTRATION_LOCAL_DATE;



-- join influencer data with mapping to bring in media partner ID
-- exclude if they are inactive or overwritten to be excluded
create or replace TEMPORARY table _sxna_influencer_data_daily AS
select i.*,
       to_varchar(m.media_partner_id) as media_partner_id,
       min(i.DAYS_SINCE_POST) over(partition by PUBLISHER_ID) as min_days_since_post
from reporting_media_base_prod.public.influencer_impact_score_daily_sxna i
left join _sxna_influencer_mapping m on m.CIQ_PUBLISHER_ID = i.PUBLISHER_ID
where MEDIA_PARTNER_ID not in (select distinct MEDIA_PARTNER_ID from _sxna_influencer_mapping where lower(EXCLUDE_FROM_HDYH) = 'exclude' or lower(active) = 'inactive');


-- get calibration rate
-- merge with hdyh selections
-- get the mean and 3 std plus/minus
-- select only when calibration rate is within 3 std plus or minus
create or replace TEMPORARY table _sxna_recently_calibrated_influencers AS
with impact as  -- get impact rate past calibration_date_range days, summed by day
    (select
        date,
           PUBLISHER_ID,
           PUBLISHER_NAME,
           MEDIA_PARTNER_ID,
           sum(IMPACT_SCORE) as impact_score_by_day
    from _sxna_influencer_data_daily
    where date >= current_date() - $calibration_date_range
    and IMPACT_SCORE > 0
    group by 1,2,3,4),

    t2 as  -- get the hdyh selections on day
    (select  i.*,
            hdyh.hdyh_selections_final
    from impact i
    join _infl_hdyh_selections_reallocated hdyh on to_varchar(hdyh.MEDIA_PARTNER_ID) = to_varchar(i.MEDIA_PARTNER_ID)
    and hdyh.REGISTRATION_LOCAL_DATE = i.date),

    t3 as  -- get the calibration rate
     (select PUBLISHER_ID, PUBLISHER_NAME, MEDIA_PARTNER_ID,
             max(date) as most_recent_calibration_date,
            sum(hdyh_selections_final) / sum(impact_score_by_day) as calibration_rate
    from t2
    group by 1,2,3),

    stdev_table as  -- get plus/minus 3 stddev from mean calibration rate
(    select
           avg(calibration_rate) as avg_calibration_rate,
           stddev(calibration_rate) as std_calibration_rate,
            2 * std_calibration_rate as plus_2_std_calibration_rate,
           -2 * std_calibration_rate as minus_2_std_calibration_rate
    from t3)

    -- filter to only include within plus/minus 3 stdev
    select *
    from t3
    where calibration_rate > (select minus_2_std_calibration_rate from stdev_table)
           and calibration_rate < (select plus_2_std_calibration_rate from stdev_table);


------ PARTITIONS -----------
set update_datetime = current_timestamp();

-- TIER 1: If they are in HDYH currently, and haven't been in there for at least 24 hours, keep them in
create or replace TEMPORARY table _sxna_current_ranking_to_keep AS
with current_ranking_to_keep as
    (select *,
    datediff("hours", update_datetime, CURRENT_TIMESTAMP()) as hours_in_hdyh
    from reporting_media_base_prod.public.hdyh_influencer_ranking_sxna
    where hours_in_hdyh <= 24)

    select
       to_varchar(MEDIA_PARTNER_ID) as MEDIA_PARTNER_ID,
       engagement_per_influencer,
       min_hours_since_last_post_per_influencer,
       ranking_score,
       rank,
           1 as tier,
       to_timestamp_ltz(UPDATE_DATETIME) as UPDATE_DATETIME

from current_ranking_to_keep;



-- TIER 2: group that has not been calibrated recently, has posted in past 30 days
create or replace TEMPORARY table _tier_2_hdyh_ranking AS
with tier_2_influencers as (
    select * from _sxna_influencer_data_daily
    where DAYS_SINCE_POST <= $n
    and MEDIA_PARTNER_ID not in (select distinct MEDIA_PARTNER_ID from _sxna_recently_calibrated_influencers)  -- if they have been calibrated in the past 90 days, filter them out
),

    -- get metrics per influencer
    metrics_per_influencer as
(  select MEDIA_PARTNER_ID,
         max(IMPACT_SCORE) as engagement_per_influencer,
          min(HOURS_SINCE_POST) as min_hours_since_last_post_per_influencer,
          engagement_per_influencer / (1 + power(min_hours_since_last_post_per_influencer, $ranking_hours_exponent)) as ranking_score
    from tier_2_influencers
    group by  1)

select MEDIA_PARTNER_ID,
       engagement_per_influencer,
       min_hours_since_last_post_per_influencer,
       ranking_score,
       rank() over (order by ranking_score desc) as rank,
       2 as tier,
       $update_datetime as update_datetime
from metrics_per_influencer
    where engagement_per_influencer is not null;


-- TIER 3: group that has been calibrated recently, has posted in past 30 days
create or replace TEMPORARY table _tier_3_hdyh_ranking AS
with tier_3_influencers as (
    select * from _sxna_influencer_data_daily
    where DAYS_SINCE_POST <= $n
    and MEDIA_PARTNER_ID in (select distinct MEDIA_PARTNER_ID from _sxna_recently_calibrated_influencers)  -- if they have been calibrated in the past 90 days, filter them out
),

    -- get metrics per influencer
    metrics_per_influencer as
(  select MEDIA_PARTNER_ID,
         max(IMPACT_SCORE) as engagement_per_influencer,
          min(HOURS_SINCE_POST) as min_hours_since_last_post_per_influencer,
          engagement_per_influencer / (1 + power(min_hours_since_last_post_per_influencer, $ranking_hours_exponent)) as ranking_score
    from tier_3_influencers
    group by  1)

select MEDIA_PARTNER_ID,
       engagement_per_influencer,
       min_hours_since_last_post_per_influencer,
       ranking_score,
       rank() over (order by ranking_score desc) as rank,
       3 as tier,
       $update_datetime as update_datetime
from metrics_per_influencer
    where engagement_per_influencer is not null;


-- Tier 4: group that has not been calibrated recently, has not posted in past 30 days
create or replace TEMPORARY table _tier_4_hdyh_ranking AS
with tier_4_influencers as (
    select * from _sxna_influencer_data_daily
    where DAYS_SINCE_POST > $n
    and MEDIA_PARTNER_ID not in (select distinct MEDIA_PARTNER_ID from _sxna_recently_calibrated_influencers)  -- if they have been calibrated in the past 90 days, filter them out
),

    -- get metrics per influencer
    metrics_per_influencer as
(  select MEDIA_PARTNER_ID,
         max(IMPACT_SCORE) as engagement_per_influencer,
          min(HOURS_SINCE_POST) as min_hours_since_last_post_per_influencer,
          engagement_per_influencer / (1 + power(min_hours_since_last_post_per_influencer, $ranking_hours_exponent)) as ranking_score
    from tier_4_influencers
    group by  1)

select MEDIA_PARTNER_ID,
       engagement_per_influencer,
       min_hours_since_last_post_per_influencer,
       ranking_score,
       rank() over (order by ranking_score desc) as rank,
       4 as tier,
       $update_datetime as update_datetime
from metrics_per_influencer
    where engagement_per_influencer is not null;


--  Tier 5: group that has been calibrated recently, has not posted in past 30 days
create or replace TEMPORARY table _tier_5_hdyh_ranking AS
with tier_5_influencers as (
    select * from _sxna_influencer_data_daily
    where DAYS_SINCE_POST > $n
    and MEDIA_PARTNER_ID in (select distinct MEDIA_PARTNER_ID from _sxna_recently_calibrated_influencers)  -- if they have been calibrated in the past 90 days, filter them out
),

    -- get metrics per influencer
    metrics_per_influencer as
(  select MEDIA_PARTNER_ID,
         max(IMPACT_SCORE) as engagement_per_influencer,
          min(HOURS_SINCE_POST) as min_hours_since_last_post_per_influencer,
          engagement_per_influencer / (1 + power(min_hours_since_last_post_per_influencer, $ranking_hours_exponent)) as ranking_score
    from tier_5_influencers
    group by  1)

select MEDIA_PARTNER_ID,
       engagement_per_influencer,
       min_hours_since_last_post_per_influencer,
       ranking_score,
       rank() over (order by ranking_score desc) as rank,
       5 as tier,
       $update_datetime as update_datetime
from metrics_per_influencer
    where engagement_per_influencer is not null;



-- Union, and filter out if the influencer is in previous tiers so they are not dulicated
-- use rownumber to select the first occurance of the media partner ID

create or replace TEMPORARY table _sxna_current_ranking AS

    with t1 as
    (select * from _sxna_current_ranking_to_keep
    union
    select * from _tier_2_hdyh_ranking
    where MEDIA_PARTNER_ID not in (select distinct MEDIA_PARTNER_ID from _sxna_current_ranking_to_keep)
    union
    select * from _tier_3_hdyh_ranking
    where MEDIA_PARTNER_ID not in (select distinct MEDIA_PARTNER_ID from _sxna_current_ranking_to_keep)
    and MEDIA_PARTNER_ID not in (select distinct MEDIA_PARTNER_ID from _tier_2_hdyh_ranking)
    union
    select * from _tier_4_hdyh_ranking
    where MEDIA_PARTNER_ID not in (select distinct MEDIA_PARTNER_ID from _sxna_current_ranking_to_keep)
    and MEDIA_PARTNER_ID not in (select distinct MEDIA_PARTNER_ID from _tier_2_hdyh_ranking)
    and MEDIA_PARTNER_ID not in (select distinct MEDIA_PARTNER_ID from _tier_3_hdyh_ranking)
    union
    select * from _tier_5_hdyh_ranking
    where MEDIA_PARTNER_ID not in (select distinct MEDIA_PARTNER_ID from _sxna_current_ranking_to_keep)
    and MEDIA_PARTNER_ID not in (select distinct MEDIA_PARTNER_ID from _tier_2_hdyh_ranking)
    and MEDIA_PARTNER_ID not in (select distinct MEDIA_PARTNER_ID from _tier_3_hdyh_ranking)
    and MEDIA_PARTNER_ID not in (select distinct MEDIA_PARTNER_ID from _tier_4_hdyh_ranking)
    order by tier, rank),

    t2 as

         (select distinct to_varchar(t1.MEDIA_PARTNER_ID) as MEDIA_PARTNER_ID,
                engagement_per_influencer,
                min_hours_since_last_post_per_influencer,
                ranking_score,
                tier,
                t1.update_datetime,
                m.INFLUENCER_CLEANED_NAME,
                row_number() over (partition by m.MEDIA_PARTNER_ID order by ranking_score desc) as row_number
        from t1
        left join _sxna_influencer_mapping m on m.MEDIA_PARTNER_ID = t1.MEDIA_PARTNER_ID
        where t1.MEDIA_PARTNER_ID is not null
        and m.INFLUENCER_CLEANED_NAME is not null)

    select distinct
                MEDIA_PARTNER_ID,
                engagement_per_influencer,
                min_hours_since_last_post_per_influencer,
                ranking_score,
                rank() over(order by tier, ranking_score desc) as rank,
                tier,
                update_datetime,
                INFLUENCER_CLEANED_NAME
    from t2
    where row_number = 1;



-- get final ranking
create or replace transient table reporting_media_base_prod.public.hdyh_influencer_ranking_sxna as
select * from _sxna_current_ranking
order by rank, tier
limit $number_of_influencers_in_hdyh;


--append to historical table
insert into reporting_media_base_prod.public.hdyh_influencer_ranking_historical
select MEDIA_PARTNER_ID,
       ranking_score,
       engagement_per_influencer,
       min_hours_since_last_post_per_influencer,
       rank,
       tier,
       update_datetime,
       INFLUENCER_CLEANED_NAME,
       $update_datetime as refresh_datetime,
       (select distinct STORE_GROUP_ID from edw_prod.data_model.dim_store where store_brand_abbr = 'SX' and store_country = 'US') as store_group_id,
       121 as store_id
from _sxna_current_ranking
order by rank, tier
limit $number_of_influencers_in_hdyh;
