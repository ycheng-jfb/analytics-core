use schema reporting_media_base_prod.public;
set daily_data_start_date_flna ='2020-12-01';


-- daily engagements data
create or replace temporary table _ciq_impact_score_flna as
select
    campaignname,
    publisherid,
    PUBLISHERNAME,
    postid,
    lower(POSTTYPE) as posttype,
    lower(SOCIALNETWORK) as socialnetwork,
    NETWORKIMPRESSIONS,
    coalesce(engagement,networkengagements) as total_engagements,

               case when lower(SOCIALNETWORK) = 'facebook' then coalesce(likes,0) + 2*coalesce(comments, 0) + 10*coalesce(totalshares,0)
               when lower(SOCIALNETWORK) = 'instagram' and lower(POSTTYPE) in ('carousel', 'video', 'image', 'album') then coalesce(likes,0) + 2*coalesce(comments,0) + 10*coalesce(saves,0)
               when lower(SOCIALNETWORK) = 'instagram' and lower(POSTTYPE) = 'story' then 15 * coalesce(coalesce(engagement,networkengagements),0)
               when lower(SOCIALNETWORK) = 'pinterest' and lower(POSTTYPE) in ('image', 'video') then coalesce(saves,0) + 10 * coalesce(totalshares, 0)
               when lower(SOCIALNETWORK) = 'twitter' then coalesce(coalesce(engagement,networkengagements), 0)
               else coalesce(coalesce(engagement,networkengagements), 0)
               end as impact_score,

    coalesce(likes, 0) as likes,
    coalesce(COMMENTS, 0) as comments,
    coalesce(TOTALSHARES, 0) as totalshares,
    coalesce(saves, 0) as saves,


       DATESUBMITTED, META_FROM_DATETIME, META_TO_DATETIME,
       META_CREATE_DATETIME,META_IS_CURRENT,
       datediff('days', DATESUBMITTED, current_date()) as days_since_post,
    datediff('hours', DATESUBMITTED, CURRENT_TIMESTAMP()) as hours_since_post,

    case when META_IS_CURRENT = true then META_CREATE_DATETIME
    else META_TO_DATETIME
    end as metric_datetime, -- use the create datetime if most current record

--     case when hour(metric_datetime) < 12 then dateadd(day, -1, to_date(metric_datetime))
--     else to_date(metric_datetime) end as metric_date  -- if the meta_to_datetime.hour < 12, then I should shift the credit to the prev day

    to_date(metric_datetime) as metric_date

from reporting_media_base_prod.creatoriq.campaign_activity_cleansed
    where DATESUBMITTED >= $daily_data_start_date_flna
    and lower(CAMPAIGNNAME) like any ('%fabletics%', '%fl%')
    and not lower(CAMPAIGNNAME)  like any ('%uk%', '%fr%', '%france%', '%germany%', '%eu%', '% men %')
    and linked=1
order by publishername asc, POSTID asc, META_TO_DATETIME asc, META_FROM_DATETIME asc;


create or replace temporary table _impact_score_per_influencer_per_day_flna as
select campaignname,
       publisherid,
       PUBLISHERNAME,
       postid,
       POSTTYPE,
       SOCIALNETWORK,
        metric_date,
       DATESUBMITTED,
       days_since_post,
       hours_since_post,
       datediff(days, to_date(DATESUBMITTED), metric_date) as days_after_post,
       sum(impact_score_daily) as impact_score_daily_summed,
       sum(likes_daily) as likes_daily_summed,
       sum(comments_daily) as comments_daily_summed,
       sum(totalshares_daily) as totalshares_daily_summed,
       sum(saves_daily) as saves_daily_summed,
       sum(networkimpressions_daily) as networkimpressions_daily_summed,
       sum(total_engagements_daily) as total_engagements_daily_summed
from (
    select *,
           impact_score - lag(impact_score, 1, 0) over (partition by publisherid, postid order by META_TO_DATETIME asc, META_FROM_DATETIME asc) as impact_score_daily,
           likes - lag(likes, 1, 0) over (partition by publisherid, postid order by META_TO_DATETIME asc, META_FROM_DATETIME asc) as likes_daily,
           comments - lag(comments, 1, 0) over (partition by publisherid, postid order by META_TO_DATETIME asc, META_FROM_DATETIME asc) as comments_daily,
           totalshares - lag(totalshares, 1, 0) over (partition by publisherid, postid order by META_TO_DATETIME asc, META_FROM_DATETIME asc) as totalshares_daily,
           saves - lag(saves, 1, 0) over (partition by publisherid, postid order by META_TO_DATETIME asc, META_FROM_DATETIME asc) as saves_daily,
           NETWORKIMPRESSIONS - lag(NETWORKIMPRESSIONS, 1, 0) over (partition by publisherid, postid order by META_TO_DATETIME asc, META_FROM_DATETIME asc) as networkimpressions_daily,
           total_engagements - lag(total_engagements, 1, 0) over (partition by publisherid, postid order by META_TO_DATETIME asc, META_FROM_DATETIME asc) as total_engagements_daily

    from _ciq_impact_score_flna
    )
group by campaignname,
       publisherid,
       PUBLISHERNAME,
       postid,
       POSTTYPE,
       SOCIALNETWORK,
        metric_date,
       DATESUBMITTED,
       days_since_post,
       hours_since_post,
        days_after_post
having impact_score_daily_summed >= 0
order by publisherid, postid, metric_date;

-- influencer data by post
create or replace temporary table _impact_score_per_influencer_by_post_flna as
select
    datediff('days', POST_DATETIME, current_date()) as days_since_post,
    datediff('hours', POST_DATETIME, CURRENT_TIMESTAMP()) as hours_since_post,
    POST_DATETIME,
           PUBLISHER_NAME,
           publisher_id,
           post_id,
--             LINK_TO_POST,
           lower(SOCIAL_NETWORK) as social_network,
           lower(POST_TYPE) as post_type,
           CASE WHEN lower(social_network) = 'youtube' then video_views else impressions end as organic_influencer_impressions,
           total_engagements,

           case when lower(social_network) = 'facebook' then coalesce(likes,0) + 2*coalesce(comments, 0) + 10*coalesce(shares,0)
               when lower(social_network) = 'instagram' and lower(post_type) in ('carousel', 'video', 'image', 'album') then coalesce(likes,0) + 2*coalesce(comments,0) + 10*coalesce(saves,0)
               when lower(social_network) = 'instagram' and lower(post_type) = 'story' then 15 * coalesce(total_engagements,0)
               when lower(social_network) = 'pinterest' and lower(post_type) in ('image', 'video') then coalesce(saves,0) + 10 * coalesce(shares, 0)
               when lower(social_network) = 'twitter' then coalesce(total_engagements, 0)
               else coalesce(total_engagements, 0)
               end as impact_score,

        coalesce(likes, 0) as likes,
        coalesce(COMMENTS, 0) as comments,
        coalesce(shares, 0) as totalshares,
        coalesce(saves, 0) as saves,

            FOLLOWERS_AT_TIME_OF_POST,
            engagement_rate,
           CAMPAIGN_NAME

    from reporting_media_base_prod.CREATORIQ.CAMPAIGN_ACTIVITY_AGGREGATE
    where lower(CAMPAIGN_NAME) like any ('%fabletics%', '%fl%')
    and not lower(CAMPAIGN_NAME)  like any ('%uk%', '%fr%', '%france%', '%germany%', '%eu%', '% men %')
    and POST_DATETIME >= '2019-01-01'
    and is_authenticated = 1
    order by PUBLISHER_NAME, hours_since_post;


-- get general decay curve
create or replace temporary table _general_decay_curve_flna as
    WITH t1 as

        (select
        post.publisher_id,
        post.post_id,
        post.impact_score as impact_score_by_post,
        d.impact_score_daily_summed as impact_score_daily,
        d.days_after_post,
        impact_score_daily / impact_score_by_post as percent_of_total,
        count(*) over (partition by post_id) as count_post_id
        from _impact_score_per_influencer_by_post_flna post
        JOIN _impact_score_per_influencer_per_day_flna d on d.publisherid = post.publisher_id and d.postid = post.post_id
        where d.days_after_post <= 7
        and impact_score_by_post > 0
        and impact_score_daily > 0
        and d.days_after_post >= 0
        and d.metric_date >= $daily_data_start_date_flna
        order by publisher_id, post_id, days_after_post),

      t2 as

        (select
        days_after_post,
        avg(percent_of_total) as percent_of_total_avg
        from t1
        where count_post_id = 8
        group by days_after_post)

    select days_after_post,
    percent_of_total_avg / (select sum(percent_of_total_avg) from t2) as percent_of_total
    from t2
    order by days_after_post;

-- set date diff param
set num_days = (select datediff(day, min(POST_DATETIME), max(POST_DATETIME)) from _impact_score_per_influencer_by_post_flna);


-- cross join so I expand to get days after post
-- take the cartesian of a sequence of days from the min to max post date,
-- then filter for where days between the post and the cartesian date is in between 0 and 30
-- then join on the general decay that I calculated
-- then multiply the decay by impact score, to get impact score by day, derived.
create or replace temporary table _influencer_impact_score_daily_derived_flna as
with t1 as
    (select *,
           datediff(day, POST_DATETIME, cross_join_date) as days_between
           from _impact_score_per_influencer_by_post_flna

    cross join (
            select
          dateadd(
            day,
            '-' || row_number() over (order by null),
            dateadd(day, '+1', (select max(to_date(POST_DATETIME)) from _impact_score_per_influencer_by_post_flna))
          ) as cross_join_date
        from table (generator(rowcount => ($num_days)))
        order by cross_join_date desc
        )
    where POST_DATETIME <= cross_join_date
    and days_between <= 7
    and days_between >= 0)

     select t1.cross_join_date as date,
            t1.PUBLISHER_NAME,
            t1.publisher_id,
            t1.post_id,
            t1.social_network,
            t1.post_type,
            decay.days_after_post,
            t1.hours_since_post,
            t1.days_since_post,
            decay.percent_of_total,
            t1.impact_score * decay.percent_of_total as impact_score_daily,
            t1.organic_influencer_impressions * decay.percent_of_total as organic_influencer_impressions_daily,
            t1.likes * decay.percent_of_total as likes_daily,
            t1.comments * decay.percent_of_total as comments_daily,
            t1.totalshares * decay.percent_of_total as totalshares_daily,
            t1.saves * decay.percent_of_total as saves_daily,
            t1.total_engagements * decay.percent_of_total as engagements_daily
    from t1
    left join _general_decay_curve_flna as decay on decay.days_after_post = t1.days_between;


-- union the daily data with the derived post data
create or replace temporary table _influencer_data_daily_flna as
select
     metric_date as date,
     publisherid as publisher_id,
     postid as post_id,
     POSTTYPE as post_type,
    SOCIALNETWORK as social_network,
    PUBLISHERNAME as publisher_name,
    impact_score_daily_summed as impact_score,
    likes_daily_summed as likes,
    comments_daily_summed as comments,
    totalshares_daily_summed as totalshares,
    saves_daily_summed as saves,
    total_engagements_daily_summed as engagements,
    networkimpressions_daily_summed as impressions,
    days_after_post,
    hours_since_post,
    days_since_post
 from _impact_score_per_influencer_per_day_flna
 where date >= $daily_data_start_date_flna

union all

select
       date,
       publisher_id,
       post_id,
       post_type,
       social_network,
       PUBLISHER_NAME,
       impact_score_daily as impact_score,
       likes_daily as likes,
       comments_daily as comments,
       totalshares_daily as totalshares,
       saves_daily as saves,
       engagements_daily as engagements,
       organic_influencer_impressions_daily as impressions,
       days_after_post,
       hours_since_post,
       days_since_post
from _influencer_impact_score_daily_derived_flna
where  date < $daily_data_start_date_flna;


-- create avg audience metrics table
create or replace temporary table _avg_audience_metrics_flna as
select network,
       avg(female) as avg_female_by_network,
       avg(percent_us) as avg_percent_us_by_network
from
    (select distinct
    PUBLISHERID,
    lower(network) as network,
    to_date(LASTUPDATED) as lastupdated,
    FEMALE,
                    case when AUDIENCECOUNTRY_TOP1_NAME = 'United States' then AUDIENCESHARE_TOP1
                        when AUDIENCECOUNTRY_TOP2_NAME = 'United States' then AUDIENCESHARE_TOP2
                        when AUDIENCECOUNTRY_TOP3_NAME = 'United States' then AUDIENCESHARE_TOP3
                        when AUDIENCECOUNTRY_TOP4_NAME = 'United States' then AUDIENCESHARE_TOP4
                        else 100 - (AUDIENCESHARE_TOP1 + AUDIENCESHARE_TOP2 + AUDIENCESHARE_TOP3 + AUDIENCESHARE_TOP4) end as percent_us
    from LAKE.CREATORIQ.AUDIENCE_FIRST_PARTY
    where publisherid in (select distinct publisher_id from _influencer_data_daily_flna)
    order by PUBLISHERID, LASTUPDATED)
group by network;


-- create avg audience metrics by publisher
create or replace temporary table _avg_audience_metrics_by_publisher_flna as
select PUBLISHERID,
       avg(female) as avg_female_by_publisher,
       avg(percent_us) as avg_percent_us_by_publisher
from
    (select distinct
    PUBLISHERID,
    lower(network) as network,
    to_date(LASTUPDATED) as lastupdated,
    FEMALE,
                    case when AUDIENCECOUNTRY_TOP1_NAME = 'United States' then AUDIENCESHARE_TOP1
                        when AUDIENCECOUNTRY_TOP2_NAME = 'United States' then AUDIENCESHARE_TOP2
                        when AUDIENCECOUNTRY_TOP3_NAME = 'United States' then AUDIENCESHARE_TOP3
                        when AUDIENCECOUNTRY_TOP4_NAME = 'United States' then AUDIENCESHARE_TOP4
                        else 100 - (AUDIENCESHARE_TOP1 + AUDIENCESHARE_TOP2 + AUDIENCESHARE_TOP3 + AUDIENCESHARE_TOP4) end as percent_us
    from LAKE.CREATORIQ.AUDIENCE_FIRST_PARTY
    where publisherid in (select distinct publisher_id from _influencer_data_daily_flna)
    order by PUBLISHERID, LASTUPDATED)
group by PUBLISHERID;


-- bring in audience metrics. A lot are null so we take the best case scenario.
-- First, we join on date and network. We forward fill between dates by publisher and network
-- then we fill null with avg by publisher
-- then we fill null with avg by network

create or replace temporary table _impact_score_with_modeled_metrics_flna as
select
date,
publisher_id,
post_id,
       post_type,
       social_network,
       publisher_name,
       impact_score as _impact_score,
       likes,
       comments,
       totalshares,
       saves,
       engagements,
       impressions,
       days_after_post,
       hours_since_post,
       days_since_post,

        coalesce(female, female_fill, avg_female_by_publisher, avg_female_by_network, (select avg(avg_female_by_network) from _avg_audience_metrics_flna)) as percent_female,
       coalesce(percent_us, percent_us_fill, avg_percent_us_by_publisher, avg_percent_us_by_network, (select avg(avg_percent_us_by_network) from _avg_audience_metrics_flna)) as percent_us

from

    (select i.*,
           a.FEMALE,
               case when AUDIENCECOUNTRY_TOP1_NAME = 'United States' then AUDIENCESHARE_TOP1
                when AUDIENCECOUNTRY_TOP2_NAME = 'United States' then AUDIENCESHARE_TOP2
                when AUDIENCECOUNTRY_TOP3_NAME = 'United States' then AUDIENCESHARE_TOP3
                when AUDIENCECOUNTRY_TOP4_NAME = 'United States' then AUDIENCESHARE_TOP4
                else 100 - (AUDIENCESHARE_TOP1 + AUDIENCESHARE_TOP2 + AUDIENCESHARE_TOP3 + AUDIENCESHARE_TOP4) end as percent_us,

            first_value(percent_us ignore nulls) over (partition by i.publisher_id, i.social_network order by i.publisher_id, i.date) as percent_us_fill,
            first_value(female ignore nulls) over (partition by i.publisher_id, i.social_network order by i.publisher_id, i.date) as female_fill,

            aam.avg_female_by_network,
           aam.avg_percent_us_by_network,
           aap.avg_female_by_publisher,
           aap.avg_percent_us_by_publisher

    from _influencer_data_daily_flna i
    left join LAKE.CREATORIQ.AUDIENCE_FIRST_PARTY a
        on a.PUBLISHERID = i.publisher_id
        and to_date(a.LASTUPDATED) = i.date
        and lower(a.network) = lower(i.social_network)
    left join _avg_audience_metrics_flna aam on lower(aam.network) = lower(i.social_network)
    left join _avg_audience_metrics_by_publisher_flna aap on aap.PUBLISHERID = i.publisher_id
    order by i.publisher_id, i.date);



-- final modeled table: multiply impact score by percent female and percent us
create or replace transient table reporting_media_base_prod.public.influencer_impact_score_daily_flna as
select date,
       publisher_id,
       publisher_name,
       post_id,
       post_type,
       social_network,
       days_after_post,
       hours_since_post,
       days_since_post,
       percent_us,
       percent_female,
       _impact_score * (percent_female/100) * (percent_us/100) as impact_score,
       likes,
       comments,
       totalshares,
       saves,
       engagements,
       impressions
       from _impact_score_with_modeled_metrics_flna;
