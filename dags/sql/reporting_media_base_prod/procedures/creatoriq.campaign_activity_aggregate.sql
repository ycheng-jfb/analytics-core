
----------------------------------------------------------------------------
-- one campaign per post (cleaning up duplicate posts in overlapping campaigns)

create or replace temporary table _all_posts_campaigns as
select distinct postid, campaignid, startdate, enddate
from lake.creatoriq.campaign_activity;

create or replace temporary table _order_by_end_date as
select *,
       row_number() over(partition by postid order by enddate desc) as rn
from _all_posts_campaigns;

create or replace temporary table _final_campaigns as
select distinct postid, campaignid
from _order_by_end_date
where rn = 1;

----------------------------------------------------------------------------
-- one authentication status per post (cleaning up when authentication drops off during patch periods)
-- hard-coding stories to always show as authenticated

create or replace temporary table _all_posts_auth as
select distinct postid, max(iff(posttype = 'story', 1, iff(linked is null,0,linked))) as max_linked, max(linkedat) as max_linkedat
from lake.creatoriq.campaign_activity
group by 1;

----------------------------------------------------------------------------
-- cleansed table with one authentication status and campaign per post

create or replace transient table reporting_media_base_prod.creatoriq.campaign_activity_cleansed as
select
        campaign_activity_key,
        id,
        ca.campaignid,
        socialnetwork,
        socialnetworkid,
        publisherid,
        username,
        type,
        ca.postid,
        score,
        earnedmedia,
        actualreach,
        networkreach,
        estimatedreach,
        reach,
        extendedreach,
        likes,
        comments,
        retweets,
        views,
        minuteswatched,
        postkey,
        linkid,
        status,
        approvedby,
        suspendedby,
        updatedby,
        posttype,
        totalshares,
        facebooklikes,
        facebookshares,
        facebookcomments,
        twittershares,
        googleplusshares,
        pinterestshares,
        notfound,
        publishername,
        campaignname,
        networkpublisherid,
        partnerid,
        logourl,
        startdate,
        enddate,
        flagshipproperty,
        averageviewduration,
        averageviewpercentage,
        subscribersgained,
        avgviewsperpost,
        subscriberslost,
        annotationclickthroughrate,
        annotationcloserate,
        monetizedplaybacks,
        grossrevenue,
        impressionbasedcpm,
        duration,
        actualimpressions,
        engagementrate,
        paidimpressions,
        paidreach,
        paidengagement,
        paidengagementrate,
        paidclicks,
        paidviews,
        paidlikes,
        paidcomments,
        paidtotalshares,
        paidsaves,
        combinedimpressions,
        combinedreach,
        combinedengagement,
        combinedclicks,
        combinedviews,
        combinedlikes,
        combinedcomments,
        combinedtotalshares,
        combinedsaves,
        combinedengagementrate,
        networkimpressions,
        estimatedimpressions,
        followersattimeofpost,
        fbigid,
        thumbnail,
        savedpicture,
        replies,
        exits,
        tapsforward,
        tapsback,
        swipeaways,
        posttitle,
        uniques,
        note,
        ftccompliant,
        swipeups,
        stickertaps,
        sentiment,
        manualreachflag,
        manualimpressionsflag,
        socialid,
        dislikes,
        saved,
        max_linked as linked,
        unlinked,
        linkbroken,
        verified,
        tokeninvaliddate,
        max_linkedat as linkedat,
        business,
        submitbydate,
        networkengagements,
        instagramshares,
        profilevisits,
        follows,
        campaignstatus,
        boardid,
        boardurl,
        boardscount,
        saves,
        isverifiedbysocialnetwork,
        datesubmitted,
        dateapproved,
        datesuspended,
        postlink,
        post,
        lastupdated,
        usernametitle,
        engagement,
        clicks,
        postclicks,
        totalreach,
        linkclicks,
        postconversionmetrics,
        impressions,
        socialmediavalue,
        estimatedreachflag,
        estimatedimpressionsflag,
        basemetric,
        basemetricvalue,
        closeups,
        peakstreamconcurrentviews,
        avgstreamconcurrentviews,
        subscribersattimeofpost,
        estimateduniquestreamviews,
        votes,
        requirementlinked,
        meta_type_1_hash,
        meta_type_2_hash,
        meta_from_datetime,
        meta_to_datetime,
        meta_is_current,
        meta_create_datetime,
        meta_update_datetime

from lake.creatoriq.campaign_activity ca
join _final_campaigns fc on ca.campaignid = fc.campaignid
    and ca.postid = fc.postid
join _all_posts_auth a on ca.postid = a.postid;

----------------------------------------------------------------------------
-- clean up multiple CIQ accounts per influencer (sxf us)

update reporting_media_base_prod.creatoriq.campaign_activity_cleansed
set publisherid = '2159379' where publisherid = '901710';

update reporting_media_base_prod.creatoriq.campaign_activity_cleansed
set publisherid = '1563992' where publisherid = '1342906';

update reporting_media_base_prod.creatoriq.campaign_activity_cleansed
set publisherid = '3185256' where publisherid = '1286352';

update reporting_media_base_prod.creatoriq.campaign_activity_cleansed
set publisherid = '3115605' where publisherid = '1842089';


----------------------------------------------------------------------------
-- create aggregate table of CIQ metrics

create or replace transient table reporting_media_base_prod.creatoriq.campaign_activity_aggregate as
select
    campaignid as campaign_id,
    campaignname as campaign_name,
    startdate as campaign_start_date,
    enddate as campaign_end_date,
    publisherid as publisher_id,
    publishername as publisher_name,
    linked as is_authenticated,
    linkedat as authenticated_datetime,
    isverifiedbysocialnetwork as verified_by_social_network,
    postid as post_id,
    socialnetwork as social_network,
    posttype as post_type,
    iff(social_network='Instagram' and post_type ='story'
        ,row_number() over(partition by campaign_id,publisher_id,post_type,cast(datesubmitted as date) order by datesubmitted asc)
        ,null) as ig_story_frame_sequence,
    datesubmitted as post_datetime,
    followersattimeofpost as followers_at_time_of_post,
    networkimpressions as impressions,
    networkreach as reach,
    likes as likes,
    comments as comments,
    views as video_views,
    saves as saves,
    totalshares as shares,
    replies as ig_story_replies,
    tapsforward as ig_story_taps_forward,
    tapsback as ig_story_taps_back,
    exits as ig_story_exits,
    dislikes as youtube_dislikes,
    subscribersgained as youtube_subscribers_gained,
    subscriberslost as youtube_subscribers_lost,
    duration as youtube_video_duration_seconds,
    averageviewduration as youtube_avg_view_duration_seconds,
    averageviewpercentage as youtube_avg_percentage_video_watched,
    minuteswatched as youtube_total_minutes_watched,
    coalesce(engagement,networkengagements) as total_engagements,
    engagementrate as engagement_rate,
    postlink as link_to_post,

    lastupdated as ciq_data_update_datetime,
    current_timestamp() as meta_create_datetime,
    current_timestamp() as meta_update_datetime

from reporting_media_base_prod.creatoriq.campaign_activity_cleansed
where meta_is_current=1;
