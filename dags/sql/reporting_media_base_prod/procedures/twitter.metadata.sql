-- get all ids for mapping
create or replace temporary table _id_scaffold as
select distinct a.account_id, c.campaign_id, ag.line_item_id, ad.promoted_tweet_id, ad.tweet_id
from lake_view.twitter.account_metadata a
left join lake_view.twitter.campaign_metadata c on a.account_id = c.account_id
left join lake_view.twitter.line_item_metadata ag on c.campaign_id = ag.campaign_id
left join lake_view.twitter.promoted_tweet_metadata ad on ag.line_item_id = ad.line_item_id;

-- the promoted tweet id is the unique id but we need to visualize the ad id in reporting
create or replace transient table reporting_media_base_prod.twitter.metadata as (
select distinct
    id.account_id,
    a.account_name,
    id.campaign_id,
    c.campaign_name,
    id.line_item_id as adgroup_id,
    l.line_item_name as adgroup_name,
    id.promoted_tweet_id as unique_ad_id,
    id.tweet_id as ad_id,
    t.name as ad_name,
    c.budget_optimization,
    l.product_type,
    l.objective,
    current_timestamp()::timestamp_ltz as meta_create_datetime,
    current_timestamp()::timestamp_ltz as meta_update_datetime
from _id_scaffold id
left join lake_view.twitter.account_metadata a on id.account_id = a.account_id
left join lake_view.twitter.campaign_metadata c on id.campaign_id = c.campaign_id
left join lake_view.twitter.line_item_metadata l on id.line_item_id = l.line_item_id
left join lake_view.twitter.promoted_tweet_metadata pt on id.promoted_tweet_id = pt.promoted_tweet_id
left join lake_view.twitter.tweet_metadata t on id.tweet_id = t.tweet_id
);
