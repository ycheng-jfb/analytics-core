-- by post
set data_latest_refresh = (select least((select max(update_datetime_utc) from reporting_media_base_prod.influencers.influencer_attribution_content_quality_score_by_post_sxna),
                    (select max(update_datetime_utc) from reporting_media_base_prod.influencers.influencer_attribution_content_quality_score_by_post_flna)));


create or replace transient table reporting_media_base_prod.influencers.influencer_attribution_dynamic_hdyh_by_post as
select i.*, channel_tier
from (
      select 'Savage X' as store_brand_name,
      'SX' as store_brand_abbr,
      'NA' as region,
      influencer_cleaned_name as influencer_name,
      media_partner_id,
      p.post_id,
      p.publisher_id,
      p.post_date,
      p.post_type,
      p.social_network,
      p.link_to_post,
      p.impact_score,
      p.likes,
      p.comments,
      p.totalshares as shares,
      p.saves,
      p.organic_influencer_impressions as impressions,
      p.followers_at_time_of_post,
      p.reach,
      p.ig_story_replies,
      p.ig_story_taps_back,
      p.ig_story_taps_forward,
      p.attributed_conversions_redistributed as vips_attributed,
      content_quality_score,
      $data_latest_refresh as data_latest_refresh
         from reporting_media_base_prod.influencers.influencer_attribution_content_quality_score_by_post_sxna p
         join lake_view.sharepoint.med_influencer_mapping m on m.ciq_publisher_id = p.publisher_id
    and m.store_name_abbr = 'sx'
    union
    select 'Fabletics' as store_brand_name,
    'FL' as store_brand_abbr,
    'NA' as region,
    influencer_cleaned_name as influencer_name,
    media_partner_id,
    p.post_id,
    p.publisher_id,
    p.post_date,
    p.post_type,
    p.social_network,
    p.link_to_post,
    p.impact_score,
    p.likes,
    p.comments,
    p.totalshares as shares,
    p.saves,
    p.organic_influencer_impressions as impressions,
    p.followers_at_time_of_post,
    p.reach,
    p.ig_story_replies,
    p.ig_story_taps_back,
    p.ig_story_taps_forward,
    p.attributed_conversions_redistributed as vips_attributed,
    content_quality_score,
    $data_latest_refresh as data_latest_refresh
    from reporting_media_base_prod.influencers.influencer_attribution_content_quality_score_by_post_flna p
         join lake_view.sharepoint.med_influencer_mapping m on m.ciq_publisher_id = p.publisher_id
    and m.store_name_abbr = 'fl'
    ) i
left join (select distinct media_partner_id, channel_tier, business_unit
            from reporting_media_prod.influencers.daily_performance_by_influencer) r on lower(cast(i.media_partner_id as varchar)) = r.media_partner_id
                    and i.store_brand_name = r.business_unit;

----------------------------------------------------------------------------
-- by influencer


set data_latest_refresh = (select least((select max(update_datetime_utc) from reporting_media_base_prod.influencers.influencer_attribution_cac_sxna),
                    (select max(update_datetime_utc) from reporting_media_base_prod.influencers.influencer_attribution_cac_sxna),
                    (select max(update_datetime_utc) from reporting_media_base_prod.influencers.influencer_attribution_calibration_scores_flna),
                    (select max(update_datetime_utc) from reporting_media_base_prod.influencers.influencer_attribution_cac_flna)));


create or replace temporary table _influencer_organic_performance as
select i.*, r2.channel_tier
from (
    select 'Savage X' as store_brand_name,
           'SX' as store_brand_abbr,
           'NA' as region,
           c.media_partner_id,
           c.publisher_id,
           cast(date as date) as date,
           is_authenticated,
           attributed_conversions_redistributed as vips_attributed,
           influencer_cleaned_name,
           upfront_spend,
           cc.hdyh_selections_final as total_responses,
           c.impact_score as total_impact_score,
           cc.most_recent_calibration_date,
           cc.calibration_rate
    from reporting_media_base_prod.influencers.influencer_attribution_cac_sxna c
    join lake_view.sharepoint.med_influencer_mapping m on c.media_partner_id = m.media_partner_id
            and m.store_name_abbr = 'sx'
    left join reporting_media_base_prod.influencers.influencer_attribution_calibration_scores_sxna cc on c.media_partner_id = cc.media_partner_id
                and cc.calibration_date = cc.most_recent_calibration_date
    union
    select 'Fabletics' as store_brand_name,
           'FL' as store_brand_abbr,
           'NA' as region,
           c.media_partner_id,
           c.publisher_id,
           cast(date as date) as date,
           1 as is_authenticated,
           attributed_conversions_redistributed as vips_attributed,
           influencer_cleaned_name,
           upfront_spend,
           cc.hdyh_selections_final as total_responses,
           c.impact_score as total_impact_score,
           cc.most_recent_calibration_date,
           cc.calibration_rate
    from reporting_media_base_prod.influencers.influencer_attribution_cac_flna c
    join lake_view.sharepoint.med_influencer_mapping m on c.media_partner_id = m.media_partner_id
            and m.store_name_abbr = 'fl'
    left join reporting_media_base_prod.influencers.influencer_attribution_calibration_scores_flna cc on c.media_partner_id = cc.media_partner_id
            and cc.calibration_date = cc.most_recent_calibration_date
    ) i
left join (select distinct media_partner_id, channel_tier, business_unit
            from reporting_media_prod.influencers.daily_performance_by_influencer) r2 on lower(i.media_partner_id) = r2.media_partner_id
                    and i.store_brand_name = r2.business_unit;

-- dynamic hdyh publisher ID combos
create or replace temporary table _mpid_publisher_id as
select distinct store_brand_name, media_partner_id, publisher_id, influencer_cleaned_name, channel_tier, is_authenticated, most_recent_calibration_date
from _influencer_organic_performance;

create or replace temporary table _influencer_paid_performance as
select business_unit as store_brand_name,
       store_brand_abbr,
       region,
       o.media_partner_id,
       i.channel_tier,
       date,

       sum(organic_clicks) as organic_clicks,
       sum(click_through_leads) as click_leads_raw,
       sum(hdyh_leads_raw) as hdyh_leads_raw,
       sum(click_through_leads+hdyh_leads_raw) as total_leads_raw,

       sum(click_through_vips_from_leads_30d+click_through_vips_same_session_not_from_lead_pool) as click_vips_raw,
       sum(hdyh_vips_raw) as hdyh_vips_raw,
       sum(click_through_vips_from_leads_30d+click_through_vips_same_session_not_from_lead_pool+hdyh_vips_raw) as total_vips_raw,

       sum(fbig_spend) as facebook_spend,
       sum(fbig_pixel_vips) as facebook_pixel_vips,
       sum(youtube_spend) as youtube_spend,
       sum(youtube_pixel_vips) as youtube_pixel_vips,
       sum(pinterest_spend) as pinterest_spend,
       sum(pinterest_pixel_vips) as pinterest_pixel_vips,
       sum(snapchat_spend) as snapchat_spend,
       sum(snapchat_pixel_vips) as snapchat_pixel_vips,
       sum(tiktok_spend) as tiktok_spend,
       sum(tiktok_pixel_vips) as tiktok_pixel_vips
from reporting_media_prod.influencers.daily_performance_by_influencer i
join _mpid_publisher_id o on lower(o.media_partner_id) = i.media_partner_id -- only include data for influencers in dynamic hdyh
where store_brand_abbr in ('FL','SX')
    and region = 'NA'
group by business_unit, store_brand_abbr, region, o.media_partner_id, i.channel_tier,date;


-- date / mpid combos
create or replace temporary table _date_combos as
select store_brand_name, store_brand_abbr, region, media_partner_id, date
from _influencer_organic_performance
union
select store_brand_name, store_brand_abbr, region, media_partner_id, date
from _influencer_paid_performance ;


create or replace temporary table _source_influencer_data as
select dc.store_brand_name,
       dc.store_brand_abbr,
       dc.region,
       dc.media_partner_id,
       dc.date,
       m.influencer_cleaned_name,
       m.channel_tier,
       m.publisher_id,

       -- organic
       m.is_authenticated,
       vips_attributed,
       upfront_spend,
       total_responses,
       total_impact_score,
       m.most_recent_calibration_date,
       calibration_rate,

       --paid
       organic_clicks,
       click_leads_raw,
       hdyh_leads_raw,
       total_leads_raw,

       click_vips_raw,
       hdyh_vips_raw,
       total_vips_raw,

       facebook_spend,
       facebook_pixel_vips,
       youtube_spend,
       youtube_pixel_vips,
       pinterest_spend,
       pinterest_pixel_vips,
       snapchat_spend,
       snapchat_pixel_vips,
       tiktok_spend,
       tiktok_pixel_vips,
        $data_latest_refresh as data_latest_refresh

from _date_combos dc
join _mpid_publisher_id m on dc.media_partner_id = m.media_partner_id
    and dc.store_brand_name = m.store_brand_name
left join _influencer_organic_performance o on o.store_brand_name = dc.store_brand_name
   and o.store_brand_abbr = dc.store_brand_abbr
   and o.region = dc.region
   and o.media_partner_id = dc.media_partner_id
   and o.date = dc.date
left join _influencer_paid_performance p on p.store_brand_name = dc.store_brand_name
   and p.store_brand_abbr = dc.store_brand_abbr
   and p.region = dc.region
   and p.media_partner_id = dc.media_partner_id
   and p.date = dc.date;


create or replace temporary table _average_scaling as
select store_brand_name,
       media_partner_id,
       sum(vips_attributed) as agg_vips_attributed,
       sum(click_vips_raw) as agg_click_vips_raw,
       sum(hdyh_vips_raw) as agg_hdyh_vips_raw,
       sum(total_vips_raw) as agg_vips_raw,
       agg_vips_attributed/iff(agg_vips_raw=0,null,agg_vips_raw) as avg_scaling_ratio,
       agg_hdyh_vips_raw/iff(agg_vips_raw=0,null,agg_vips_raw) as avg_pct_vips_hdyh,
       1-avg_pct_vips_hdyh as avg_pct_vips_click
from _source_influencer_data
group by 1,2;


create or replace transient table reporting_media_base_prod.influencers.influencer_attribution_dynamic_hdyh_by_influencer as
select s.*,

-- overall
    avg_scaling_ratio,
    vips_attributed/iff(total_vips_raw=0,null,total_vips_raw) as scaling_factor,
    total_leads_raw*iff(scaling_factor is null,avg_scaling_ratio,scaling_factor) as leads_attributed,

-- click
    click_leads_raw/iff(total_leads_raw=0,null,total_leads_raw) as leads_pct_click,
    click_leads_raw*iff(scaling_factor is null,avg_scaling_ratio,scaling_factor) as click_leads_attributed,
    click_vips_raw/iff(total_vips_raw=0,null,total_vips_raw) as vips_pct_click,
    click_vips_raw*scaling_factor as click_vips_attributed_ratio,
    iff(total_vips_raw=0 or total_vips_raw is null,avg_pct_vips_click*vips_attributed,click_vips_attributed_ratio) as click_vips_attributed,

-- hdyh
    hdyh_leads_raw/iff(total_leads_raw=0,null,total_leads_raw) as leads_pct_hdyh,
    hdyh_leads_raw*iff(scaling_factor is null,avg_scaling_ratio,scaling_factor) as hdyh_leads_attributed,
    hdyh_vips_raw/iff(total_vips_raw=0,null,total_vips_raw) as vips_pct_hdyh,
    hdyh_vips_raw*scaling_factor as hdyh_vips_attributed_ratio,
    iff(total_vips_raw=0 or total_vips_raw is null,avg_pct_vips_hdyh*vips_attributed,hdyh_vips_attributed_ratio) as hdyh_vips_attributed

from _source_influencer_data s
left join _average_scaling a on a.store_brand_name = s.store_brand_name
    and a.media_partner_id = s.media_partner_id
    and a.avg_scaling_ratio is not null;

----------------------------------------------------------------------------
-- calibration over time by influencer

set data_latest_refresh = (select least((select max(update_datetime_utc) from reporting_media_base_prod.influencers.influencer_attribution_calibration_with_spend_and_cac_sxna),
                    (select max(update_datetime_utc) from reporting_media_base_prod.influencers.influencer_attribution_calibration_with_spend_and_cac_flna)));

create or replace transient table reporting_media_base_prod.influencers.influencer_attribution_dynamic_hdyh_calibration_over_time as
select i.*, r.channel_tier as influencer_classification
from (
    select
        'Savage X' as store_brand_name,
        'SX' as store_brand_abbr,
        'NA' as region,
        influencer_cleaned_name as influencer_name,
        c.media_partner_id,
        ciq_publisher_id as publisher_id,
        is_authenticated,
        date,
        impact_score,
        upfront_spend,
        attributed_conversions_redistributed as attributed_vips,
        cac,
        calibration_rate,
        calibration_date,
        most_recent_calibration_date,
        $data_latest_refresh as data_latest_refresh
    from reporting_media_base_prod.influencers.influencer_attribution_calibration_with_spend_and_cac_sxna c
    join lake_view.sharepoint.med_influencer_mapping m on c.media_partner_id = m.media_partner_id
            and m.store_name_abbr = 'sx'
    union
    select
        'Fabletics' as store_brand_name,
        'FL' as store_brand_abbr,
        'NA' as region,
        influencer_cleaned_name as influencer_name,
        c.media_partner_id,
        ciq_publisher_id as publisher_id,
        1 as is_authenticated,
        date,
        impact_score,
        upfront_spend,
        attributed_conversions_redistributed as attributed_vips,
        cac,
        calibration_rate,
        calibration_date,
        most_recent_calibration_date,
        $data_latest_refresh as data_latest_refresh
    from reporting_media_base_prod.influencers.influencer_attribution_calibration_with_spend_and_cac_flna c
    join lake_view.sharepoint.med_influencer_mapping m on c.media_partner_id = m.media_partner_id
            and m.store_name_abbr = 'fl') i
left join (select distinct media_partner_id, channel_tier, business_unit
            from reporting_media_prod.influencers.daily_performance_by_influencer) r on lower(i.media_partner_id) = r.media_partner_id
                    and i.store_brand_name = r.business_unit;
