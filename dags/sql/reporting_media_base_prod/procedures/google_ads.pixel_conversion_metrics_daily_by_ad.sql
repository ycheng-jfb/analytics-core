
create or replace transient table reporting_media_base_prod.google_ads.pmax_conversions as
select
    pmax.date,
    pmax.customer_id,
    am.account_name,
    pmax.campaign_id,
    am.campaign_name,
    to_number(split_part(pmax.conversion_action,'/',4)) as conversion_tracker_id,
    conversion_action_name,
    pmax.conversions,
    pmax.view_through_conversions,
    pmax.all_conversions,
    pmax.meta_update_datetime
from lake_view.google_ads.performance_max_conversion_by_campaign pmax
join lake_view.sharepoint.med_google_ads_conversion_mapping b on pmax.conversion_action = b.conversion_tracker_id
left join reporting_media_base_prod.google_ads.metadata am on pmax.campaign_id=am.campaign_id
where lower(advertising_channel_type) ='performance_max'
and (b.conversion_tracker_id is null
    or (
        pmax.date >= b.start_date
        and pmax.date <= b.end_date
        ));

create or replace temporary table _stg_pixel_conversion_metrics_daily_by_ad as
    select
        account_id,
        campaign_id,
        ad_id,
        pixel_conversion_id,
        pixel_conversion_name,
        date,
        all_conversions,
        conversions,
        view_through_conversions,
        current_timestamp() as meta_create_datetime,
        current_timestamp() as meta_update_datetime
    from (
        select
            external_customer_id as account_id,
            campaign_id,
            ad_id,
            a.conversion_tracker_id as pixel_conversion_id,
            pixel.pixel_conversion_name,
            date,
            sum(all_conversions) as all_conversions,
            sum(conversions) as conversions,
            sum(view_through_conversions) as view_through_conversions
        from reporting_media_base_prod.google_ads.ad_conversions a
        left join (
            select
                conversion_tracker_id as pixel_conversion_id,
                conversion_type_name as pixel_conversion_name,
                row_number() over(partition by pixel_conversion_id order by meta_update_datetime desc) as rn_pixel
            from reporting_media_base_prod.google_ads.ad_conversions
            ) as pixel on pixel.pixel_conversion_id = a.conversion_tracker_id and pixel.rn_pixel = 1
        group by 1,2,3,4,5,6
        union
        select
            customer_id as account_id,
            campaign_id as campaing_id,
            campaign_id as ad_id,
            conversion_tracker_id as pixel_conversion_id,
            pixel.pixel_conversion_name,
            date,
            sum(all_conversions) as all_conversions,
            sum(conversions) as conversions,
            sum(view_through_conversions) as view_through_conversions
        from reporting_media_base_prod.google_ads.pmax_conversions a
        left join (
            select
                conversion_tracker_id as pixel_conversion_id,
                conversion_action_name as pixel_conversion_name,
                row_number() over(partition by pixel_conversion_id order by meta_update_datetime desc) as rn_pixel
            from reporting_media_base_prod.google_ads.pmax_conversions
            ) as pixel on pixel.pixel_conversion_id = a.conversion_tracker_id and pixel.rn_pixel = 1
        group by 1,2,3,4,5,6
        );

set high_watermark_datetime = (select max(meta_update_datetime) from reporting_media_base_prod.google_ads.ad_conversions);

create or replace transient table reporting_media_base_prod.google_ads.pixel_conversion_metrics_daily_by_ad as
   select
        src.account_id,
        src.campaign_id,
        src.ad_id,
        src.date,

        -- leads
        sum(iff(pixel_conversion_name = 'lead', click_through_conversions, 0)) as pixel_lead_30x7_click,
        sum(iff(pixel_conversion_name = 'lead', view_through_conversions, 0)) as pixel_lead_30x7_view,
        sum(iff(pixel_conversion_name = 'lead 1x1', click_through_conversions, 0)) as pixel_lead_1x1_click,
        sum(iff(pixel_conversion_name = 'lead 1x1', view_through_conversions, 0)) as pixel_lead_1x1_view,
        sum(iff(pixel_conversion_name = 'lead 3x3', click_through_conversions, 0)) as pixel_lead_3x3_click,
        sum(iff(pixel_conversion_name = 'lead 3x3', view_through_conversions, 0)) as pixel_lead_3x3_view,
        sum(iff(pixel_conversion_name = 'lead 7x7', click_through_conversions, 0)) as pixel_lead_7x7_click,
        sum(iff(pixel_conversion_name = 'lead 7x7', view_through_conversions, 0)) as pixel_lead_7x7_view,
        sum(iff(pixel_conversion_name = 'lead 1x1x1', click_through_conversions, 0)) as pixel_lead_1x1x1_click,
        sum(iff(pixel_conversion_name = 'lead 1x1x1', view_through_conversions, 0)) as pixel_lead_1x1x1_view,
        sum(iff(pixel_conversion_name = 'lead 30x7x1', click_through_conversions, 0)) as pixel_lead_30x7x1_click,
        sum(iff(pixel_conversion_name = 'lead 30x7x1', view_through_conversions, 0)) as pixel_lead_30x7x1_view,
        sum(iff(pixel_conversion_name = 'lead 7x3x1', click_through_conversions, 0)) as pixel_lead_7x3x1_click,
        sum(iff(pixel_conversion_name = 'lead 7x3x1', view_through_conversions, 0)) as pixel_lead_7x3x1_view,
        sum(iff(pixel_conversion_name = 'lead 7x1', click_through_conversions, 0)) as pixel_lead_7x1_click,
        sum(iff(pixel_conversion_name = 'lead 7x1', view_through_conversions, 0)) as pixel_lead_7x1_view,
        sum(iff(pixel_conversion_name = 'lead 14x1', click_through_conversions, 0)) as pixel_lead_14x1_click,
        sum(iff(pixel_conversion_name = 'lead 14x1', view_through_conversions, 0)) as pixel_lead_14x1_view,
        sum(iff(pixel_conversion_name = 'lead 30dc', click_through_conversions, 0)) as pixel_lead_30dc_click,
        sum(iff(pixel_conversion_name = 'lead 1dc', click_through_conversions, 0)) as pixel_lead_1dc_click,

        -- vips
        sum(iff(pixel_conversion_name = 'vip', click_through_conversions, 0)) as pixel_vip_30x7_click,
        sum(iff(pixel_conversion_name = 'vip', view_through_conversions, 0)) as pixel_vip_30x7_view,
        sum(iff(pixel_conversion_name = 'vip 1x1', click_through_conversions, 0)) as pixel_vip_1x1_click,
        sum(iff(pixel_conversion_name = 'vip 1x1', view_through_conversions, 0)) as pixel_vip_1x1_view,
        sum(iff(pixel_conversion_name = 'vip 3x3', click_through_conversions, 0)) as pixel_vip_3x3_click,
        sum(iff(pixel_conversion_name = 'vip 3x3', view_through_conversions, 0)) as pixel_vip_3x3_view,
        sum(iff(pixel_conversion_name = 'vip 7x7', click_through_conversions, 0)) as pixel_vip_7x7_click,
        sum(iff(pixel_conversion_name = 'vip 7x7', view_through_conversions, 0)) as pixel_vip_7x7_view,
        sum(iff(pixel_conversion_name = 'vip 1x1x1', click_through_conversions, 0)) as pixel_vip_1x1x1_click,
        sum(iff(pixel_conversion_name = 'vip 1x1x1', view_through_conversions, 0)) as pixel_vip_1x1x1_view,
        sum(iff(pixel_conversion_name = 'vip 30x7x1', click_through_conversions, 0)) as pixel_vip_30x7x1_click,
        sum(iff(pixel_conversion_name = 'vip 30x7x1', view_through_conversions, 0)) as pixel_vip_30x7x1_view,
        sum(iff(pixel_conversion_name = 'vip 7x3x1', click_through_conversions, 0)) as pixel_vip_7x3x1_click,
        sum(iff(pixel_conversion_name = 'vip 7x3x1', view_through_conversions, 0)) as pixel_vip_7x3x1_view,
        sum(iff(pixel_conversion_name = 'vip 7x1', click_through_conversions, 0)) as pixel_vip_7x1_click,
        sum(iff(pixel_conversion_name = 'vip 7x1', view_through_conversions, 0)) as pixel_vip_7x1_view,
        sum(iff(pixel_conversion_name = 'vip 14x1', click_through_conversions, 0)) as pixel_vip_14x1_click,
        sum(iff(pixel_conversion_name = 'vip 14x1', view_through_conversions, 0)) as pixel_vip_14x1_view,
        sum(iff(pixel_conversion_name = 'vip 30dc', click_through_conversions, 0)) as pixel_vip_30dc_click,
        sum(iff(pixel_conversion_name = 'vip 1dc', click_through_conversions, 0)) as pixel_vip_1dc_click,

        $high_watermark_datetime as pixel_conversion_meta_update_datetime,
        current_timestamp() as meta_create_datetime,
        current_timestamp() as meta_update_datetime
    from (
        select
            ad.account_id,
            ad.campaign_id,
            ad.ad_id,
            ad.date,
            case
                when pixel_conversion_name ilike '%vip' then 'vip 30x7'
                when pixel_conversion_name ilike '%vip 1x1' then 'vip 1x1'
                when pixel_conversion_name ilike '%vip 3x3' then 'vip 3x3'
                when pixel_conversion_name ilike '%vip 30dc' then 'vip 30dc'
                when pixel_conversion_name ilike '%vip 1dc' then 'vip 1dc'
                when pixel_conversion_name ilike '%vip 7x7' then 'vip 7x7'
                when pixel_conversion_name ilike '%vip 30x7x1' then 'vip 30x7x1'
                when pixel_conversion_name ilike '%vip 7x3x1' then 'vip 7x3x1'
                when pixel_conversion_name ilike '%vip 1x1x1' then 'vip 1x1x1'
                when pixel_conversion_name ilike '%vip 7x1' then 'vip 7x1'
                when pixel_conversion_name ilike '%vip 14x1' then 'vip 14x1'

                when pixel_conversion_name ilike '%lead' then 'lead 30x7'
                when pixel_conversion_name ilike '%lead 1x1' then 'lead 1x1'
                when pixel_conversion_name ilike '%lead 3x3' then 'lead 3x3'
                when pixel_conversion_name ilike '%lead 30dc' then 'lead 30dc'
                when pixel_conversion_name ilike '%lead 1dc' then 'lead 1dc'
                when pixel_conversion_name ilike '%lead 7x7' then 'lead 7x7'
                when pixel_conversion_name ilike '%lead 30x7x1' then 'lead 30x7x1'
                when pixel_conversion_name ilike '%lead 7x3x1' then 'lead 7x3x1'
                when pixel_conversion_name ilike '%lead 1x1x1' then 'lead 1x1x1'
                when pixel_conversion_name ilike '%lead 7x1' then 'lead 7x1'
                when pixel_conversion_name ilike '%lead 14x1' then 'lead 14x1'

                else 'exclude'
            end as pixel_conversion_name,

            sum(iff((ad.account_id = '9347951239' and date < '2019-03-12'
                    or ad.account_id = '2934744006' and date < '2019-07-30'),
                    ad.all_conversions - ad.view_through_conversions, ad.all_conversions)) as click_through_conversions,

            sum(ad.view_through_conversions) as view_through_conversions

        from _stg_pixel_conversion_metrics_daily_by_ad ad
       group by 1,2,3,4,5) as src
    group by 1,2,3,4;
