
set high_watermark_datetime = (select max(meta_update_datetime) as datetime from lake_view.pinterest.spend_ad1);

create or replace transient table reporting_media_base_prod.pinterest.spend_metrics_daily_by_ad as
select
        account_id,
        campaign_id,
        adgroup_id,
        ad_id,
        date,

        spend_account_currency,
        spend_usd,
        spend_local,

        impressions,
        clicks,

        pixel_lead_click_1d,
        pixel_lead_view_1d,
        pixel_add_to_cart_click_1d,
        pixel_add_to_cart_view_1d,
        pixel_vip_click_1d,
        pixel_vip_view_1d,
        pixel_purchase_click_1d,
        pixel_purchase_view_1d,

        pixel_lead_click_7d,
        pixel_lead_view_7d,
        pixel_add_to_cart_click_7d,
        pixel_add_to_cart_view_7d,
        pixel_vip_click_7d,
        pixel_vip_view_7d,
        pixel_purchase_click_7d,
        pixel_purchase_view_7d,

        pixel_lead_click_30d,
        pixel_lead_view_30d,
        pixel_add_to_cart_click_30d,
        pixel_add_to_cart_view_30d,
        pixel_vip_click_30d,
        pixel_vip_view_30d,
        pixel_purchase_click_30d,
        pixel_purchase_view_30d,

        $high_watermark_datetime as pixel_meta_update_datetime,
        current_timestamp()::timestamp_ltz as meta_create_datetime,
        current_timestamp()::timestamp_ltz as meta_update_datetime

    from (
        select distinct
        a1.advertiser_id as account_id,
        a1.pin_promotion_campaign_id as campaign_id,
        a1.pin_promotion_ad_group_id as adgroup_id,
        a1.pin_promotion_id as ad_id,
        a1.date,

        sum(coalesce(a1.spend_in_micro_dollar/1000000,0)) as spend_account_currency,
        sum((coalesce(a1.spend_in_micro_dollar/1000000,0))*coalesce(lkpusd.exchange_rate,1)) as spend_usd,
        sum((coalesce(a1.spend_in_micro_dollar/1000000,0))*coalesce(lkplocal.exchange_rate,1)) as spend_local,

        sum(coalesce(a1.impression_1,0)) as impressions,
        sum(coalesce(a1.clickthrough_1,0)) as clicks,

        sum(coalesce(a1.total_click_lead,0)) as pixel_lead_click_1d,
        sum(coalesce(a1.total_engagement_lead,0)+coalesce(a1.total_view_lead,0)) as pixel_lead_view_1d,
        sum(coalesce(a1.total_click_add_to_cart,0)) as pixel_add_to_cart_click_1d,
        sum(coalesce(a1.total_engagement_add_to_cart,0)+coalesce(a1.total_view_add_to_cart,0)) as pixel_add_to_cart_view_1d,
        sum(coalesce(a1.total_click_signup,0)) as pixel_vip_click_1d,
        sum(coalesce(a1.total_view_signup,0)+coalesce(a1.total_engagement_signup,0)) as pixel_vip_view_1d,
        sum(coalesce(a1.total_click_checkout,0)) as pixel_purchase_click_1d,
        sum(coalesce(a1.total_engagement_checkout,0)+coalesce(a1.total_view_checkout,0)) as pixel_purchase_view_1d,

        sum(coalesce(a7.total_click_lead,0)) as pixel_lead_click_7d,
        sum(coalesce(a7.total_engagement_lead,0)+coalesce(a7.total_view_lead,0)) as pixel_lead_view_7d,
        sum(coalesce(a7.total_click_add_to_cart,0)) as pixel_add_to_cart_click_7d,
        sum(coalesce(a7.total_engagement_add_to_cart,0)+coalesce(a7.total_view_add_to_cart,0)) as pixel_add_to_cart_view_7d,
        sum(coalesce(a7.total_click_signup,0)) as pixel_vip_click_7d,
        sum(coalesce(a7.total_view_signup,0)+coalesce(a7.total_engagement_signup,0)) as pixel_vip_view_7d,
        sum(coalesce(a7.total_click_checkout,0)) as pixel_purchase_click_7d,
        sum(coalesce(a7.total_engagement_checkout,0)+coalesce(a7.total_view_checkout,0)) as pixel_purchase_view_7d,

        sum(coalesce(a30.total_click_lead,0)) as pixel_lead_click_30d,
        sum(coalesce(a30.total_engagement_lead,0)+coalesce(a30.total_view_lead,0)) as pixel_lead_view_30d,
        sum(coalesce(a30.total_click_add_to_cart,0)) as pixel_add_to_cart_click_30d,
        sum(coalesce(a30.total_engagement_add_to_cart,0)+coalesce(a30.total_view_add_to_cart,0)) as pixel_add_to_cart_view_30d,
        sum(coalesce(a30.total_click_signup,0)) as pixel_vip_click_30d,
        sum(coalesce(a30.total_view_signup,0)+coalesce(a30.total_engagement_signup,0)) as pixel_vip_view_30d,
        sum(coalesce(a30.total_click_checkout,0)) as pixel_purchase_click_30d,
        sum(coalesce(a30.total_engagement_checkout,0)+coalesce(a30.total_view_checkout,0)) as pixel_purchase_view_30d

        from lake_view.pinterest.spend_ad1 a1
        full join (select distinct
                        pin_promotion_id,
                        date,
                        sum(total_click_lead) as total_click_lead,
                        sum(total_view_lead) as total_view_lead,
                        sum(total_engagement_lead) as total_engagement_lead,
                        sum(total_click_add_to_cart) as total_click_add_to_cart,
                        sum(total_view_add_to_cart) as total_view_add_to_cart,
                        sum(total_engagement_add_to_cart) as total_engagement_add_to_cart,
                        sum(total_click_signup) as total_click_signup,
                        sum(total_view_signup) as total_view_signup,
                        sum(total_engagement_signup) as total_engagement_signup,
                        sum(total_click_checkout) as total_click_checkout,
                        sum(total_view_checkout) as total_view_checkout,
                        sum(total_engagement_checkout) as total_engagement_checkout
                    from lake_view.pinterest.spend_ad7
                    group by 1,2) a7 on a1.pin_promotion_id = a7.pin_promotion_id and a1.date = a7.date
        full join (select distinct
                        pin_promotion_id,
                        date,
                        sum(total_click_lead) as total_click_lead,
                        sum(total_view_lead) as total_view_lead,
                        sum(total_engagement_lead) as total_engagement_lead,
                        sum(total_click_add_to_cart) as total_click_add_to_cart,
                        sum(total_view_add_to_cart) as total_view_add_to_cart,
                        sum(total_engagement_add_to_cart) as total_engagement_add_to_cart,
                        sum(total_click_signup) as total_click_signup,
                        sum(total_view_signup) as total_view_signup,
                        sum(total_engagement_signup) as total_engagement_signup,
                        sum(total_click_checkout) as total_click_checkout,
                        sum(total_view_checkout) as total_view_checkout,
                        sum(total_engagement_checkout) as total_engagement_checkout
                    from lake_view.pinterest.spend_ad30
                    group by 1,2) a30 on a1.pin_promotion_id = a30.pin_promotion_id and a1.date = a30.date

        left join lake_view.sharepoint.med_account_mapping_media am on lower(am.source_id) = lower(a1.advertiser_id)
                    and am.source ilike '%pinterest%'
        left join edw_prod.data_model.dim_store st on st.store_id = am.store_id
        left join edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
                    and a1.date = lkpusd.rate_date_pst
                    and lkpusd.dest_currency = 'USD'
        left join edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
                    and a1.date = lkplocal.rate_date_pst
                    and lkplocal.dest_currency = st.store_currency
        group by 1,2,3,4,5);
