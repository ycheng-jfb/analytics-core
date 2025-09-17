-- pinterest optimization dataset on conversion date
create or replace transient table reporting_media_prod.pinterest.pinterest_optimization_dataset_on_conversion_date as
select
        pin.*,

        date,

        tfee_pct_of_media_spend as vendor_fees,
        spend_account_currency,
        spend_account_currency * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_account_currency,
        spend_usd,
        spend_usd * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_usd,
        spend_local,
        spend_local * (1 + ifnull(vendor_fees,0)) as spend_with_vendor_fees_local,

        impressions,
        clicks,

        -- lead / atc re-mapping

        -- lead
       CASE
                WHEN store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_add_to_cart_click_1d
                WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_add_to_cart_click_1d
                ELSE pixel_lead_click_1d
            END as pixel_lead_click_1d,

            CASE
                WHEN store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_add_to_cart_view_1d
                WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_add_to_cart_view_1d
                ELSE pixel_lead_view_1d
            END as pixel_lead_view_1d,

            CASE
                WHEN store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_add_to_cart_click_7d
                WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_add_to_cart_click_7d
                ELSE pixel_lead_click_7d
            END as pixel_lead_click_7d,

             CASE
                WHEN store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_add_to_cart_view_7d
                WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_add_to_cart_view_7d
                ELSE pixel_lead_view_7d
            END as pixel_lead_view_7d,

            CASE
                WHEN store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_add_to_cart_click_30d
                WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_add_to_cart_click_30d
                ELSE pixel_lead_click_30d
            END as pixel_lead_click_30d,

            CASE
                WHEN store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_add_to_cart_view_30d
                WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_add_to_cart_view_30d
                ELSE pixel_lead_view_30d
            END as pixel_lead_view_30d,

        -- atc
         CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_lead_click_1d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_lead_click_1d
            ELSE pixel_add_to_cart_click_1d
            END as pixel_atc_click_1d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_lead_view_1d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_lead_view_1d
            ELSE pixel_add_to_cart_view_1d
            END as pixel_atc_view_1d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_lead_click_7d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_lead_click_7d
            ELSE pixel_add_to_cart_click_7d
            END as pixel_atc_click_7d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_lead_view_7d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_lead_view_7d
            ELSE pixel_add_to_cart_view_7d
            END as pixel_atc_view_7d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_lead_click_30d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_lead_click_30d
            ELSE pixel_add_to_cart_click_30d
            END as pixel_atc_click_30d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_lead_view_30d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_lead_view_30d
            ELSE pixel_add_to_cart_view_30d
            END as pixel_atc_view_30d,

        -- vip / purchase re-mapping

        -- purchase
        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_vip_click_1d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_vip_click_1d
            ELSE pixel_purchase_click_1d
            END as pixel_purchase_click_1d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_vip_view_1d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_vip_view_1d
            ELSE pixel_purchase_view_1d
            END as pixel_purchase_view_1d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_vip_click_7d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_vip_click_7d
            ELSE pixel_purchase_click_7d
            END as pixel_purchase_click_7d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_vip_view_7d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_vip_view_7d
            ELSE pixel_purchase_view_7d
            END as pixel_purchase_view_7d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_vip_click_30d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_vip_click_30d
            ELSE pixel_purchase_click_30d
            END as pixel_purchase_click_30d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_vip_view_30d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_vip_view_30d
            ELSE pixel_purchase_view_30d
            END as pixel_purchase_view_30d,

        -- vip
        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_purchase_click_1d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_purchase_click_1d
            ELSE pixel_vip_click_1d
            END as pixel_vip_click_1d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_purchase_view_1d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_purchase_view_1d
            ELSE pixel_vip_view_1d
            END as pixel_vip_view_1d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_purchase_click_7d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_purchase_click_7d
            ELSE pixel_vip_click_7d
            END as pixel_vip_click_7d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_purchase_view_7d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_purchase_view_7d
            ELSE pixel_vip_view_7d
            END as pixel_vip_view_7d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_purchase_click_30d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_purchase_click_30d
            ELSE pixel_vip_click_30d
            END as pixel_vip_click_30d,

        CASE
            WHEN store_brand_name in ('Fabletics', 'Fabletics Men', 'Fabletics Scrubs', 'Yitty') and region = 'NA' and date >= '2024-10-10' THEN pixel_purchase_view_30d
            WHEN store_brand_name in ('Savage X') and region = 'NA' and date >= '2024-12-19' THEN pixel_purchase_view_30d
            ELSE pixel_vip_view_30d
            END as pixel_vip_view_30d,

        pix.pixel_meta_update_datetime,
        current_timestamp()::timestamp_ltz as process_meta_update_datetime

from reporting_media_base_prod.pinterest.spend_metrics_daily_by_ad_on_conversion_date pix
left join reporting_media_base_prod.pinterest.vw_pinterest_naming_convention pin on pin.ad_id = pix.ad_id
left join lake_view.sharepoint.med_vendor_fees_media v
    on (case when lower(v.identifier) = 'campaign' and pin.campaign_name ilike '%' || lower(v.identifier_value) || '%'
            then cast(pix.account_id as varchar) = v.account_id
        when lower(v.identifier) = 'adset' and pin.adgroup_name ilike '%' || lower(v.identifier_value) || '%'
            then cast(pix.account_id as varchar) = v.account_id end)
    and date between v.start_date_include and v.end_date_exclude - 1
    and lower(v.channel) = 'pinterest';

-- new pinterest optimization datasource that combines data attributed to spend and conv date
create or replace transient table reporting_media_prod.pinterest.pinterest_optimization_dataset_conv_spend_date as
select 'Spend Date' as date_conversions_attributed, *
from reporting_media_prod.pinterest.pinterest_optimization_dataset
union all
select 'Conversion Date' as date_conversions_attributed, *
from reporting_media_prod.pinterest.pinterest_optimization_dataset_on_conversion_date;
