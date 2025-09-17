
CREATE OR REPLACE TRANSIENT TABLE REPORTING_MEDIA_PROD.SNAPCHAT.SNAPCHAT_OPTIMIZATION_DATASET AS
    SELECT SNAP.*,
           DATE,
           TFEE_PCT_OF_MEDIA_SPEND AS VENDOR_FEES,
           SPEND_ACCOUNT_CURRENCY,
           SPEND_ACCOUNT_CURRENCY * (1 + IFNULL(VENDOR_FEES,0)) AS SPEND_WITH_VENDOR_FEES_ACCOUNT_CURRENCY,
           SPEND_USD,
           SPEND_USD * (1 + IFNULL(VENDOR_FEES,0)) AS SPEND_WITH_VENDOR_FEES_USD,
           SPEND_LOCAL,
           SPEND_LOCAL * (1 + IFNULL(VENDOR_FEES,0)) AS SPEND_WITH_VENDOR_FEES_LOCAL,
           IMPRESSIONS,
           CLICKS,
           PIXEL_VIP_CLICK_1D,
           PIXEL_VIP_VIEW_1D,
           PIXEL_LEAD_CLICK_1D,
           PIXEL_LEAD_VIEW_1D,
           PIXEL_VIP_CLICK_7D,
           PIXEL_VIP_VIEW_7D,
           PIXEL_LEAD_CLICK_7D,
           PIXEL_LEAD_VIEW_7D,
           PIXEL_VIP_CLICK_28D,
           PIXEL_VIP_VIEW_28D,
           PIXEL_LEAD_CLICK_28D,
           PIXEL_LEAD_VIEW_28D,

           PIXEL_META_UPDATE_DATETIME,
           CURRENT_TIMESTAMP() AS PROCESS_META_UPDATE_DATETIME

FROM REPORTING_MEDIA_BASE_PROD.SNAPCHAT.SPEND_METRICS_DAILY_BY_AD PIXEL
LEFT JOIN REPORTING_MEDIA_BASE_PROD.SNAPCHAT.VW_SNAPCHAT_NAMING_CONVENTION SNAP ON SNAP.AD_ID = PIXEL.AD_ID
left join lake_view.sharepoint.med_vendor_fees_media v
    on (case when lower(v.identifier) = 'campaign' and snap.campaign_name ilike '%' || lower(v.identifier_value) || '%'
            and date between v.start_date_include and v.end_date_exclude - 1
        then cast(snap.account_id as varchar) = v.account_id
        when lower(v.identifier) = 'adset' and snap.adgroup_name ilike '%' || lower(v.identifier_value) || '%'
            and date between v.start_date_include and v.end_date_exclude - 1
        then cast(snap.account_id as varchar) = v.account_id end)
    and lower(v.channel) = 'snapchat';
