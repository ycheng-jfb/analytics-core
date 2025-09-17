
SET high_watermark_datetime = (SELECT MAX(META_UPDATE_DATETIME) FROM LAKE_VIEW.SNAPCHAT.PIXEL_DATA_1DAY);

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.SNAPCHAT.SPEND_METRICS_DAILY_BY_AD AS
SELECT
        ACCOUNT_ID,
        CAMPAIGN_ID,
        ADGROUP_ID,
        AD_ID,
        DATE,
        SPEND_ACCOUNT_CURRENCY,
        SPEND_USD,
        SPEND_LOCAL,
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
        $high_watermark_datetime AS PIXEL_META_UPDATE_DATETIME,
        CURRENT_TIMESTAMP()::timestamp_ltz AS META_CREATE_DATETIME,
        CURRENT_TIMESTAMP()::timestamp_ltz AS META_UPDATE_DATETIME

    FROM (
        SELECT
        cmd.AD_ACCOUNT_ID AS ACCOUNT_ID,
        adsm.CAMPAIGN_ID AS CAMPAIGN_ID,
        adm.AD_SQUAD_ID AS ADGROUP_ID,
        p1.AD_ID AS AD_ID,
        CAST(p1.START_TIME AS DATE) AS DATE,

        SUM(coalesce(p1.SPEND,0)/1000000) AS SPEND_ACCOUNT_CURRENCY,
        SUM((coalesce(p1.SPEND,0)/1000000)*COALESCE(lkpusd.EXCHANGE_RATE,1)) AS SPEND_USD,
        SUM((coalesce(p1.SPEND,0)/1000000)*COALESCE(lkplocal.EXCHANGE_RATE,1)) AS SPEND_LOCAL,

        SUM(coalesce(p1.IMPRESSIONS,0)) AS IMPRESSIONS,
        SUM(coalesce(p1.SWIPES,0)) AS CLICKS,

        SUM(coalesce(p1.CONVERSION_PURCHASES_SWIPE_UP,0)) AS PIXEL_VIP_CLICK_1D,
        SUM(coalesce(p1.CONVERSION_SIGN_UPS_SWIPE_UP,0)) AS PIXEL_LEAD_CLICK_1D,
        SUM(COALESCE(p1.CONVERSION_PURCHASES_VIEW,0)) AS PIXEL_VIP_VIEW_1D,
        SUM(COALESCE(p1.CONVERSION_SIGN_UPS_VIEW,0)) AS PIXEL_LEAD_VIEW_1D,

        SUM(COALESCE(p7.CONVERSION_PURCHASES_SWIPE_UP,0)) AS PIXEL_VIP_CLICK_7D,
        SUM(COALESCE(p7.CONVERSION_SIGN_UPS_SWIPE_UP,0)) AS PIXEL_LEAD_CLICK_7D,
        SUM(COALESCE(p7.CONVERSION_PURCHASES_VIEW,0)) AS PIXEL_VIP_VIEW_7D,
        SUM(COALESCE(p7.CONVERSION_SIGN_UPS_VIEW,0)) AS PIXEL_LEAD_VIEW_7D,

        SUM(COALESCE(p28.CONVERSION_PURCHASES_SWIPE_UP,0)) AS PIXEL_VIP_CLICK_28D,
        SUM(COALESCE(p28.CONVERSION_SIGN_UPS_SWIPE_UP,0)) AS PIXEL_LEAD_CLICK_28D,
        SUM(COALESCE(p28.CONVERSION_PURCHASES_VIEW,0)) AS PIXEL_VIP_VIEW_28D,
        SUM(COALESCE(p28.CONVERSION_SIGN_UPS_VIEW,0)) AS PIXEL_LEAD_VIEW_28D

        FROM LAKE_VIEW.snapchat.pixel_data_1day p1
            JOIN LAKE_VIEW.snapchat.ad_metadata adm ON p1.AD_ID = adm.AD_ID
            JOIN LAKE_VIEW.snapchat.adsquad_metadata adsm ON adm.AD_SQUAD_ID = adsm.ADSQUAD_ID
            JOIN LAKE_VIEW.snapchat.campaign_metadata cmd on adsm.campaign_id=cmd.campaign_id
        LEFT JOIN LAKE_VIEW.snapchat.pixel_data_7day p7 ON p1.AD_ID = p7.AD_ID AND CAST(p1.START_TIME AS DATE) = CAST(p7.START_TIME AS DATE)
        LEFT JOIN LAKE_VIEW.snapchat.pixel_data_28day p28 ON p1.AD_ID = p28.AD_ID AND CAST(p1.START_TIME AS DATE) = CAST(p28.START_TIME AS DATE)
        LEFT JOIN lake_view.sharepoint.med_account_mapping_media am ON lower(am.SOURCE_ID) = lower(cmd.AD_ACCOUNT_ID)
                    AND am.SOURCE ilike '%snapchat%'
        LEFT JOIN edw_prod.data_model.dim_store st on st.STORE_ID = am.STORE_ID
        LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
                    AND CAST(p1.START_TIME AS DATE) = lkpusd.rate_date_pst
                    AND lkpusd.dest_currency = 'USD'
        LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
                    AND CAST(p1.START_TIME AS DATE) = lkplocal.rate_date_pst
                    AND lkplocal.dest_currency = st.STORE_CURRENCY
        GROUP BY 1,2,3,4,5)
;
