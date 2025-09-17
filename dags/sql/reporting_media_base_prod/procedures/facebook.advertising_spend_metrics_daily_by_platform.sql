SET meta_update_datetime = (SELECT MAX(META_UPDATE_DATETIME) FROM lake_view.FACEBOOK.AD_INSIGHTS_BY_PLATFORM);
CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.FACEBOOK.ADVERTISING_SPEND_METRICS_DAILY_BY_PLATFORM AS
SELECT
    ACCOUNT_ID,
    STORE_ID,
    AD_ID,
    DATE,

    --IFNULL(SPEND*100/NULLIF(SUM(spend) OVER (PARTITION BY ad_id ORDER BY (SELECT 0)),0),0) as spend_allocation_pct,
    --IFNULL(impressions*100/NULLIF(SUM(spend) OVER (PARTITION BY ad_id ORDER BY (SELECT 0)),0),0) as impressions_allocation_pct,
    --IFNULL(clicks*100/NULLIF(SUM(spend) OVER (PARTITION BY ad_id ORDER BY (SELECT 0)),0),0) as clicks_allocation_pct,

    PUBLISHER_PLATFORM,
    PLATFORM_POSITION,
    IMPRESSION_DEVICE,
    SPEND_ACCOUNT_CURRENCY,
    SPEND_USD,
    SPEND_LOCAL,
    IMPRESSIONS,
    CLICKS,

    $meta_update_datetime AS PIXEL_SPEND_META_UPDATE_DATETIME,
    CURRENT_TIMESTAMP()::timestamp_ltz AS META_CREATE_DATETIME,
    CURRENT_TIMESTAMP()::timestamp_ltz AS META_UPDATE_DATETIME
FROM
    (
    SELECT
        ACCOUNT_ID,
        st.STORE_ID,
        AD_ID,
        PUBLISHER_PLATFORM,
        PLATFORM_POSITION,
        IMPRESSION_DEVICE,
        date AS DATE,
        SUM(SPEND) AS SPEND_ACCOUNT_CURRENCY,
        SUM((SPEND)*COALESCE(lkpusd.EXCHANGE_RATE,1)) AS SPEND_USD,
        SUM((SPEND)*COALESCE(lkplocal.EXCHANGE_RATE,1)) AS SPEND_LOCAL,
        SUM(IMPRESSIONS) AS IMPRESSIONS,
        SUM(COALESCE(outbound_clicks,INLINE_LINK_CLICKS)) AS CLICKS

        FROM LAKE_VIEW.FACEBOOK.AD_INSIGHTS_BY_PLATFORM fb
        LEFT JOIN lake_view.sharepoint.med_account_mapping_media am ON lower(am.SOURCE_ID) = lower(fb.ACCOUNT_ID)
                AND am.SOURCE ilike '%facebook%'
        LEFT JOIN edw_prod.data_model.dim_store st on st.STORE_ID = am.STORE_ID
        LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
                AND fb.date = lkpusd.rate_date_pst
                AND lkpusd.dest_currency = 'USD'
        LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
                AND fb.date = lkplocal.rate_date_pst
                AND lkplocal.dest_currency = st.STORE_CURRENCY
        GROUP BY 1,2,3,4,5,6,7
);
