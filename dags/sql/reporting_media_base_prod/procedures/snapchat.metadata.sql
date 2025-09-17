CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.SNAPCHAT.METADATA AS
SELECT
        SRC.ACCOUNT_ID,
        SRC.CAMPAIGN_ID,
        SRC.CAMPAIGN_NAME,
        SRC.ADGROUP_ID,
        SRC.ADGROUP_NAME,
        SRC.AD_ID,
        SRC.AD_NAME,
        CURRENT_TIMESTAMP()::timestamp_ltz AS meta_create_datetime,
        CURRENT_TIMESTAMP()::timestamp_ltz AS meta_update_datetime

    FROM (
        SELECT DISTINCT
            spend.ACCOUNT_ID,
            spend.CAMPAIGN_ID,
            cam.CAMPAIGN_NAME,
            spend.ADGROUP_ID,
            adgr.ADGROUP_NAME,
            spend.AD_ID,
            ad.AD_NAME

        FROM reporting_media_base_prod.SNAPCHAT.SPEND_METRICS_DAILY_BY_AD spend

        LEFT JOIN (
            SELECT
                adm.NAME AS AD_NAME,
                adm.AD_ID,
                ROW_NUMBER() OVER(PARTITION BY p1.AD_ID ORDER BY p1.meta_update_datetime DESC) AS rn_ad
            FROM LAKE_VIEW.SNAPCHAT.PIXEL_DATA_1DAY p1
             JOIN LAKE_VIEW.SNAPCHAT.AD_METADATA adm ON p1.AD_ID = adm.AD_ID)
                                                        AS ad ON ad.AD_ID = spend.AD_ID AND rn_ad = 1

         LEFT JOIN (
            SELECT
                adsm.NAME AS ADGROUP_NAME,
                adsm.ADSQUAD_ID AS ADGROUP_ID,
                ROW_NUMBER() OVER(PARTITION BY AD_SQUAD_ID ORDER BY adsm.meta_update_datetime DESC) AS rn_adgr
            FROM LAKE_VIEW.SNAPCHAT.PIXEL_DATA_1DAY p1
             JOIN LAKE_VIEW.SNAPCHAT.AD_METADATA adm ON p1.AD_ID = adm.AD_ID
             JOIN LAKE_VIEW.SNAPCHAT.ADSQUAD_METADATA adsm ON adm.AD_SQUAD_ID = adsm.ADSQUAD_ID)
                                                        AS adgr ON adgr.ADGROUP_ID = spend.ADGROUP_ID AND rn_adgr = 1


        LEFT JOIN (
            SELECT
                cm.NAME AS CAMPAIGN_NAME,
                cm.CAMPAIGN_ID,
                ROW_NUMBER() OVER(PARTITION BY cm.CAMPAIGN_ID ORDER BY cm.meta_update_datetime DESC) AS rn_cam
            FROM LAKE_VIEW.SNAPCHAT.PIXEL_DATA_1DAY p1
            JOIN LAKE_VIEW.SNAPCHAT.AD_METADATA adm ON p1.AD_ID = adm.AD_ID
            JOIN LAKE_VIEW.SNAPCHAT.ADSQUAD_METADATA adsm ON adm.AD_SQUAD_ID = adsm.ADSQUAD_ID
            JOIN LAKE_VIEW.SNAPCHAT.CAMPAIGN_METADATA cm ON adsm.CAMPAIGN_ID = cm.CAMPAIGN_ID)
                                                        AS cam ON cam.CAMPAIGN_ID = spend.CAMPAIGN_ID AND rn_cam = 1

        ) AS SRC;
