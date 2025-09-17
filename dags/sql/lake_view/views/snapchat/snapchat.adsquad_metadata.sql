CREATE VIEW IF NOT EXISTS LAKE_VIEW.SNAPCHAT.ADSQUAD_METADATA(
        ADSQUAD_ID,
        UPDATED_AT,
        CREATED_AT,
        NAME,
        STATUS,
        CAMPAIGN_ID,
        ADSQUAD_TYPE,
        PLACEMENT,
        BILLING_EVENT,
        BID_MICRO,
        AUTO_BID,
        TARGET_BID,
        DAILY_BUDGET_MICRO,
        START_TIME,
        OPTIMIZATION_GOAL,
        DELIVERY_CONSTRAINT,
        PACING_TYPE,
        META_CREATE_DATETIME,
        META_UPDATE_DATETIME
) as
SELECT ID AS ADSQUAD_ID
                ,UPDATED_AT
                ,CREATED_AT
                ,NAME
                ,STATUS
                ,CAMPAIGN_ID
                ,TYPE as ADSQUAD_TYPE
                ,PLACEMENT
                ,BILLING_EVENT
                ,BID_MICRO
                ,AUTO_BID
                ,TARGET_BID
                ,DAILY_BUDGET_MICRO
                ,START_TIME
                ,OPTIMIZATION_GOAL
                ,DELIVERY_CONSTRAINT
                ,PACING_TYPE
                ,convert_timezone('America/Los_Angeles',_FIVETRAN_SYNCED) as META_CREATE_DATETIME
                ,convert_timezone('America/Los_Angeles',_FIVETRAN_SYNCED) as META_UPDATE_DATETIME
        FROM lake_fivetran.MED_SNAPCHAT_AD1_V1.ad_squad_history qualify row_number() OVER (
                        PARTITION BY id ORDER BY UPDATED_AT DESC
                        ) = 1;
