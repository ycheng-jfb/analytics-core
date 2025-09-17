CREATE VIEW IF NOT EXISTS LAKE_VIEW.SNAPCHAT.CAMPAIGN_METADATA(
        CAMPAIGN_ID,
        UPDATED_AT,
        CREATED_AT,
        NAME,
        AD_ACCOUNT_ID,
        DAILY_BUDGET_MICRO,
        STATUS,
        START_TIME,
        END_TIME,
        META_CREATE_DATETIME,
        META_UPDATE_DATETIME
) as
SELECT ID as CAMPAIGN_ID
            ,UPDATED_AT
            ,CREATED_AT
            ,NAME
            ,AD_ACCOUNT_ID
            ,DAILY_BUDGET_MICRO
            ,STATUS
            ,START_TIME
            ,END_TIME
                ,convert_timezone('America/Los_Angeles',_FIVETRAN_SYNCED) as META_CREATE_DATETIME
                ,convert_timezone('America/Los_Angeles',_FIVETRAN_SYNCED) as META_UPDATE_DATETIME
        from lake_fivetran.MED_SNAPCHAT_AD1_V1.CAMPAIGN_HISTORY
qualify row_number() over(partition by id order by UPDATED_AT desc)=1;
