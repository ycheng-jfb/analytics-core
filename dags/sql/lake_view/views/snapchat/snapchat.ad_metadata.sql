CREATE VIEW IF NOT EXISTS LAKE_VIEW.SNAPCHAT.AD_METADATA(
	AD_ID,
	UPDATED_AT,
	CREATED_AT,
	NAME,
	AD_SQUAD_ID,
	CREATIVE_ID,
	STATUS,
	AD_TYPE,
  REVIEW_STATUS,
  REVIEW_STATUS_REASON,
	META_CREATE_DATETIME,
	META_UPDATE_DATETIME
) as
SELECT ID AS AD_ID
            ,UPDATED_AT
            ,CREATED_AT
            ,NAME
            ,AD_SQUAD_ID
            ,CREATIVE_ID
            ,STATUS
            ,TYPE AS AD_TYPE
            ,REVIEW_STATUS
            ,REVIEW_STATUS_REASON
            ,convert_timezone('America/Los_Angeles',_FIVETRAN_SYNCED) AS META_CREATE_DATETIME
            ,convert_timezone('America/Los_Angeles',_FIVETRAN_SYNCED) AS META_UPDATE_DATETIME
        FROM lake_fivetran.MED_SNAPCHAT_AD1_V1.AD_HISTORY qualify row_number() OVER (
                PARTITION BY id ORDER BY UPDATED_AT DESC
                ) = 1;
