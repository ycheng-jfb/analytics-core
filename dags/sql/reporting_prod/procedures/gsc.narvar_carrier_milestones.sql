/* Identify timestamp for delta */
SET LAST_UPDATE = (
    SELECT COALESCE(MAX(META_UPDATE_DATETIME), '1970-01-01')
    FROM REPORTING_PROD.GSC.NARVAR_CARRIER_MILESTONES
);

/* Parse JSON for the delta */
CREATE OR REPLACE TEMPORARY TABLE _narvar_json_delta AS
SELECT DISTINCT
    raw_packages.order_id,
    raw_packages.tracking_number,
    raw_packages.tracking_carrier_name AS carrier_name,
    CAST(
        COALESCE(
            raw_events.value:date_with_derived_offset::string,
            raw_events.value:date::string
            ) AS TIMESTAMP_TZ
        ) AS milestone_datetime,
    CAST(raw_events.value:local_date_time::string AS TIMESTAMP_NTZ) AS local_date_time,
    raw_events.value:carrier_status_code_description::string AS carrier_milestone_event,
    raw_events.value:status_code_mapping:code::string AS narvar_milestone_status_code,
    raw_events.value:status_code_mapping:code_description::string AS narvar_milestone_event,
    raw_events.value:new::boolean AS is_recent_event,
    raw_packages.is_returned_package,
    raw_packages.updated_at,
    meta_update_datetime
FROM (
    SELECT DISTINCT
        raw_tracking.value as tracking_json,
        CAST(raw_json:meta_info:updated_at::string AS TIMESTAMP_TZ) AS updated_at,
        raw_json:tracking:order_number::string AS order_id,
        raw_tracking.value:tracking_number::string AS tracking_number,
        raw_tracking.value:carrier_moniker::string AS tracking_carrier_name,
        raw_tracking.value:is_return::string AS is_returned_package,
        file_name,
        meta_update_datetime
    FROM (
        SELECT
            raw_json,
            file_name,
            meta_update_datetime
        FROM LAKE.NARVAR.CARRIER_MILESTONES_JSON
        WHERE meta_update_datetime > DATEADD(HOUR, -3, $LAST_UPDATE)
        ) AS X
        , LATERAL FLATTEN (INPUT => X.raw_json:tracking:packages) AS raw_tracking
    ) AS raw_packages
    , LATERAL FLATTEN (INPUT => tracking_json:events) AS raw_events
QUALIFY RANK() OVER(PARTITION BY
    raw_packages.order_id,
    raw_packages.tracking_number,
    raw_packages.tracking_carrier_name
    ORDER BY file_name DESC, raw_packages.updated_at DESC, meta_update_datetime DESC
    ) = 1
;

/* Insert new data */
INSERT INTO REPORTING_PROD.GSC.NARVAR_CARRIER_MILESTONES (
    ORDER_ID,
    TRACKING_NUMBER,
    CARRIER_NAME,
    MILESTONE_DATETIME,
    LOCAL_DATE_TIME,
    CARRIER_MILESTONE_EVENT,
    NARVAR_MILESTONE_STATUS_CODE,
    NARVAR_MILESTONE_EVENT,
    IS_RECENT_EVENT,
    IS_RETURNED_PACKAGE,
    UPDATED_AT,
    META_UPDATE_DATETIME
)
SELECT DISTINCT
    SOURCE.ORDER_ID,
    SOURCE.TRACKING_NUMBER,
    SOURCE.CARRIER_NAME,
    SOURCE.MILESTONE_DATETIME,
    SOURCE.LOCAL_DATE_TIME,
    SOURCE.CARRIER_MILESTONE_EVENT,
    SOURCE.NARVAR_MILESTONE_STATUS_CODE,
    SOURCE.NARVAR_MILESTONE_EVENT,
    SOURCE.IS_RECENT_EVENT,
    SOURCE.IS_RETURNED_PACKAGE,
    SOURCE.UPDATED_AT,
    SOURCE.META_UPDATE_DATETIME
FROM _narvar_json_delta AS SOURCE
LEFT JOIN (
    SELECT
        ORDER_ID,
        TRACKING_NUMBER,
        CARRIER_NAME,
        MAX(META_UPDATE_DATETIME) AS META_UPDATE_DATETIME
    FROM REPORTING_PROD.GSC.NARVAR_CARRIER_MILESTONES
    GROUP BY
        ORDER_ID,
        TRACKING_NUMBER,
        CARRIER_NAME
    ) AS TARGET
    ON COALESCE(SOURCE.ORDER_ID, -1) = COALESCE(TARGET.ORDER_ID, -1)
    AND COALESCE(SOURCE.TRACKING_NUMBER, '') = COALESCE(TARGET.TRACKING_NUMBER, '')
    AND COALESCE(SOURCE.CARRIER_NAME, '') = COALESCE(TARGET.CARRIER_NAME, '')
WHERE SOURCE.META_UPDATE_DATETIME > COALESCE(TARGET.META_UPDATE_DATETIME, '1970-01-01')
ORDER BY
    SOURCE.MILESTONE_DATETIME ASC,
    SOURCE.ORDER_ID ASC,
    SOURCE.TRACKING_NUMBER ASC,
    SOURCE.CARRIER_NAME ASC;
