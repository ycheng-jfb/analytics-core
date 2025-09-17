CREATE PIPE LAKE.NARVAR.NARVAR_JSON_INGESTION
    auto_ingest = true
AS
COPY INTO LAKE.NARVAR.CARRIER_MILESTONES_JSON
FROM (
    SELECT
        $1 AS raw_json,
        METADATA$FILENAME AS FILE_NAME,
        CURRENT_TIMESTAMP() AS meta_update_datetime
    FROM @LAKE_STG.PUBLIC.TSOS_DA_INT_NARVAR/techstyleos
        (file_format => 'LAKE_STG.PUBLIC.FF_JSON')
    )
;

/*
COPY INTO LAKE.NARVAR.CARRIER_MILESTONES_JSON
FROM (
    SELECT
        $1 AS raw_json,
        METADATA$FILENAME AS FILE_NAME,
        CURRENT_TIMESTAMP() AS meta_update_datetime
    FROM @LAKE_STG.PUBLIC.TSOS_DA_INT_NARVAR/techstyleos
        (file_format => 'LAKE_STG.PUBLIC.FF_JSON')
    )
;
*/
