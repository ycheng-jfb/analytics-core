SET feedback_low_watermark_datetime = DATEADD(MONTH, -3, CURRENT_DATE());
SET feedback_high_watermark_datetime = DATEADD(DAY, -10, CURRENT_DATE());

CREATE OR REPLACE TEMP TABLE _stella_feedback AS
SELECT
    *
FROM LAKE.STELLA.FEEDBACK
WHERE request_created_at >= $feedback_low_watermark_datetime
    AND request_created_at < $feedback_high_watermark_datetime;

CREATE OR REPLACE TEMP TABLE _stella_feedback_backfill AS
SELECT
    *
FROM LAKE.STELLA.FEEDBACK_BACKFILL
WHERE request_created_at >= $feedback_low_watermark_datetime
    AND request_created_at < $feedback_high_watermark_datetime;

CREATE OR REPLACE TEMP TABLE _stella_feedback_disputed_records AS
SELECT s.uuid
FROM _stella_feedback s
LEFT JOIN  _stella_feedback_backfill sb
    ON s.uuid = sb.uuid
WHERE sb.uuid IS NULL;

MERGE INTO LAKE.STELLA.FEEDBACK t
USING (
    SELECT
        uuid
    FROM _stella_feedback_disputed_records
) s ON equal_null(t.uuid, s.uuid)
WHEN MATCHED THEN UPDATE
SET
    t.disputed = True,
    t.meta_update_datetime = CURRENT_TIMESTAMP;
