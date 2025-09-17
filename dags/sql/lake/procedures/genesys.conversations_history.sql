SET watermark = (
    SELECT COALESCE(MAX(meta_update_datetime_src), '1970-01-01') AS meta_update_datetime_src
    FROM lake.genesys.conversations_history
    );

INSERT INTO lake.genesys.conversations_history(
        conversationId,
        conversationEnd,
        conversationStart,
        mediaStatsMinConversationMos,
        mediaStatsMinConversationRFactor,
        originatingDirection,
        participants,
        externalTag,
        source_filename,
        last_modified,
        meta_update_datetime_src,
        IS_CURRENT,
        EFFECTIVE_START_DATETIME,
        EFFECTIVE_END_DATETIME,
        META_UPDATE_DATETIME
)
    SELECT
        conversationId,
        conversationEnd,
        conversationStart,
        mediaStatsMinConversationMos,
        mediaStatsMinConversationRFactor,
        originatingDirection,
        participants,
        externalTag,
        source_filename,
        last_modified,
        meta_update_datetime as meta_update_datetime_src,
        TRUE as IS_CURRENT,
        meta_update_datetime_src as EFFECTIVE_START_DATETIME,
        NULL::TIMESTAMP_LTZ as EFFECTIVE_END_DATETIME,
        CURRENT_TIMESTAMP() AS META_UPDATE_DATETIME
    FROM lake.genesys.conversations_new n
    WHERE meta_update_datetime >= DATEADD(MINUTE, -5, $watermark)
      AND NOT EXISTS(
          select distinct
              h.conversationId,
              h.meta_update_datetime_src
          from lake.genesys.conversations_history h
          where h.meta_update_datetime_src >= DATEADD(MINUTE, -5, $watermark)
          and h.conversationId = n.conversationId
          and h.meta_update_datetime_src = n.meta_update_datetime

        );

CREATE OR REPLACE TEMPORARY TABLE _rows AS
WITH _fix AS (
SELECT
    conversationId,
    meta_update_datetime_src,
    effective_start_datetime,
    effective_end_datetime,
    ROW_NUMBER() OVER(PARTITION BY conversationId ORDER BY effective_start_datetime ASC) AS rnk
FROM lake.genesys.conversations_history AS H
)

SELECT
   A.conversationId,
   A.meta_update_datetime_src,
   A.effective_start_datetime,
   COALESCE(dateadd(MILLISECOND, -1, B.effective_start_datetime), '9999-12-31 00:00:00.000 -0800'
       ) AS new_effective_end_datetime
FROM _fix AS A
    LEFT JOIN _fix AS B
    ON A.conversationId = B.conversationId
    AND A.rnk = B.rnk - 1
;

/* Update effective timestamps */
UPDATE lake.genesys.conversations_history AS A
SET A.effective_end_datetime = B.new_effective_end_datetime,
    META_UPDATE_DATETIME = current_timestamp
FROM _rows AS B
WHERE A.conversationId = B.conversationId
    AND A.meta_update_datetime_src = B.meta_update_datetime_src
    AND (A.effective_end_datetime <> B.new_effective_end_datetime
    	OR A.effective_end_datetime IS NULL);

/* Update IS_CURRENT */
UPDATE lake.genesys.conversations_history
SET
    IS_CURRENT = CASE WHEN effective_end_datetime = '9999-12-31' THEN TRUE ELSE FALSE END,
    META_UPDATE_DATETIME = current_timestamp
WHERE conversationId IN (SELECT DISTINCT conversationId FROM _rows WHERE conversationId IS NOT NULL)
    AND IS_CURRENT <> CASE WHEN effective_end_datetime = '9999-12-31' THEN TRUE ELSE FALSE END
;
