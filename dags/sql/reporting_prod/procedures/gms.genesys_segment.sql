SET low_watermark_datetime = %(low_watermark)s :: TIMESTAMP_LTZ;


CREATE OR REPLACE TEMPORARY TABLE _genesys_segment_stg AS
SELECT conversationid                            								        AS conversation_id,
       p.value:participantId::STRING             								        AS participant_id,
       s.value:sessionId::STRING                 								        AS session_id,
       TO_TIMESTAMP_LTZ(m.value:segmentStart)    AS segment_start,
       TO_TIMESTAMP_LTZ(m.value:segmentEnd)      AS segment_end,
       m.value:queueId::STRING                   								        AS queue_id,
       m.value:wrapUpCode::STRING                								        AS wrapup_code,
       m.value:wrapUpNote::STRING                								        AS wrapup_note,
       m.value:errorCode::STRING                 								        AS error_code,
       m.value:disconnectType::STRING            								        AS disconnect_type,
       m.value:segmentType::STRING               								        AS segment_type,
       m.value:requestedLanguageId::STRING       								        AS requested_language_id,
       m.value:sourceConversationId::STRING      								        AS source_conversation_id,
       m.value:destinationConversationId::STRING 								        AS destination_conversation_id,
       m.value:sourceSessionId::STRING           								        AS source_session_id,
       m.value:destinationSessionId::STRING      								        AS destination_session_id,
       m.value:conference                        								        AS conference,
       m.value:groupId::STRING                   								        AS group_id,
       m.value:subject::STRING                   								        AS subject,
       m.value:audioMuted                        								        AS audio_muted,
       m.value:videoMuted                        								        AS video_muted
FROM lake.genesys.conversations
   , LATERAL FLATTEN(INPUT => participants) p
   , LATERAL FLATTEN(INPUT => p.value:sessions) s
   , LATERAL FLATTEN(INPUT => s.value:segments) m
WHERE meta_update_datetime > $low_watermark_datetime;


MERGE INTO reporting_prod.gms.genesys_segment t
    USING (SELECT *
                FROM (select *,
                        HASH(*) as meta_row_hash,
                        row_number()
                            OVER (PARTITION BY conversation_id,session_id,segment_start,segment_type
                                ORDER BY coalesce(segment_end,'1900-01-01') DESC) AS rn
                        FROM _genesys_segment_stg) gcm
                where rn = 1) s ON equal_null(t.conversation_id, s.conversation_id)
        AND equal_null(t.session_id, s.session_id)
        AND equal_null(t.segment_start, s.segment_start)
        AND equal_null(t.segment_type, s.segment_type)
    WHEN NOT MATCHED THEN INSERT
        (conversation_id, participant_id, session_id, segment_start, segment_end, queue_id, wrapup_code, wrapup_note,
         error_code, disconnect_type, segment_type, requested_language_id, source_conversation_id,
         destination_conversation_id, source_session_id, destination_session_id, conference, group_id, subject,
         audio_muted, video_muted, meta_row_hash)
        VALUES (conversation_id, participant_id, session_id, segment_start, segment_end, queue_id, wrapup_code,
                wrapup_note, error_code, disconnect_type, segment_type, requested_language_id, source_conversation_id,
                destination_conversation_id, source_session_id, destination_session_id, conference, group_id, subject,
                audio_muted, video_muted, meta_row_hash)
    WHEN MATCHED AND s.meta_row_hash != t.meta_row_hash
        AND coalesce(s.segment_end, '1900-01-01') > coalesce(t.segment_end, '1900-01-01') THEN
        UPDATE
            SET t.participant_id = s.participant_id
                ,t.segment_end = s.segment_end
                ,t.queue_id = s.queue_id
                ,t.wrapup_code = s.wrapup_code
                ,t.wrapup_note = s.wrapup_note
                ,t.error_code = s.error_code
                ,t.disconnect_type = s.disconnect_type
                ,t.requested_language_id = s.requested_language_id
                ,t.source_conversation_id = s.source_conversation_id
                ,t.destination_conversation_id = s.destination_conversation_id
                ,t.source_session_id = s.source_session_id
                ,t.destination_session_id = s.destination_session_id
                ,t.conference = s.conference
                ,t.group_id = s.group_id
                ,t.subject = s.subject
                ,t.audio_muted = s.audio_muted
                ,t.video_muted = s.video_muted
                ,t.meta_row_hash = s.meta_row_hash
                ,t.meta_update_datetime = CURRENT_TIMESTAMP;
