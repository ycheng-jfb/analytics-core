BEGIN;
DELETE FROM lake_stg.alchemer.survey_responses_quarterly_stg;
COPY INTO lake_stg.alchemer.survey_responses_quarterly_stg (survey_id, survey_title, respondent_id, session_id, customer_id, customer_id_type, contact_id, question_id, question_label, l1_question_id, l1_question_label, l2_question_id, l2_question_label, l3_question_id, l3_question_label, question_type, response, response_text, response_numeric, response_alt, answer_id, updated_at, date_submitted)
FROM '@lake_stg.public.tsos_da_int_inbound/lake/lake.alchemer.survey_responses_quarterly/v1'
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '\t',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
)
ON_ERROR = 'SKIP_FILE_1%'
;

DELETE FROM lake.alchemer.survey_responses
WHERE date_submitted >= (SELECT MIN(date_submitted) from lake_stg.alchemer.survey_responses_quarterly_stg)
AND date_submitted <= (SELECT MAX(date_submitted) from lake_stg.alchemer.survey_responses_quarterly_stg);

MERGE INTO lake.alchemer.survey_responses t
USING (
    SELECT
        a.*
    FROM (
        SELECT
            *,
            hash(*) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER ( PARTITION BY survey_id, respondent_id, question_id ORDER BY coalesce(updated_at, '1900-01-01') DESC ) AS rn
        FROM lake_stg.alchemer.survey_responses_quarterly_stg
     ) a
    WHERE a.rn = 1
) s ON equal_null(t.survey_id, s.survey_id)
    AND equal_null(t.respondent_id, s.respondent_id)
    AND equal_null(t.question_id, s.question_id)
WHEN NOT MATCHED THEN INSERT (
    survey_id, survey_title, respondent_id, session_id, customer_id, customer_id_type, contact_id, question_id, question_label, l1_question_id, l1_question_label, l2_question_id, l2_question_label, l3_question_id, l3_question_label, question_type, response, response_text, response_numeric, response_alt, answer_id, updated_at, date_submitted, meta_row_hash, meta_create_datetime, meta_update_datetime
)
VALUES (
    survey_id, survey_title, respondent_id, session_id, customer_id, customer_id_type, contact_id, question_id, question_label, l1_question_id, l1_question_label, l2_question_id, l2_question_label, l3_question_id, l3_question_label, question_type, response, response_text, response_numeric, response_alt, answer_id, updated_at, date_submitted, meta_row_hash, meta_create_datetime, meta_update_datetime
)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
    AND coalesce(s.updated_at, '1900-01-01') > coalesce(t.updated_at, '1900-01-01') THEN UPDATE
SET t.survey_id = s.survey_id,
    t.survey_title = s.survey_title,
    t.respondent_id = s.respondent_id,
    t.session_id = s.session_id,
    t.customer_id = s.customer_id,
    t.customer_id_type = s.customer_id_type,
    t.contact_id = s.contact_id,
    t.question_id = s.question_id,
    t.question_label = s.question_label,
    t.l1_question_id = s.l1_question_id,
    t.l1_question_label = s.l1_question_label,
    t.l2_question_id = s.l2_question_id,
    t.l2_question_label = s.l2_question_label,
    t.l3_question_id = s.l3_question_id,
    t.l3_question_label = s.l3_question_label,
    t.question_type = s.question_type,
    t.response = s.response,
    t.response_text = s.response_text,
    t.response_numeric = s.response_numeric,
    t.response_alt = s.response_alt,
    t.answer_id = s.answer_id,
    t.updated_at = s.updated_at,
    t.date_submitted = s.date_submitted,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime;
COMMIT;
