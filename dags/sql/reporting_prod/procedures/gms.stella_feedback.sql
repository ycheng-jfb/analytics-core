SET low_watermark_datetime = COALESCE((select max(meta_update_datetime)
from reporting_prod.gms.stella_feedback),'1900-01-01'::TIMESTAMP_LTZ);

CREATE OR REPLACE TEMPORARY TABLE _stella_feedback_excellence_response AS
SELECT
    uuid,
    listagg(excell.value:option_text::string,',') as areas_of_excellence_response
FROM lake.stella.feedback
, LATERAL FLATTEN (input => answers:areas_of_excellence:selected_options) excell
GROUP BY
uuid;

CREATE OR REPLACE TEMPORARY TABLE _stella_feedback_improvement_response AS
SELECT
    uuid,
    listagg(improv.value:option_text::string,',') as areas_of_improvement_response
FROM lake.stella.feedback
, LATERAL FLATTEN (input => answers:areas_of_improvement:selected_options) improv
GROUP BY
uuid;

CREATE OR REPLACE TEMPORARY TABLE _stella_feedback_multiple_choice_response AS
SELECT
    uuid,
    listagg(mc.value:option_text::string,',') as multiple_choice_response
FROM lake.stella.feedback
, LATERAL FLATTEN (input => answers:multiple_choice:selected_options) mc
GROUP BY
uuid;

CREATE OR REPLACE TEMPORARY TABLE _stella_feedback_nps AS
SELECT
    uuid,
    listagg(DISTINCT nps.this:comment::string, ',') as nps_comment,
    listagg(DISTINCT nps_score.value:integer_value::string, ',') as nps_score
FROM lake.stella.feedback
, LATERAL FLATTEN (input => answers:nps_epan) nps
, LATERAL FLATTEN (input => answers:nps_epan:selected_options) nps_score
GROUP BY uuid;


CREATE OR REPLACE TEMPORARY TABLE _stella_feedback_stg AS
SELECT
	 sf.uuid                    AS uuid,
	 sequence_id                AS sequence_id,
	 branding                   AS branding,
	 channel                    AS channel,
	 ext_interaction_id         AS conversation_id,
	 external_url               AS external_url,
	 language                   AS language,
	 survey_id                  AS survey_id,
	 survey_name                AS survey_name,
	 tags                       AS tags,
	 request_created_at         AS request_created_at,
	 request_delivery_status    AS request_delivery_status,
	 request_sent_at            AS request_sent_at,
	 requested_via              AS requested_via,
	 response_received_at       AS response_received_at,
	 reward_eligible            AS reward_eligible,
	 reward_name                AS reward_name,
	 employee:custom_id         AS employee_custom_id,
	 employee:email             AS employee_email,
	 employee:first_name        AS employee_first_name,
	 employee:last_name         AS employee_last_name,
	 team_leader:custom_id      AS team_leader_custom_id,
	 team_leader:full_name      AS team_leader_full_name,
	 customer:custom_id         AS customer_custom_id,
	 customer:email             AS customer_email,
	 customer:full_name         AS customer_full_name,
	 answers:agent_star_rating:question_text AS answers_agent_star_rating_question_text,
	 COALESCE(answers:agent_star_rating:selected_options:"32223":integer_value,  -- 5
	          answers:agent_star_rating:selected_options:"32219":integer_value,  -- 4
	          answers:agent_star_rating:selected_options:"32212":integer_value,  -- 3
	          answers:agent_star_rating:selected_options:"32207":integer_value,  -- 2
	          answers:agent_star_rating:selected_options:"32201":integer_value   -- 1
	          )                 AS answers_agent_star_rating_value,
	 answers:agent_star_rating:comment AS answers_agent_star_rating_comment ,
	 e.areas_of_excellence_response AS areas_of_excellence_response,
	 i.areas_of_improvement_response AS areas_of_improvement_response,
	 mc.multiple_choice_response AS multiple_choice_response,
     n.nps_comment,
     n.nps_score,
	 answers                    AS answers,
	 marketing                  AS marketing,
	 custom_properties          AS custom_properties,
	 sf.disputed                AS disputed,
     meta_create_datetime       AS meta_create_datetime,
     meta_update_datetime       AS meta_update_datetime
FROM lake.stella.feedback AS sf
LEFT JOIN _stella_feedback_excellence_response AS e
    on sf.uuid = e.uuid
LEFT JOIN _stella_feedback_improvement_response AS i
    on sf.uuid = i.uuid
LEFT JOIN _stella_feedback_multiple_choice_response AS mc
    on sf.uuid = mc.uuid
LEFT JOIN _stella_feedback_nps AS n
    on sf.uuid = n.uuid
WHERE meta_update_datetime > $low_watermark_datetime;

MERGE INTO reporting_prod.gms.stella_feedback AS t
    USING (
        SELECT *
        FROM (
            select *,
                HASH(*) AS meta_row_hash,
                row_number() OVER (PARTITION BY uuid ORDER BY meta_update_datetime DESC) AS rn
            FROM _stella_feedback_stg
            ) AS sf
        where rn = 1
    ) AS s ON equal_null(t.uuid, s.uuid)
    WHEN NOT MATCHED THEN INSERT (
        uuid, sequence_id, branding, channel, conversation_id, external_url, language,
        survey_id, survey_name, tags, request_created_at, request_delivery_status,
        request_sent_at, requested_via, response_received_at, reward_eligible,
        reward_name, employee_custom_id, employee_email, employee_first_name,
        employee_last_name, team_leader_custom_id, team_leader_full_name,
        customer_custom_id, customer_email, customer_full_name,
        answers_agent_star_rating_question_text, answers_agent_star_rating_value,
        answers_agent_star_rating_comment, areas_of_excellence_response,
        areas_of_improvement_response, multiple_choice_response,
        nps_comment, nps_score,
        answers, marketing, custom_properties, disputed,
        meta_row_hash, meta_create_datetime, meta_update_datetime
        )
    VALUES (
        uuid, sequence_id, branding, channel, conversation_id, external_url, language,
        survey_id, survey_name, tags, request_created_at, request_delivery_status,
        request_sent_at, requested_via, response_received_at, reward_eligible,
        reward_name, employee_custom_id, employee_email, employee_first_name,
        employee_last_name, team_leader_custom_id, team_leader_full_name,
        customer_custom_id, customer_email, customer_full_name,
        answers_agent_star_rating_question_text, answers_agent_star_rating_value,
        answers_agent_star_rating_comment, areas_of_excellence_response,
        areas_of_improvement_response, multiple_choice_response,
        nps_comment, nps_score,
        answers, marketing, custom_properties, disputed,
        meta_row_hash, meta_create_datetime, meta_update_datetime)
    WHEN MATCHED
        AND (s.meta_row_hash != t.meta_row_hash OR s.disputed != t.disputed)
        AND s.meta_update_datetime > t.meta_update_datetime
    THEN
    UPDATE SET
        t.sequence_id = s.sequence_id
        ,t.branding = s.branding
        ,t.channel = s.channel
        ,t.conversation_id = s.conversation_id
        ,t.external_url = s.external_url
        ,t.language = s.language
        ,t.survey_id = s.survey_id
        ,t.survey_name = s.survey_name
        ,t.tags = s.tags
        ,t.request_created_at = s.request_created_at
        ,t.request_delivery_status = s.request_delivery_status
        ,t.request_sent_at = s.request_sent_at
        ,t.requested_via = s.requested_via
        ,t.response_received_at = s.response_received_at
        ,t.reward_eligible = s.reward_eligible
        ,t.reward_name = s.reward_name
        ,t.employee_custom_id = s.employee_custom_id
        ,t.employee_email = s.employee_email
        ,t.employee_first_name = s.employee_first_name
        ,t.employee_last_name = s.employee_last_name
        ,t.team_leader_custom_id = s.team_leader_custom_id
        ,t.team_leader_full_name = s.team_leader_full_name
        ,t.customer_custom_id = s.customer_custom_id
        ,t.customer_email = s.customer_email
        ,t.customer_full_name = s.customer_full_name
        ,t.answers_agent_star_rating_question_text = s.answers_agent_star_rating_question_text
        ,t.answers_agent_star_rating_value = s.answers_agent_star_rating_value
        ,t.answers_agent_star_rating_comment = s.answers_agent_star_rating_comment
        ,t.areas_of_excellence_response = s.areas_of_excellence_response
        ,t.areas_of_improvement_response = s.areas_of_improvement_response
        ,t.multiple_choice_response = s.multiple_choice_response
        ,t.nps_comment = s.nps_comment
        ,t.nps_score = s.nps_score
        ,t.answers = s.answers
        ,t.marketing = s.marketing
        ,t.custom_properties = s.custom_properties
        ,t.disputed = s.disputed
        ,t.meta_row_hash = s.meta_row_hash
        ,t.meta_update_datetime = CURRENT_TIMESTAMP;
