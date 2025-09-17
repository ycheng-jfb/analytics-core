SET low_watermark_datetime = %(low_watermark)s :: TIMESTAMP_LTZ;


CREATE OR REPLACE TEMPORARY TABLE _genesys_conversation_stg AS
SELECT gd.conversationid                            AS conversationid,
       gd.conversationend                           AS conversationend,
       gd.conversationstart                         AS conversationstart,
       gd.mediastatsminconversationmos              AS mediastatsminconversationmos,
       gd.mediastatsminconversationrfactor          AS mediastatsminconversationrfactor,
       gd.originatingdirection                      AS originatingdirection,
       gd.externaltag                               AS externaltag,
       p.value:participantId::STRING                AS participantid,
       CASE
           WHEN NVL(LOWER(s.value:messageType::STRING), '') = 'webmessaging'
           THEN COALESCE(
               JSON_EXTRACT_PATH_TEXT(s.value:flow, 'transferTargetName'),
               p.value:participantName::STRING
           )
           ELSE p.value:participantName::STRING
       END AS participantName,
       p.value:userId::STRING                       AS userid,
       p.value:purpose::STRING                      AS purpose,
       p.value:externalContactId::STRING            AS externalcontactid,
       p.value:externalOrganizationId::STRING       AS externalorganizationid,
       p.value:flaggedReason::STRING                AS flaggedreason,
       s.value:mediaType::STRING                    AS mediatype,
       s.value:sessionId::STRING                    AS sessionid,
       s.value:addressOther::STRING                 AS addressother,
       s.value:addressSelf::STRING                  AS addressself,
       s.value:addressFrom::STRING                  AS addressfrom,
       s.value:addressTo::STRING                    AS addressto,
       s.value:messageType::STRING                  AS messagetype,
       s.value:ani::STRING                          AS ani,
       s.value:direction::STRING                    AS direction,
       s.value:dnis::STRING                         AS dnis,
       s.value:sessionDnis::STRING                  AS sessiondnis,
       s.value:outboundCampaignId::STRING           AS outboundcampaignid,
       s.value:outboundContactId::STRING            AS outboundcontactid,
       s.value:outboundContactListId::STRING        AS outboundcontactlistid,
       s.value:dispositionAnalyzer::STRING          AS dispositionanalyzer,
       s.value:dispositionName::STRING              AS dispositionname,
       s.value:edgeId::STRING                       AS edgeid,
       s.value:remoteNameDisplayable::STRING        AS remotenamedisplayable,
       s.value:roomId::STRING                       AS roomid,
       s.value:monitoredSessionId::STRING           AS monitoredsessionid,
       s.value:monitoredParticipantId::STRING       AS monitoredparticipantid,
       s.value:scriptId::STRING                     AS scriptid,
       s.value:peerId::STRING                       AS peerid,
       s.value:skipEnabled                          AS skipenabled,
       s.value:timeoutSeconds                       AS timeoutseconds,
       s.value:cobrowseRole::STRING                 AS cobrowserole,
       s.value:cobrowseRoomId::STRING               AS cobrowseroomid,
       s.value:mediaBridgeId::STRING                AS mediabridgeid,
       s.value:screenShareAddressSelf::STRING       AS screenshareaddressself,
       s.value:sharingScreen                        AS sharingscreen,
       s.value:screenShareRoomId::STRING            AS screenshareroomid,
       s.value:videoRoomId::STRING                  AS videoroomid,
       s.value:videoAddressSelf::STRING             AS videoaddressself,
       s.value:recording                            AS recording,
       s.value:journeyCustomerId::STRING            AS journeycustomerid,
       s.value:journeyCustomerIdType::STRING        AS journeycustomeridtype,
       s.value:journeyCustomerSessionId::STRING     AS journeycustomersessionid,
       s.value:journeyCustomerSessionIdType::STRING AS journeycustomersessionidtype,
       s.value:journeyActionId::STRING              AS journeyactionid,
       s.value:journeyActionMapId::STRING           AS journeyactionmapid,
       s.value:journeyActionMapVersion::STRING      AS journeyactionmapversion,
       s.value:protocolCallId::STRING               AS protocolcallid,
       s.value:metrics::VARIANT                     AS metrics_info
FROM lake.genesys.conversations_new gd
   , LATERAL FLATTEN(INPUT => participants) p
   , LATERAL FLATTEN(INPUT => p.value:sessions) s
WHERE meta_update_datetime > $low_watermark_datetime;

CREATE OR REPLACE TEMPORARY TABLE _genesys_conversation_metrics_stg AS
SELECT c.value: name::STRING   AS name,
       c.value: value::VARIANT AS value,
       conversationid,
       participantid,
       sessionid
FROM _genesys_conversation_stg
   , LATERAL FLATTEN(INPUT => metrics_info) c;


CREATE OR REPLACE TEMPORARY TABLE _genesys_conversation_metrics AS
SELECT a.*,
       b.name,
       b.value
FROM _genesys_conversation_stg a
    LEFT OUTER JOIN _genesys_conversation_metrics_stg b ON a.conversationid = b.conversationid
    AND a.participantid = b.participantid
    AND a.sessionid = b.sessionid;


CREATE OR REPLACE TEMPORARY TABLE _genesys_conversation_metrics_sum AS
SELECT conversationid                                                             AS conversation_id,
       conversationstart                                                          AS conversation_start,
       conversationend                                                            AS conversation_end,
       mediastatsminconversationmos                                               AS media_stats_min_conversation_mos,
       mediastatsminconversationrfactor                                           AS media_stats_min_conversation_rfactor,
       originatingdirection                                                       AS originating_direction,
       externaltag                                                                AS external_tag,
       participantid                                                              AS participant_id,
       participantname                                                            AS participant_name,
       userid                                                                     AS user_id,
       purpose                                                                    AS purpose,
       externalcontactid                                                          AS external_contact_id,
       externalorganizationid                                                     AS external_organization_id,
       flaggedreason                                                              AS flagged_reason,
       mediatype                                                                  AS media_type,
       sessionid                                                                  AS session_id,
       addressother                                                               AS address_other,
       addressself                                                                AS address_self,
       addressfrom                                                                AS address_from,
       addressto                                                                  AS address_to,
       messagetype                                                                AS message_type,
       ani                                                                        AS ani,
       direction                                                                  AS direction,
       dnis                                                                       AS dnis,
       sessiondnis                                                                AS session_dnis,
       outboundcampaignid                                                         AS outbound_campaign_id,
       outboundcontactid                                                          AS outbound_contact_id,
       outboundcontactlistid                                                      AS outbound_contact_list_id,
       dispositionanalyzer                                                        AS disposition_analyzer,
       dispositionname                                                            AS disposition_name,
       edgeid                                                                     AS edge_id,
       remotenamedisplayable                                                      AS remote_name_displayable,
       roomid                                                                     AS room_id,
       monitoredsessionid                                                         AS monitored_session_id,
       monitoredparticipantid                                                     AS monitored_participant_id,
       scriptid                                                                   AS script_id,
       peerid                                                                     AS peer_id,
       skipenabled                                                                AS skip_enabled,
       timeoutseconds                                                             AS time_out_seconds,
       cobrowserole                                                               AS co_browse_role,
       cobrowseroomid                                                             AS co_browse_roomid,
       mediabridgeid                                                              AS media_bridge_id,
       screenshareaddressself                                                     AS screen_share_address_self,
       sharingscreen                                                              AS sharing_screen,
       screenshareroomid                                                          AS screen_share_roomid,
       videoroomid                                                                AS video_roomid,
       videoaddressself                                                           AS video_address_self,
       recording                                                                  AS recording,
       journeycustomerid                                                          AS journey_customer_id,
       journeycustomeridtype                                                      AS journey_customer_id_type,
       journeycustomersessionid                                                   AS journey_customer_session_id,
       journeycustomersessionidtype                                               AS journey_customer_session_id_type,
       journeyactionid                                                            AS journey_action_id,
       journeyactionmapid                                                         AS journey_action_map_id,
       journeyactionmapversion                                                    AS journey_action_map_version,
       protocolcallid                                                             AS protocol_call_id,
       "'tAcd'"                                                                   AS t_acd,
       "'tAnswered'"                                                              AS t_answered,
       "'tFlowOut'"                                                               AS t_flowout,
       "'tTalk'"                                                                  AS t_talk,
       "'tTalkComplete'"                                                          AS t_talk_complete,
       "'tHandle'"                                                                AS t_handle,
       "'tDialing'"                                                               AS t_dialing,
       "'tContacting'"                                                            AS t_contacting,
       "'tHeld'"                                                                  AS t_held,
       "'tHeldComplete'"                                                          AS t_held_complete,
       "'oInteracting'"                                                           AS o_interacting,
       "'oWaiting'"                                                               AS o_waiting,
       "'nTransferred'"                                                           AS n_transferred,
       "'nError'"                                                                 AS n_error,
       "'nStateTransitionError'"                                                  AS n_state_transition_error,
       "'tAlert'"                                                                 AS t_alert,
       "'tNotResponding'"                                                         AS t_not_responding,
       "'oServiceLevel'"                                                          AS o_service_level,
       "'oServiceTarget'"                                                         AS o_service_target,
       "'nOverSla'"                                                               AS n_over_sla,
       "'tWait'"                                                                  AS t_wait,
       "'tIvr'"                                                                   AS t_ivr,
       "'tAbandon'"                                                               AS t_abandon,
       "'tVoicemail'"                                                             AS t_voicemail,
       "'tAcw'"                                                                   AS t_acw,
       "'nOutboundAttempted'"                                                     AS n_outbound_attempted,
       "'nOutboundConnected'"                                                     AS n_outbound_connected,
       "'nOutboundAbandoned'"                                                     AS n_outbound_abandoned,
       "'nOutbound'"                                                              AS n_outbound,
       "'tUserResponseTime'"                                                      AS t_user_response_time,
       "'tAgentResponseTime'"                                                     AS t_agent_response_time,
       TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE('US/Pacific', conversationstart)) AS conversation_start_hq,
       "'nConsult'"                                                               AS n_consult
FROM _genesys_conversation_metrics
    PIVOT (SUM(value) FOR name IN (
        'tAcd',
        'tAnswered',
        'tFlowOut',
        'tTalk',
        'tTalkComplete',
        'tHandle',
        'tDialing',
        'tContacting',
        'tHeld',
        'tHeldComplete',
        'oInteracting',
        'oWaiting',
        'nTransferred',
        'nError',
        'nStateTransitionError',
        'tAlert',
        'tNotResponding',
        'oServiceLevel',
        'oServiceTarget',
        'nOverSla',
        'tWait',
        'tIvr',
        'tAbandon',
        'tVoicemail',
        'tAcw',
        'nOutboundAttempted',
        'nOutboundConnected',
        'nOutboundAbandoned',
        'nOutbound',
        'tUserResponseTime',
        'tAgentResponseTime',
        'nConsult'
        ))
ORDER BY conversationid, participantid, sessionid;


MERGE INTO reporting_prod.gms.genesys_conversation_new t
    USING (SELECT *
                FROM (select *,
                        HASH(*) as meta_row_hash,
                        row_number()
                            OVER (PARTITION BY conversation_id,participant_id,session_id
                                ORDER BY coalesce(conversation_end,'1900-01-01') DESC) AS rn
                        FROM _genesys_conversation_metrics_sum) gcm
                where rn = 1) s ON equal_null(t.conversation_id, s.conversation_id)
                                AND equal_null(t.participant_id, s.participant_id)
                                AND equal_null(t.session_id, s.session_id)
    WHEN NOT MATCHED THEN INSERT (conversation_id, conversation_start, conversation_end,
                                  media_stats_min_conversation_mos, media_stats_min_conversation_rfactor,
                                  originating_direction, external_tag, participant_id, participant_name, user_id, purpose,
                                  external_contact_id, external_organization_id, flagged_reason, media_type, session_id,
                                  address_other, address_self, address_from, address_to, message_type, ani, direction,
                                  dnis, session_dnis, outbound_campaign_id, outbound_contact_id,
                                  outbound_contact_list_id, disposition_analyzer, disposition_name, edge_id,
                                  remote_name_displayable, room_id, monitored_session_id, monitored_participant_id,
                                  script_id, peer_id, skip_enabled, time_out_seconds, co_browse_role, co_browse_roomid,
                                  media_bridge_id, screen_share_address_self, sharing_screen, screen_share_roomid,
                                  video_roomid, video_address_self, recording, journey_customer_id,
                                  journey_customer_id_type, journey_customer_session_id,
                                  journey_customer_session_id_type, journey_action_id, journey_action_map_id,
                                  journey_action_map_version, protocol_call_id, t_acd, t_answered, t_flowout, t_talk,
                                  t_talk_complete, t_handle, t_dialing, t_contacting, t_held, t_held_complete,
                                  o_interacting, o_waiting, n_transferred, n_error, n_state_transition_error, t_alert,
                                  t_not_responding, o_service_level, o_service_target, n_over_sla, t_wait, t_ivr,
                                  t_abandon, t_voicemail, t_acw, n_outbound_attempted, n_outbound_connected,
                                  n_outbound_abandoned, n_outbound, t_user_response_time, t_agent_response_time,
                                  conversation_start_hq,meta_row_hash, n_consult
        )
        VALUES (conversation_id, conversation_start, conversation_end, media_stats_min_conversation_mos,
                media_stats_min_conversation_rfactor, originating_direction, external_tag, participant_id, participant_name, user_id,
                purpose, external_contact_id, external_organization_id, flagged_reason, media_type, session_id,
                address_other, address_self, address_from, address_to, message_type, ani, direction, dnis, session_dnis,
                outbound_campaign_id, outbound_contact_id, outbound_contact_list_id, disposition_analyzer,
                disposition_name, edge_id, remote_name_displayable, room_id, monitored_session_id,
                monitored_participant_id, script_id, peer_id, skip_enabled, time_out_seconds, co_browse_role,
                co_browse_roomid, media_bridge_id, screen_share_address_self, sharing_screen, screen_share_roomid,
                video_roomid, video_address_self, recording, journey_customer_id, journey_customer_id_type,
                journey_customer_session_id, journey_customer_session_id_type, journey_action_id, journey_action_map_id,
                journey_action_map_version, protocol_call_id, t_acd, t_answered, t_flowout, t_talk, t_talk_complete,
                t_handle, t_dialing, t_contacting, t_held, t_held_complete, o_interacting, o_waiting, n_transferred,
                n_error, n_state_transition_error, t_alert, t_not_responding, o_service_level, o_service_target,
                n_over_sla, t_wait, t_ivr, t_abandon, t_voicemail, t_acw, n_outbound_attempted, n_outbound_connected,
                n_outbound_abandoned, n_outbound, t_user_response_time, t_agent_response_time, conversation_start_hq,meta_row_hash, n_consult)
    WHEN MATCHED AND s.meta_row_hash != t.meta_row_hash
        AND coalesce(s.conversation_end, '1900-01-01') > coalesce(t.conversation_end, '1900-01-01') THEN
        UPDATE
            SET t.conversation_start = s.conversation_start
                ,t.conversation_end = s.conversation_end
                ,t.media_stats_min_conversation_mos = s.media_stats_min_conversation_mos
                ,t.media_stats_min_conversation_rfactor = s.media_stats_min_conversation_rfactor
                ,t.originating_direction = s.originating_direction
                ,t.external_tag = s.external_tag
                ,t.participant_name = s.participant_name
                ,t.user_id = s.user_id
                ,t.purpose = s.purpose
                ,t.external_contact_id = s.external_contact_id
                ,t.external_organization_id = s.external_organization_id
                ,t.flagged_reason = s.flagged_reason
                ,t.media_type = s.media_type
                ,t.address_other = s.address_other
                ,t.address_self = s.address_self
                ,t.address_from = s.address_from
                ,t.address_to = s.address_to
                ,t.message_type = s.message_type
                ,t.ani = s.ani
                ,t.direction = s.direction
                ,t.dnis = s.dnis
                ,t.session_dnis = s.session_dnis
                ,t.outbound_campaign_id = s.outbound_campaign_id
                ,t.outbound_contact_id = s.outbound_contact_id
                ,t.outbound_contact_list_id = s.outbound_contact_list_id
                ,t.disposition_analyzer = s.disposition_analyzer
                ,t.disposition_name = s.disposition_name
                ,t.edge_id = s.edge_id
                ,t.remote_name_displayable = s.remote_name_displayable
                ,t.room_id = s.room_id
                ,t.monitored_session_id = s.monitored_session_id
                ,t.monitored_participant_id = s.monitored_participant_id
                ,t.script_id = s.script_id
                ,t.peer_id = s.peer_id
                ,t.skip_enabled = s.skip_enabled
                ,t.time_out_seconds = s.time_out_seconds
                ,t.co_browse_role = s.co_browse_role
                ,t.co_browse_roomid = s.co_browse_roomid
                ,t.media_bridge_id = s.media_bridge_id
                ,t.screen_share_address_self = s.screen_share_address_self
                ,t.sharing_screen = s.sharing_screen
                ,t.screen_share_roomid = s.screen_share_roomid
                ,t.video_roomid = s.video_roomid
                ,t.video_address_self = s.video_address_self
                ,t.recording = s.recording
                ,t.journey_customer_id = s.journey_customer_id
                ,t.journey_customer_id_type = s.journey_customer_id_type
                ,t.journey_customer_session_id = s.journey_customer_session_id
                ,t.journey_customer_session_id_type = s.journey_customer_session_id_type
                ,t.journey_action_id = s.journey_action_id
                ,t.journey_action_map_id = s.journey_action_map_id
                ,t.journey_action_map_version = s.journey_action_map_version
                ,t.protocol_call_id = s.protocol_call_id
                ,t.t_acd = s.t_acd
                ,t.t_answered = s.t_answered
                ,t.t_flowout = s.t_flowout
                ,t.t_talk = s.t_talk
                ,t.t_talk_complete = s.t_talk_complete
                ,t.t_handle = s.t_handle
                ,t.t_dialing = s.t_dialing
                ,t.t_contacting = s.t_contacting
                ,t.t_held = s.t_held
                ,t.t_held_complete = s.t_held_complete
                ,t.o_interacting = s.o_interacting
                ,t.o_waiting = s.o_waiting
                ,t.n_transferred = s.n_transferred
                ,t.n_error = s.n_error
                ,t.n_state_transition_error = s.n_state_transition_error
                ,t.t_alert = s.t_alert
                ,t.t_not_responding = s.t_not_responding
                ,t.o_service_level = s.o_service_level
                ,t.o_service_target = s.o_service_target
                ,t.n_over_sla = s.n_over_sla
                ,t.t_wait = s.t_wait
                ,t.t_ivr = s.t_ivr
                ,t.t_abandon = s.t_abandon
                ,t.t_voicemail = s.t_voicemail
                ,t.t_acw = s.t_acw
                ,t.n_outbound_attempted = s.n_outbound_attempted
                ,t.n_outbound_connected = s.n_outbound_connected
                ,t.n_outbound_abandoned = s.n_outbound_abandoned
                ,t.n_outbound = s.n_outbound
                ,t.t_user_response_time = s.t_user_response_time
                ,t.t_agent_response_time = s.t_agent_response_time
                ,t.conversation_start_hq = s.conversation_start_hq
                ,t.meta_row_hash = s.meta_row_hash
                ,t.meta_update_datetime = CURRENT_TIMESTAMP
                ,t.n_consult = s.n_consult;

CREATE OR REPLACE TEMP TABLE _conv_ids_workflow AS
SELECT DISTINCT conversationId, peerId, messageType, participantName
FROM _genesys_conversation_stg
WHERE LOWER(messageType) = 'webmessaging' AND LOWER(purpose) = 'workflow' AND participantName IS NOT NULL;

MERGE INTO reporting_prod.gms.genesys_conversation_new a
USING _conv_ids_workflow b
ON a.conversation_id = b.conversationId AND a.peer_id = b.peerId AND LOWER(a.purpose) = 'acd'
WHEN MATCHED THEN UPDATE SET a.participant_name = b.participantName;
