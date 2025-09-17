CREATE TRANSIENT TABLE IF NOT EXISTS lake.genesys.conversations_history (
    -- Original table columns
    conversationId STRING,
    conversationEnd TIMESTAMP_LTZ(3),
    conversationStart TIMESTAMP_LTZ(3),
    mediaStatsMinConversationMos NUMBER(38,16),
    mediaStatsMinConversationRFactor NUMBER(38,16),
    originatingDirection STRING,
    participants VARIANT,
    externalTag STRING,
    source_filename STRING,
    last_modified TIMESTAMP_LTZ(3),
    meta_update_datetime_src TIMESTAMP_LTZ(3),

    -- History tracking columns
    IS_CURRENT BOOLEAN,
    EFFECTIVE_START_DATETIME TIMESTAMP_LTZ(3),
    EFFECTIVE_END_DATETIME TIMESTAMP_LTZ(3),
    META_UPDATE_DATETIME TIMESTAMP_LTZ(3)
);
