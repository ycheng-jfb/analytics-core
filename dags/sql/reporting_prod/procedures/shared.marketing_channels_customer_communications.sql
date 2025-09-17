SET email_blast_sends_watermark = (
    SELECT IFNULL(DATEADD(hour, -48, (
        SELECT MAX(event_time)
        FROM reporting_prod.shared.marketing_channels_customer_communications
        WHERE source_table = 'lake.iterable.email_blast_sends'
    )), '1990-01-01')
);
SET email_triggered_sends_watermark = (
    SELECT IFNULL(DATEADD(hour, -48, (
        SELECT MAX(event_time)
        FROM reporting_prod.shared.marketing_channels_customer_communications
        WHERE source_table = 'lake.iterable.email_triggered_sends'
    )), '1990-01-01')
);
SET push_sends_watermark = (
    SELECT IFNULL(DATEADD(hour, -48, (
        SELECT MAX(event_time)
        FROM reporting_prod.shared.marketing_channels_customer_communications
        WHERE source_table = 'lake.iterable.push_sends'
    )), '1990-01-01')
);
SET sms_sends_watermark = (
    SELECT IFNULL(DATEADD(hour, -48, (
        SELECT MAX(event_time)
        FROM reporting_prod.shared.marketing_channels_customer_communications
        WHERE source_table = 'lake.iterable.sms_sends'
    )), '1990-01-01')
);


CREATE OR REPLACE TEMPORARY TABLE _email_blast_sends_delta
AS
SELECT
    TRY_TO_NUMBER(u.user_id)                            AS customer_id,
    s.store_group_id                                    AS store_group_id,
    ebs.project_id                                      AS source_store_id,
    NULL                                                AS contact_id,
    'email'                                             AS type,
    ebs.created_at                                      AS event_time,
    'lake.iterable.email_blast_sends'                   AS source_table,
    ebs.template_id                                     AS campaign_id,
    ebs.template_name                                   AS campaign_name,
    ebs.email_subject                                   AS subject,
    IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(ebs.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
    LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(ebs.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),NULL) AS version_name
FROM campaign_event_data.org_3223.email_blast_sends_view ebs
LEFT JOIN  campaign_event_data.org_3223.users u
    ON ebs.itbl_user_id = u.itbl_user_id
LEFT JOIN EDW_PROD.DATA_MODEL.DIM_CUSTOMER cust
    ON edw_prod.stg.udf_unconcat_brand(cust.customer_id) = TRY_TO_NUMBER(u.user_id)
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.STORE s
    ON cust.store_id = s.store_id
WHERE
    ebs.created_at > $email_blast_sends_watermark;


CREATE OR REPLACE TEMPORARY TABLE _email_triggered_sends_delta
AS
SELECT
    TRY_TO_NUMBER(u.user_id)                            AS customer_id,
    s.store_group_id                                    AS store_group_id,
    ets.project_id                                      AS source_store_id,
    NULL                                                AS contact_id,
    'email'                                             AS type,
    ets.created_at                                      AS event_time,
    'lake.iterable.email_triggered_sends'               AS source_table,
    ets.template_id                                     AS campaign_id,
    ets.campaign_name                                   AS campaign_name,
    ets.email_subject                                   AS subject,
    IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(ets.campaign_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
    LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(ets.campaign_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),NULL) AS version_name
FROM campaign_event_data.org_3223.email_triggered_sends_view ets
LEFT JOIN  campaign_event_data.org_3223.users u
    ON ets.itbl_user_id = u.itbl_user_id
LEFT JOIN EDW_PROD.DATA_MODEL.DIM_CUSTOMER cust
    ON edw_prod.stg.udf_unconcat_brand(cust.customer_id) = TRY_TO_NUMBER(u.user_id)
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.STORE s
    ON cust.store_id = s.store_id
WHERE
    ets.created_at > $email_triggered_sends_watermark;


CREATE OR REPLACE TEMPORARY TABLE _push_sends_delta
AS
SELECT
    TRY_TO_NUMBER(u.user_id)                            AS customer_id,
    s.store_group_id                                    AS store_group_id,
    ps.project_id                                       AS source_store_id,
    NULL                                                AS contact_id,
    'push'                                              AS type,
    ps.created_at                                       AS event_time,
    'lake.iterable.push_sends'                          AS source_table,
    ps.template_id                                      AS campaign_id,
    ps.template_name                                    AS campaign_name,
    NULL                                                AS subject,
    IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(ps.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
    LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(ps.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),NULL) AS version_name
FROM campaign_event_data.org_3223.push_sends_view AS ps
LEFT JOIN  campaign_event_data.org_3223.users u
    ON ps.itbl_user_id = u.itbl_user_id
LEFT JOIN EDW_PROD.DATA_MODEL.DIM_CUSTOMER cust
    ON edw_prod.stg.udf_unconcat_brand(cust.customer_id) = TRY_TO_NUMBER(u.user_id)
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.STORE s
    ON cust.store_id = s.store_id
WHERE
    ps.created_at > $push_sends_watermark;


CREATE OR REPLACE TEMPORARY TABLE _sms_sends_delta
AS
SELECT
    TRY_TO_NUMBER(u.user_id)                             AS customer_id,
    s.store_group_id                                     AS store_group_id,
    sms.project_id                                       AS source_store_id,
    sms.from_phone_number_id                             AS contact_id,
    'sms'                                                AS type,
    sms.created_at                                       AS event_time,
    'lake.iterable.sms_sends'                            AS source_table,
    sms.template_id                                      AS campaign_id,
    sms.template_name                                    AS campaign_name,
    NULL                                                 AS subject,
    IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(sms.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
    LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(sms.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),NULL) AS version_name
FROM campaign_event_data.org_3223.sms_sends_view AS sms
LEFT JOIN campaign_event_data.org_3223.campaigns c
    ON sms.campaign_id = c.id
LEFT JOIN  campaign_event_data.org_3223.users u
    ON sms.itbl_user_id = u.itbl_user_id
LEFT JOIN EDW_PROD.DATA_MODEL.DIM_CUSTOMER cust
    ON edw_prod.stg.udf_unconcat_brand(cust.customer_id) = TRY_TO_NUMBER(u.user_id)
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.STORE s
    ON cust.store_id = s.store_id
WHERE
    sms.created_at > $sms_sends_watermark;


MERGE INTO reporting_prod.shared.marketing_channels_customer_communications AS target
USING (
/*
    !!! UNCOMMENT IF YOU NEED TO REBUILD THE TABLE !!!
    (
    WITH push_campaigns AS (
        SELECT
            campaign_id,
            customer_id,
            name
        FROM (
            SELECT
                campaign_id,
                customer_id,
                name,
                ROW_NUMBER() OVER (PARTITION BY CAMPAIGN_ID, customer_id ORDER BY EVENT_TIME DESC) AS row_n
            FROM lake.emarsys.push_campaigns
            )
        WHERE row_n = 1
    ),

    am AS (
        SELECT
            source_id,
            store_id
        FROM lake_view.sharepoint.med_account_mapping_media
        WHERE source = 'Attentive'
    ),

    b AS (
        SELECT
            id,
            sailthru_account_id,
            name
        FROM (
            SELECT
                id,
                sailthru_account_id,
                name,
                ROW_NUMBER() OVER (PARTITION BY id, sailthru_account_id ORDER BY updated_at DESC) AS row_n
            FROM lake.sailthru.data_exporter_blast
        )
        WHERE row_n = 1
    ),

    email_campaigns AS (
        SELECT
            campaign_id,
            customer_id,
            name,
            subject,
            version_name
        FROM (
            SELECT
                campaign_id,
                customer_id,
                name,
                subject,
                version_name,
                ROW_NUMBER() OVER (PARTITION BY CAMPAIGN_ID, customer_id ORDER BY EVENT_TIME DESC) AS row_n
            FROM lake.emarsys.email_campaigns_v2
        )
        WHERE row_n = 1
    ),

    sub AS (
        SELECT
            emarsys_user_id,
            brand,
            customer_id
        FROM (
            SELECT
                emarsys_user_id,
                brand,
                customer_id,
                ROW_NUMBER() OVER (PARTITION BY emarsys_user_id, brand ORDER BY updated_at DESC) AS row_n
            FROM lake.emarsys.email_subscribes
            )
        WHERE row_n = 1
    ),

    message_clean AS (
        SELECT
            message_id::VARCHAR as message_id,
            message_type_clean
        FROM (
            SELECT DISTINCT
                message_id,
                message_name
            FROM lake.media.vw_attentive_attentive_sms_legacy
        ) AS id
        JOIN (
            SELECT DISTINCT
                message_name,
                IFF(CONTAINS(message_type, 'https'),
                    SUBSTR(LOWER(message_type), 1,
                    REGEXP_INSTR(LOWER(message_type), 'https:', 1, 1, 1) -
                    (LENGTH('https:') + 2)),
                message_type) AS message_type_clean
        FROM lake.media.vw_attentive_attentive_sms_legacy
        WHERE message_type IS NOT NULL
        QUALIFY ROW_NUMBER() OVER(PARTITION BY message_name ORDER BY timestamp DESC) = 1
        ) AS clean
            ON id.message_name = clean.message_name
    )
*/
    SELECT
        A.customer_id,
        A.store_group_id,
        A.source_store_id AS crm_source_id,
        REPLACE(LOWER(csm.store_group), ' ', '_') as store_brand,
        A.contact_id,
        A.type,
        A.event_time,
        A.source_table,
        A.campaign_id AS campaign_id,
        A.campaign_name AS campaign_name,
        A.subject,
        A.version_name
    FROM (
            SELECT * FROM _email_blast_sends_delta
            UNION
            SELECT * FROM _email_triggered_sends_delta
            UNION
            SELECT * FROM _push_sends_delta
            UNION
            SELECT * FROM _sms_sends_delta
/*
        !!! UNCOMMENT IF YOU NEED TO REBUILD THE TABLE !!!
        UNION
        SELECT
            ps.extid                                    AS customer_id,
            am.store_id                                 AS store_group_id,
            NULL                                        AS store_id,
            mb.profile_id::VARCHAR                      AS contact_id,
            'email'                                     AS type,
            mb.send_time                                AS event_time,
            'lake.sailthru.data_exporter_message_blast' AS source_table,
            mb.sailthru_account_id                      AS source_store_id,
            mb.blast_id                                 AS campaign_creative_id,
            b.name                                      AS campaign_creative_name,
            mb.message_id::VARCHAR                      AS message_id,
            NULL                                        AS subject,
            NULL                                        AS version_name,
            NULL                                        AS message_name
        FROM lake.sailthru.data_exporter_message_blast mb
        LEFT JOIN reporting.crm.sailthru_data_exporter_profile_summary ps
            ON mb.profile_id = ps.id
        LEFT JOIN lake.sailthru.account_mapping am
            ON mb.sailthru_account_id = am.sailthru_account_id
        LEFT JOIN b
            ON mb.blast_id = b.id
            AND mb.sailthru_account_id = b.sailthru_account_id
        SELECT
            sub.customer_id           AS customer_id,
            sm.store_group_id,
            NULL                      AS store_id,
            ps.contact_id::VARCHAR as contact_id,
            'push'                    AS type,
            ps.event_time,
            'lake.emarsys.push_sends' AS source_table,
            ps.customer_id            AS source_store_id,
            ps.CAMPAIGN_ID            AS campaign_creative_id,
            pc.name                   AS campaign_creative_name,
            NULL                      AS message_id,
            NULL                      AS subject,
            NULL                      AS version_name,
            NULL                      AS message_name
        FROM lake_view.emarsys.push_sends AS ps
        LEFT JOIN lake.emarsys.customer_store_mapping AS sm
            ON ps.customer_id = sm.customer_id
        LEFT JOIN push_campaigns AS pc
            ON ps.campaign_id = pc.campaign_id
            AND ps.customer_id = pc.customer_id
        LEFT JOIN sub
            ON sub.EMARSYS_USER_ID = ps.CONTACT_ID
            AND sub.BRAND = ps.STORE_GROUP
        WHERE
            ps.event_time > (
                SELECT IFNULL((
                    SELECT MAX(event_time)
                    FROM reporting_prod.shared.marketing_channels_customer_communications
                    WHERE source_table = 'lake.emarsys.push_sends'
                ), '1990-01-01')
            )

        UNION
        SELECT
            sms.customer_id,
            s.store_group_id,
            am.store_id,
            IFF(sms.phone IS NOT NULL, sms.phone, sms.visitor_id)::VARCHAR AS contact_id,
            'sms'                                                          AS type,
            sms.timestamp                                                  AS event_time,
            'med_db_staging.attentive.vw_attentive_sms'                    AS source_table,
            sms.company_id                                                 AS source_store_id,
            NULLIF(sms.creative_id, '')                                    AS campaign_creative_id,
            sms.creative_name                                              AS campaign_creative_name,
            NULLIF(sms.message_id, '')::VARCHAR                            AS message_id,
            mc.message_type_clean                                          AS subject,
            NULL                                                           AS version_name,
            sms.message_name                                               AS message_name
        FROM lake.media.vw_attentive_attentive_sms_legacy AS sms
        LEFT JOIN am
            ON sms.company_id = am.source_id
        LEFT JOIN lake_consolidated_view.ultra_merchant.store AS s
            ON am.store_id = s.store_id
        LEFT JOIN message_clean AS mc
            ON sms.message_id = mc.message_id
        WHERE
            sms.timestamp > (
                SELECT IFNULL((
                    SELECT MAX(event_time)
                    FROM reporting_prod.shared.marketing_channels_customer_communications
                    WHERE source_table = 'med_db_staging.attentive.vw_attentive_sms'
                ), '1990-01-01')
            )
            AND UPPER(sms.type) = 'MESSAGE_RECEIPT'

        UNION
        SELECT
            sub.customer_id            AS customer_id,
            sm.store_group_id,
            NULL                       AS store_id,
            es.contact_id::VARCHAR as contact_id,
            'email'                    AS type,
            es.event_time,
            'lake.emarsys.email_sends' AS source_table,
            es.customer_id             AS source_store_id,
            es.CAMPAIGN_ID             AS campaign_creative_id,
            ec.name                    AS campaign_creative_name,
            es.message_id::VARCHAR     AS message_id,
            ec.subject                 AS subject,
            ec.version_name            AS version_name,
            NULL                       AS message_name
        FROM lake_view.emarsys.email_sends AS es
        LEFT JOIN lake.emarsys.customer_store_mapping AS sm
            ON es.customer_id = sm.customer_id
        LEFT JOIN email_campaigns AS ec
            ON es.campaign_id = ec.campaign_id
            AND es.customer_id = ec.customer_id
        LEFT JOIN sub
            ON sub.EMARSYS_USER_ID = es.CONTACT_ID
            AND sub.BRAND = es.STORE_GROUP
        WHERE
            es.event_time > (
                SELECT IFNULL((
                    SELECT MAX(event_time)
                    FROM reporting_prod.shared.marketing_channels_customer_communications
                    WHERE source_table = 'lake.emarsys.email_sends'
                ), '1990-01-01')
            )
 */
        ) AS A
/*
    LEFT JOIN edw_prod.data_model.DIM_STORE AS ds
        ON ds.store_id = A.store_id
    LEFT JOIN lake.emarsys.customer_store_mapping AS em
        ON em.CUSTOMER_ID = A.source_store_id
    LEFT JOIN lake.sailthru.account_mapping AS st
        ON st.sailthru_account_id = A.source_store_id
 */
    LEFT JOIN iterable.public.customer_store_mapping csm
        ON csm.store_group_id = A.store_group_id
    ORDER BY event_time ASC
) AS source
ON equal_null(target.customer_id, source.customer_id)
    AND equal_null(target.store_group_id::VARCHAR, source.store_group_id::VARCHAR)
    AND equal_null(target.crm_source_id, source.crm_source_id)
    AND equal_null(target.store_brand::VARCHAR, source.store_brand::VARCHAR)
    AND equal_null(target.contact_id::VARCHAR, source.contact_id::VARCHAR)
    AND equal_null(target.type, source.type)
    AND equal_null(target.event_time, source.event_time)
    AND equal_null(target.source_table, source.source_table)
    AND equal_null(target.campaign_id, source.campaign_id)
    AND equal_null(target.campaign_name::VARCHAR, source.campaign_name::VARCHAR)
    AND equal_null(target.subject::VARCHAR, source.subject::VARCHAR)
    AND equal_null(target.version_name::VARCHAR, source.version_name::VARCHAR)
WHEN NOT MATCHED THEN
    INSERT (
        customer_id,
        store_group_id,
        crm_source_id,
        store_brand,
        contact_id,
        type,
        event_time,
        source_table,
        campaign_id,
        campaign_name,
        subject,
        version_name
    )
    VALUES (
        source.customer_id,
        source.store_group_id,
        source.crm_source_id,
        source.store_brand,
        source.contact_id,
        source.type,
        source.event_time,
        source.source_table,
        source.campaign_id,
        source.campaign_name,
        source.subject,
        source.version_name
    );
