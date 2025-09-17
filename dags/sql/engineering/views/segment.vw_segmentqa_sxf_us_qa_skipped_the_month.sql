CREATE OR REPLACE VIEW engineering.segment.vw_segmentqa_sxf_us_qa_skipped_the_month AS
SELECT DISTINCT
    TO_TIMESTAMP(RECORD_METADATA:CreateTime::string) AS create_time,
    RECORD_METADATA:key::string AS key,
    RECORD_METADATA:offset::NUMBER(38,0) AS offset,
    RECORD_METADATA:partition::NUMBER(38,0) AS partition,
    RECORD_METADATA:topic::string AS topic,
    RECORD_CONTENT:correlationid::string AS correlation_id,
    parse_json(RECORD_CONTENT:data):automated_test::STRING AS automated_test,
    parse_json(RECORD_CONTENT:data):category::STRING AS category,
    parse_json(RECORD_CONTENT:data):correlation_id::STRING AS data_correlation_id,
    parse_json(RECORD_CONTENT:data):customer_id::STRING AS customer_id,
    parse_json(RECORD_CONTENT:data):customer_ip::STRING AS customer_ip,
    parse_json(RECORD_CONTENT:data):page_hostname::STRING AS page_hostname,
    parse_json(RECORD_CONTENT:data):session_id::STRING AS session_id,
    parse_json(RECORD_CONTENT:data):store_group_id::STRING AS store_group_id,
    parse_json(RECORD_CONTENT:data):store_id::STRING AS store_id,
    parse_json(RECORD_CONTENT:data):visitor_id::STRING AS visitor_id,
    RECORD_CONTENT:date::date AS date,
    RECORD_CONTENT:id::string AS id,
    RECORD_CONTENT:ip::string AS ip,
    RECORD_CONTENT:source::string AS source,
    RECORD_CONTENT:specversion::NUMBER(38,0) AS spec_version,
    RECORD_CONTENT:status::string AS status,
    RECORD_CONTENT:subject::string AS subject,
    RECORD_CONTENT:success::string AS success,
    RECORD_CONTENT:time::timestamp AS time,
    RECORD_CONTENT:type::string AS type
FROM engineering.segment.segmentqa_sxf_us_qa_skipped_the_month
  , LATERAL FLATTEN (input => RECORD_METADATA) a
  , LATERAL FLATTEN (input => RECORD_CONTENT) b;
