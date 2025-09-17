BEGIN;

DELETE FROM lake_stg.excel.kpi_scorecard_etech_stg;

COPY INTO lake_stg.excel.kpi_scorecard_etech_stg (sheet_name, week, internal_qc_score_voice, internal_alert_rate_voice, internal_qc_score_chat_email, internal_alert_rate_chat_email)
FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.kpi_scorecard_etech/v2/'
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
)
ON_ERROR = 'SKIP_FILE_1%'
;

CREATE OR REPLACE TEMP TABLE _store_details
(
    division VARCHAR,
    region VARCHAR,
    country VARCHAR,
    bu VARCHAR,
    language VARCHAR,
    campaign VARCHAR
);

INSERT INTO _store_details (division, region, country, bu, language, campaign) VALUES
        ('FF', 'EU', 'SE', 'JF', 'English', 'JF SE'),
        ('FF', 'EU', 'DK', 'JF', 'English', 'JF DK'),
        ('FF', 'EU', 'NL', 'JF', 'English', 'JF NL'),
        ('FL', 'AU', 'AU', 'FL', 'English', 'FL AU'),
        ('SXF', 'EU', 'NL', 'SXF', 'English', 'SXF NL'),
        ('SXF', 'EU', 'DK', 'SXF', 'English', 'SXF DK'),
        ('SXF', 'EU', 'SE', 'SXF', 'English', 'SXF SE'),
        ('FF', 'NA', 'US', 'SD', 'English', 'SD NA'),
        ('FF', 'NA', 'US', 'JF', 'Spanish', 'JF NA'),
        ('FL', 'EU', 'FR', 'FL', 'English', 'FL FR'),
        ('FL', 'NA', 'US', 'FL', 'English', 'FL NA'),
        ('GL', 'NA', 'US', 'GL', 'English', 'GLOBAL'),
        ('NA', 'NA', 'US', 'NA', 'English', 'NA'),
        ('SXF', 'NA', 'US', 'SXF', 'English', 'SXF NA'),
        ('FL', 'EU', 'ES', 'FL', 'English', 'FL ES'),
        ('FF', 'NA', 'US', 'FK', 'English', 'FK NA'),
        ('EU', 'EU', 'EU', 'EU', 'English', 'EU'),
        ('SXF', 'EU', 'UK', 'SXF', 'English', 'SXF UK'),
        ('FF', 'EU', 'ES', 'JF', 'English', 'JF ES'),
        ('SXF', 'EU', 'DE', 'SXF', 'English', 'SXF DE'),
        ('FF', 'NA', 'US', 'SD', 'Spanish', 'SD NA'),
        ('FF', 'EU', 'DE', 'JF', 'English', 'JF DE'),
        ('FF', 'EU', 'FR', 'JF', 'English', 'JF FR'),
        ('FL', 'EU', 'UK', 'FL', 'English', 'FL UK'),
        ('SXF', 'EU', 'ES', 'SXF', 'English', 'SXF ES'),
        ('SXF', 'EU', 'FR', 'SXF', 'English', 'SXF FR'),
        ('FF', 'EU', 'UK', 'JF', 'English', 'JF UK'),
        ('FL', 'EU', 'DE', 'FL', 'English', 'FL DE'),
        ('FF', 'NA', 'US', 'JF', 'English', 'JF NA');

ALTER SESSION SET WEEK_START = 4;

CREATE OR REPLACE TEMP TABLE _kpi_scorecard_etech_stg LIKE lake.excel.kpi_scorecard_etech;

ALTER TABLE _kpi_scorecard_etech_stg
    DROP COLUMN meta_create_datetime, meta_update_datetime, meta_row_hash;

INSERT INTO _kpi_scorecard_etech_stg
(
    region,
    bu,
    division,
    country,
    language,
    campaign,
    date,
    internal_qc_score_voice,
    internal_alert_rate_voice,
    internal_qc_score_chat_email,
    internal_alert_rate_chat_email
)
SELECT
    CASE
        WHEN upper(trim(sheet_name)) IN ('NORTH AMERICA', 'GLOBAL') OR
             upper(trim(sheet_name)) LIKE ANY ('%NA%', '%US%', '%CA%')
            THEN 'NA'
        WHEN upper(trim(sheet_name)) = 'EUROPE'
            THEN 'EU'
        WHEN upper(trim(sheet_name)) LIKE ANY ('%JFNA%', '%SDNA%') THEN 'NA'
        ELSE 'EU'
    END AS region,
    CASE
        WHEN upper(trim(sheet_name)) = 'NORTH AMERICA'
            THEN 'NA'
        WHEN upper(trim(sheet_name)) = 'EUROPE'
            THEN 'EU'
        WHEN upper(left(trim(sheet_name), 2)) = 'SX'
            THEN 'SXF'
        ELSE upper(left(trim(sheet_name), 2))
    END AS bu,
    IFF(bu in ('SD', 'JF', 'FK'), 'FF', bu) as division,
    CASE
        WHEN region = 'NA' THEN 'US'
        WHEN upper(trim(sheet_name)) = 'EUROPE' THEN 'EU'
        WHEN upper(trim(sheet_name)) ILIKE '%ROE%' THEN 'ROE'
        ELSE upper(right(trim(sheet_name), 2))
    END AS country,
    IFF(upper(sheet_name) like any ('%SPA%','%SP%', '%SPANISH%'), 'Spanish', 'English') AS language,
    CASE
        WHEN upper(trim(sheet_name)) IN ('NORTH AMERICA') THEN 'NA'
        WHEN upper(trim(sheet_name)) IN ('EUROPE') THEN 'EU'
        WHEN upper(trim(sheet_name)) LIKE '%JFNA%' THEN 'JF NA'
        WHEN upper(trim(sheet_name)) LIKE '%SDNA%' THEN 'SD NA'
        WHEN upper(trim(sheet_name)) LIKE '%FLNA%' THEN 'FL NA'
        ELSE upper(trim(sheet_name))
    END AS campaign,
    DATEADD('week', cast(replace(trim(upper(WEEK)),'CW','') AS INT) - 1, LAST_DAY(date_trunc('YEAR', current_timestamp)::TIMESTAMP_NTZ, 'week')) AS date,
    internal_qc_score_voice * 100.00 AS internal_qc_score_voice,
    internal_alert_rate_voice * 100.00 AS internal_alert_rate_voice,
    internal_qc_score_chat_email * 100.00 AS internal_qc_score_chat_email,
    internal_alert_rate_chat_email * 100.00 AS internal_alert_rate_chat_email
FROM lake_stg.excel.kpi_scorecard_etech_stg;

CREATE OR REPLACE TEMP TABLE _roe_store_map
(
    roe_store VARCHAR,
    division VARCHAR,
    region VARCHAR,
    country VARCHAR,
    bu VARCHAR,
    language VARCHAR,
    campaign VARCHAR
);

INSERT INTO _roe_store_map VALUES
        ('JF ROE', 'FF', 'EU', 'SE', 'JF', 'English', 'JF SE'),
        ('JF ROE', 'FF', 'EU', 'DK', 'JF', 'English', 'JF DK'),
        ('JF ROE', 'FF', 'EU', 'NL', 'JF', 'English', 'JF NL'),
        ('SXF ROE', 'SXF', 'EU', 'SE', 'SXF', 'English', 'SXF SE'),
        ('SXF ROE', 'SXF', 'EU', 'DK', 'SXF', 'English', 'SXF DK'),
        ('SXF ROE', 'SXF', 'EU', 'NL', 'SXF', 'English', 'SXF NL'),
        ('FL ROE', 'FL', 'EU', 'SE', 'FL', 'English', 'FL SE'),
        ('FL ROE', 'FL', 'EU', 'DK', 'FL', 'English', 'FL DK'),
        ('FL ROE', 'FL', 'EU', 'NL', 'FL', 'English', 'FL NL');
INSERT INTO _kpi_scorecard_etech_stg
(
    region,
    bu,
    division,
    country,
    language,
    campaign,
    date,
    internal_qc_score_voice,
    internal_alert_rate_voice,
    internal_qc_score_chat_email,
    internal_alert_rate_chat_email
)

SELECT
    rsm.region,
    rsm.bu,
    rsm.division,
    rsm.country,
    rsm.language,
    rsm.campaign,
    a.date,
    a.internal_qc_score_voice,
    a.internal_alert_rate_voice,
    a.internal_qc_score_chat_email,
    a.internal_alert_rate_chat_email
FROM  _kpi_scorecard_etech_stg a
JOIN _roe_store_map rsm
    ON rsm.roe_store = a.campaign
WHERE a.campaign IN ('JF ROE', 'FL ROE', 'SXF ROE')
AND rsm.campaign NOT IN (SELECT campaign FROM _kpi_scorecard_etech_stg);

DELETE FROM _kpi_scorecard_etech_stg
WHERE campaign IN ('JF ROE', 'FL ROE', 'SXF ROE');

INSERT INTO _kpi_scorecard_etech_stg
(
    region,
    bu,
    division,
    country,
    language,
    campaign,
    date
)
WITH CTE
AS
 (
     SELECT
         sd.region,
         sd.bu,
         sd.division,
         sd.country,
         sd.language,
         sd.campaign,
         d.date
     FROM _store_details sd
     CROSS JOIN (
                    SELECT DISTINCT
                        date
                    FROM _kpi_scorecard_etech_stg
                ) d
 )
SELECT
    c.region,
    c.bu,
    c.division,
    c.country,
    c.language,
    c.campaign,
    c.date
FROM CTE c
LEFT JOIN _kpi_scorecard_etech_stg k
    ON k.campaign = c.campaign
    AND k.date = c.date
WHERE k.campaign IS NULL;



MERGE INTO lake.excel.kpi_scorecard_etech t
USING (
    SELECT
        a.*
    FROM (
        SELECT
            *,
            hash(*) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER ( PARTITION BY division, region, country, bu, language, campaign, date ORDER BY ( SELECT NULL ) ) AS rn
        FROM _kpi_scorecard_etech_stg
     ) a
    WHERE a.rn = 1
) s ON equal_null(t.division, s.division)
    AND equal_null(t.region, s.region)
    AND equal_null(t.country, s.country)
    AND equal_null(t.bu, s.bu)
    AND equal_null(t.language, s.language)
    AND equal_null(t.campaign, s.campaign)
    AND equal_null(t.date, s.date)
WHEN NOT MATCHED THEN INSERT (
    division, region, country, bu, language, campaign, date, internal_qc_score_voice, internal_alert_rate_voice, internal_qc_score_chat_email, internal_alert_rate_chat_email, meta_row_hash, meta_create_datetime, meta_update_datetime
)
VALUES (
    division, region, country, bu, language, campaign, date, internal_qc_score_voice, internal_alert_rate_voice, internal_qc_score_chat_email, internal_alert_rate_chat_email, meta_row_hash, meta_create_datetime, meta_update_datetime
)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash THEN UPDATE
SET
    t.internal_qc_score_voice = s.internal_qc_score_voice,
    t.internal_alert_rate_voice = s.internal_alert_rate_voice,
    t.internal_qc_score_chat_email = s.internal_qc_score_chat_email,
    t.internal_alert_rate_chat_email = s.internal_alert_rate_chat_email,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime;

COMMIT;
