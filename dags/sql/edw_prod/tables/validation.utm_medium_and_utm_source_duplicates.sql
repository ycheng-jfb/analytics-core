CREATE TABLE IF NOT EXISTS validation.utm_medium_and_utm_source_duplicates
(
    utm_medium      VARCHAR,
    utm_source      VARCHAR,
    brand           VARCHAR,
    utm_campaign    VARCHAR,
    referrer        VARCHAR,
    duplicate_count NUMBER(38, 0)
);
