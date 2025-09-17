CREATE TRANSIENT TABLE IF NOT EXISTS staging.customer_session
(
    session_id                         NUMBER(38, 0) NOT NULL PRIMARY KEY,
    datetime_added                     TIMESTAMP_LTZ(9),
    customer_id                        NUMBER(38, 0),
    store_brand_abbr                   VARCHAR(10),
    previous_customer_session_id       NUMBER(38, 0),
    previous_customer_session_datetime TIMESTAMP_NTZ(9),
    next_customer_session_id           NUMBER(38, 0),
    next_customer_session_datetime     TIMESTAMP_NTZ(9),
    meta_original_session_id           NUMBER(38, 0),
    meta_row_hash                      NUMBER(38, 0),
    meta_create_datetime               TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP,
    meta_update_datetime               TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP
);
