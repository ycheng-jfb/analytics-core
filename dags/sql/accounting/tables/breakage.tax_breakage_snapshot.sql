CREATE TRANSIENT TABLE IF NOT EXISTS breakage.tax_breakage_snapshot
(
    currency             VARCHAR(55),
    region               VARCHAR(16777216),
    display_store        VARCHAR(16777216),
    issued_year          NUMBER(4, 0),
    issued_amount        FLOAT,
    redeemed_to_date     FLOAT,
    unredeemed_to_date   FLOAT,
    breakage_to_record   FLOAT,
    target_date          DATE,
    snapshot_timestamp   TIMESTAMP_LTZ(9),
    meta_create_datetime TIMESTAMP_LTZ(9),
    meta_update_datetime TIMESTAMP_LTZ(9),
    expired_to_date      FLOAT
);
