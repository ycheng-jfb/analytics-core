CREATE TRANSIENT TABLE IF NOT EXISTS breakage.tax_breakage
(
    currency           VARCHAR(55),
    region             VARCHAR(16777216),
    display_store      VARCHAR(16777216),
    issued_year        NUMBER(4, 0),
    issued_amount      FLOAT,
    redeemed_to_date   FLOAT,
    expired_to_date    FLOAT,
    unredeemed_to_date FLOAT,
    breakage_to_record FLOAT
);
