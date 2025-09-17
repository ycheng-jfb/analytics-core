CREATE OR REPLACE TRANSIENT TABLE validation.exchange_rate_check_for_duplicates
(
    table_name    VARCHAR(49),
    src_currency  VARCHAR,
    dest_currency VARCHAR,
    rate_date_pst TIMESTAMP_NTZ(9)
);
