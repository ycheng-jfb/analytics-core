CREATE OR REPLACE TRANSIENT TABLE validation.finance_currency_conversion_missing_currency
(
    local_currency        VARCHAR(10),
    max_effective_date_to TIMESTAMP_NTZ(9)
);
