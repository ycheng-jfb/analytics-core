TRUNCATE TABLE validation.finance_currency_conversion_missing_currency;
INSERT INTO validation.finance_currency_conversion_missing_currency
WITH _distinct_local_currency AS (
    SELECT DISTINCT local_currency
    FROM reference.finance_currency_conversion
    )

SELECT
    dlc.local_currency,
    max(effective_date_to) AS max_effective_date_to
FROM _distinct_local_currency dlc
LEFT JOIN reference.finance_currency_conversion fcc
    ON dlc.local_currency = fcc.local_currency
    AND effective_date_to >= DATEADD(DAY, 7, CURRENT_TIMESTAMP)
GROUP BY dlc.local_currency
HAVING max(effective_date_to) IS NULL
;
