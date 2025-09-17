TRUNCATE TABLE validation.exchange_rate_check_for_duplicates;
INSERT INTO validation.exchange_rate_check_for_duplicates
SELECT 'edw_prod.reference.currency_exchange_rate_by_date' AS table_name,
       src_currency,
       dest_currency,
       rate_date_pst
FROM reference.currency_exchange_rate_by_date
GROUP BY src_currency, dest_currency, rate_date_pst
HAVING COUNT(*) > 1
UNION
SELECT 'edw_prod.reference.currency_exchange_rate' AS table_name,
       src_currency,
       dest_currency,
       rate_datetime
FROM reference.currency_exchange_rate
GROUP BY src_currency, dest_currency, rate_datetime
HAVING COUNT(*) > 1;
