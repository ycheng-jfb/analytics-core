CREATE OR REPLACE TEMP TABLE _receivables_trx_name AS
SELECT receivables_trx_name, 'EU' as file_source
FROM lake.oracle_ebs.chargeback_eu eu
UNION
SELECT receivables_trx_name, 'US' as file_source
FROM lake.oracle_ebs.chargeback_us eu;

TRUNCATE TABLE validation.receivables_trx_name;
INSERT INTO validation.receivables_trx_name
SELECT trx.receivables_trx_name, file_source
FROM _receivables_trx_name trx
LEFT JOIN reference.gl_trx_store gl
    ON LOWER(gl.transaction_name) = LOWER(trx.receivables_trx_name)
WHERE gl.transaction_name IS NULL;
