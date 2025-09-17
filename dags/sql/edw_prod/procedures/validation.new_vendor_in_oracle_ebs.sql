TRUNCATE TABLE validation.new_vendor_in_oracle_ebs;

CREATE OR REPLACE TEMP TABLE _new_vendor AS
SELECT DISTINCT ds.source AS vendor_name, 'chargeback_us' AS table_name
FROM lake.oracle_ebs.chargeback_us ds
         LEFT JOIN lake.oracle_ebs.chargeback_us AT (OFFSET => -24 * 60 * 60) AS pds
                   ON pds.source = ds.source
WHERE pds.source IS NULL
UNION ALL
SELECT DISTINCT ds.source AS vendor_name, 'chargeback_eu' AS table_name
FROM lake.oracle_ebs.chargeback_eu ds
         LEFT JOIN lake.oracle_ebs.chargeback_eu AT (OFFSET => -24 * 60 * 60) AS pds
                   ON pds.source = ds.source
WHERE pds.source IS NULL;

INSERT INTO validation.new_vendor_in_oracle_ebs(vendor_name, table_name)
SELECT vendor_name, table_name FROM _new_vendor;
