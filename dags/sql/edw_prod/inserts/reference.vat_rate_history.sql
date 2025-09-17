TRUNCATE TABLE reference.vat_rate_history;
INSERT INTO reference.vat_rate_history
(
 country_code
,rate
,start_date
,expires_date
,meta_create_datetime
,meta_update_datetime
)
SELECT 'AT', 0.200000, '2000-01-01', '9999-12-31', current_timestamp, current_timestamp
UNION
SELECT 'BE', 0.210000, '2000-01-01', '9999-12-31', current_timestamp, current_timestamp
UNION
SELECT 'DE', 0.190000, '2000-01-01', '2020-06-29', current_timestamp, current_timestamp
UNION
SELECT 'DE', 0.160000, '2020-06-30', '9999-12-31', current_timestamp, current_timestamp
UNION
SELECT 'DK', 0.250000, '2000-01-01', '9999-12-31', current_timestamp, current_timestamp
UNION
SELECT 'ES', 0.210000, '2000-01-01', '9999-12-31', current_timestamp, current_timestamp
UNION
SELECT 'FR', 0.196000, '2000-01-01', '2013-12-31', current_timestamp, current_timestamp
UNION
SELECT 'FR', 0.200000, '2014-01-01', '9999-12-31', current_timestamp, current_timestamp
UNION
SELECT 'GB', 0.200000, '2000-01-01', '9999-12-31', current_timestamp, current_timestamp
UNION
SELECT 'IE', 0.230000, '2000-01-01', '9999-12-31', current_timestamp, current_timestamp
UNION
SELECT 'IT', 0.220000, '2000-01-01', '9999-12-31', current_timestamp, current_timestamp
UNION
SELECT 'NL', 0.210000, '2000-01-01', '9999-12-31', current_timestamp, current_timestamp
UNION
SELECT 'PL', 0.230000, '2000-01-01', '9999-12-31', current_timestamp, current_timestamp
UNION
SELECT 'SE', 0.250000, '2000-01-01', '9999-12-31', current_timestamp, CURRENT_TIMESTAMP;
