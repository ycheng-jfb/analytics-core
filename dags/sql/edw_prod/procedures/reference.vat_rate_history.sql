SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMP TABLE _vat_stg
AS
SELECT
    vr.country_code,
    vr.rate,
    iff(vrh.country_code IS NULL, 'insert', 'update') AS meta_action,
    iff(vrh.country_code IS NULL, '1900-01-01', vr.meta_update_datetime::DATE) AS start_date,
    iff(vrh.country_code IS NULL, '9999-12-31', DATEADD(day, -1, vr.meta_update_datetime::DATE)) AS end_date
FROM lake_consolidated.ultra_merchant.vat_rate vr
LEFT JOIN reference.vat_rate_history vrh
    ON vrh.country_code = vr.country_code
    AND vrh.expires_date = '9999-12-31'
WHERE not equal_null(vr.rate, vrh.rate);

UPDATE reference.vat_rate_history vrh
SET vrh.expires_date = vs.end_date,
    vrh.meta_update_datetime = $execution_start_time
FROM _vat_stg vs
WHERE
    vrh.country_code = vs.country_code
    AND vrh.expires_date = '9999-12-31'
    AND vs.meta_action = 'update';

INSERT INTO reference.vat_rate_history
(
    country_code,
    rate,
    start_date,
    expires_date,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    country_code,
    rate,
    start_date,
    '9999-12-31' AS expires_date,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _vat_stg vs;
