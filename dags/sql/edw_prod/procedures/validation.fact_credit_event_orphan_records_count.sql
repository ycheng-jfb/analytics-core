SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

TRUNCATE TABLE validation.fact_credit_event_orphan_records;

INSERT INTO validation.fact_credit_event_orphan_records(credit_id, meta_create_datetime, meta_update_datetime)
SELECT DISTINCT fce.credit_id,
                $execution_start_time AS meta_update_datetime,
                $execution_start_time AS meta_create_datetime
FROM stg.fact_credit_event fce
         LEFT JOIN stg.dim_credit dc
                   ON fce.credit_key = dc.credit_key AND fce.is_deleted = FALSE
WHERE dc.credit_key IS NULL;

TRUNCATE TABLE validation.fact_credit_event_orphan_records_count;

INSERT INTO validation.fact_credit_event_orphan_records_count
SELECT COUNT(*) AS orphan_record_count
FROM validation.fact_credit_event_orphan_records
WHERE meta_create_datetime = (SELECT MAX(meta_create_datetime) FROM validation.fact_credit_event_orphan_records)
HAVING COUNT(*) > 100;
