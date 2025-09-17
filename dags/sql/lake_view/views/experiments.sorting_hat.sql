CREATE VIEW IF NOT EXISTS LAKE_VIEW.EXPERIMENTS.SORTING_HAT
AS SELECT
    TRIM(o.value:testId, '"')::NUMBER as test_metadata_id,
    TRIM(o.value:testName, '"')::VARCHAR as test_name,
    r.value:id::NUMBER AS customer_id,
    r.value:variant::VARCHAR AS variant_version,
    TO_TIMESTAMP_LTZ(SUBSTR(o.value:dateTimeCreated, 1, 10)) AS datetime_created,
    meta_create_datetime,
    meta_update_datetime
FROM lake.experiments.sorting_hat i,
    LATERAL FLATTEN(test_object) o,
    LATERAL FLATTEN(o.VALUE:results) r;
