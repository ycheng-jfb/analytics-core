TRUNCATE TABLE validation.edw_exception_count;
INSERT INTO validation.edw_exception_count
SELECT
    table_name,
    row_count,
    table_schema
FROM information_schema.tables
WHERE table_schema = 'EXCP'
    AND table_name <> 'LKP_MEMBERSHIP_EVENT' -- excluding lkp_membership_event
    AND row_count >= 100
ORDER BY row_count DESC;
