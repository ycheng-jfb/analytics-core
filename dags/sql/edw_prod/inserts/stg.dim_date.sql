alter session set week_start = 7,week_of_year_policy = 1;

TRUNCATE TABLE stg.dim_date;

INSERT INTO stg.dim_date
WITH CTE_FULL_DATE AS (
SELECT DATEADD(DAY, SEQ4(), '2000-01-01 00:00:00') AS full_date
FROM TABLE(GENERATOR(ROWCOUNT=>100000))
)
SELECT
REPLACE(TO_DATE(full_date),'-','')::INT as date_key
,TO_DATE(full_date) as full_date
,DAYOFWEEK(full_date) as day_of_week
,CASE DAYNAME(full_date)
    WHEN 'Sun' THEN 'Sunday'
    WHEN 'Mon' THEN 'Monday'
    WHEN 'Tue' THEN 'Tuesday'
    WHEN 'Wed' THEN 'Wednesday'
    WHEN 'Thu' THEN 'Thursday'
    WHEN 'Fri' THEN 'Friday'
    WHEN 'Sat' THEN 'Saturday' END as day_name_of_week
,DAYOFMONTH(full_date) as day_of_month
,DAYOFYEAR(full_date) as day_of_year
,WEEKOFYEAR(full_date) as week_of_year
,CASE MONTHNAME(full_date)
    WHEN 'Jan' THEN 'January'
    WHEN 'Feb' THEN 'February'
    WHEN 'Mar' THEN 'March'
    WHEN 'Apr' THEN 'April'
    WHEN 'May' THEN 'May'
    WHEN 'Jun' THEN 'June'
    WHEN 'Jul' THEN 'July'
    WHEN 'Aug' THEN 'August'
    WHEN 'Sep' THEN 'September'
    WHEN 'Oct' THEN 'October'
    WHEN 'Nov' THEN 'November'
    WHEN 'Dec' THEN 'December' END as month_name
,DATE_TRUNC('MONTH', full_date) as month_date
,QUARTER(full_date) as calendar_quarter
,YEAR(full_date) as calendar_year
,YEAR(full_date)||'Q'||QUARTER(full_date) as calendar_year_quarter
,'1900-01-01 00:00:00.000' as effective_start_datetime
,'9999-12-31 00:00:00.000' as effective_end_datetime
, 1 as is_current
, current_timestamp as meta_create_datetime
, current_timestamp as meta_update_datetime
FROM CTE_FULL_DATE
WHERE TO_DATE(full_date) <= '2040-12-31'
UNION
select -1, '1900-01-01', -1, 'Unknown', -1, -1, -1, 'Unknown', '1900-01-01', -1, -1, 'Unknown', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp;
