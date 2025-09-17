use reporting_prod;
TRUNCATE TABLE reporting_prod.blue_yonder.export_by_calendar_interface_stg;

INSERT INTO reporting_prod.blue_yonder.export_by_calendar_interface_stg
(
DAY,
WEEK,
MONTH,
MONTH_DESC,
MONTH_STARTDATE,
QUARTER,
QUARTER_DESC,
QUARTER_STARTDATE,
HALF,
HALF_DESC,
HALF_STARTDATE,
YEAR,
YEAR_DESC,
YEAR_STARTDATE
)
WITH date_parts AS (SELECT FULL_DATE,
                           WEEK_OF_YEAR,
                           MONTH_DATE,
                           MONTH_NAME,
                           CALENDAR_YEAR,
                           CALENDAR_YEAR_QUARTER,
                           CALENDAR_QUARTER,
                           DAY_OF_WEEK,
                           IFF(MONTH(MONTH_DATE) BETWEEN 1 AND 6, 1, 2) AS HALF_YEAR
                    FROM EDW_PROD.DATA_MODEL_FL.DIM_DATE
                    where CALENDAR_YEAR >= 2021)
SELECT FULL_DATE                                                                        AS DAY,
        iff(
        datediff(day,full_date,date(year(full_date)||'-12-31')) < (7-DAY_OF_WEEK),
        CONCAT('W', CALENDAR_YEAR+1, '_', 1),
        CONCAT('W', CALENDAR_YEAR, '_', WEEK_OF_YEAR)
        )                                                                               AS WEEK,
       CONCAT('M', SPLIT_PART(MONTH_DATE, '-', 1), '_', SPLIT_PART(MONTH_DATE, '-', 2)) AS MONTH,
       CONCAT(MONTHNAME(FULL_DATE), ' ', CALENDAR_YEAR)                                 AS MONTH_DESC,
       MONTH_DATE                                                                       AS MONTH_STARTDATE,
       CONCAT('Q', CALENDAR_QUARTER, '_', CALENDAR_YEAR)                                AS QUARTER,
       CONCAT('Q', CALENDAR_QUARTER, ' ', CALENDAR_YEAR)                                AS QUARTER_DESC,
       DATE_TRUNC('QUARTER', MONTH_DATE)                                                AS QUARTER_STARTDATE,
       CONCAT('H', HALF_YEAR, '_', CALENDAR_YEAR)                                       AS HALF,
       CONCAT('H', HALF_YEAR, ' ', CALENDAR_YEAR)                                       AS HALF_DESC,
       IFF(HALF_YEAR = 1, DATE_FROM_PARTS(YEAR(MONTH_DATE), 1, 1),
           DATE_FROM_PARTS(YEAR(MONTH_DATE), 7, 1))                                     AS HALF_STARTDATE,
       CONCAT('Y', CALENDAR_YEAR)                                                       AS YEAR,
       CONCAT('Y', CALENDAR_YEAR)                                                       AS YEAR_DESC,
       DATE_TRUNC('YEAR', MONTH_DATE)                                                   AS YEAR_STARTDATE
FROM date_parts
ORDER BY DAY;

update reporting_prod.blue_yonder.export_by_CALENDAR_interface_stg
set
day = reporting_prod.blue_yonder.udf_cleanup_field(day),
week = reporting_prod.blue_yonder.udf_cleanup_field(week),
month = reporting_prod.blue_yonder.udf_cleanup_field(month),
month_desc = reporting_prod.blue_yonder.udf_cleanup_field(month_desc),
month_startdate = reporting_prod.blue_yonder.udf_cleanup_field(month_startdate),
quarter = reporting_prod.blue_yonder.udf_cleanup_field(quarter),
quarter_desc = reporting_prod.blue_yonder.udf_cleanup_field(quarter_desc),
quarter_startdate = reporting_prod.blue_yonder.udf_cleanup_field(quarter_startdate),
half = reporting_prod.blue_yonder.udf_cleanup_field(half),
half_desc = reporting_prod.blue_yonder.udf_cleanup_field(half_desc),
half_startdate = reporting_prod.blue_yonder.udf_cleanup_field(half_startdate),
year = reporting_prod.blue_yonder.udf_cleanup_field(year),
year_desc = reporting_prod.blue_yonder.udf_cleanup_field(year_desc),
year_startdate = reporting_prod.blue_yonder.udf_cleanup_field(year_startdate);
