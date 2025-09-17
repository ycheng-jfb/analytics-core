CREATE OR REPLACE DYNAMIC TABLE REPORTING_PROD.SXF.VIEW_DATE_VIEW
    TARGET_LAG = '10 minutes'
    WAREHOUSE = DA_WH_ANALYTICS
AS (
    -- Daily --
    SELECT DISTINCT
        full_date as date,
        dayofyear(date) as dayofyear,
        month(date) as monthofyear,
        year(Date) as year,
        null as weekofyear,
        null as yearofweek,
        'Daily' as date_view
    FROM EDW_PROD.DATA_MODEL_SXF.DIM_DATE
    WHERE year>=2018 AND year<=year(current_date())
    UNION
    -- Weekly --
    SELECT DISTINCT
        full_date as date,
        null as dayofyear,
        null as monthofyear,
        null as year,
        weekofyear(dateadd(day,1,full_date)) as weekofyear, --- Alter to Start week 1 day before Monday (Sun-Sat)
        yearofweek(dateadd(day,1,full_date)) as yearofweek, --- Alter to Start week 1 day before Monday (Sun-Sat)
        'Weekly' as date_view
    FROM EDW_PROD.DATA_MODEL_SXF.DIM_DATE
    WHERE year(full_date)>=2018 AND year<=year(current_date())
    UNION
    -- Monthly --
    SELECT DISTINCT
        full_date as date,
        null as dayofyear,
        month(date) as monthofyear,
        year(Date) as year,
        null as weekofyear,
        null as yearofweek,
        'Monthly' as date_view
    FROM EDW_PROD.DATA_MODEL_SXF.DIM_DATE
    WHERE year>=2018 AND year<=year(current_date())
    UNION
    -- Yearly --
    SELECT DISTINCT
        full_date as date,
        null as dayofyear,
        null as monthofyear,
        year(full_Date) as year,
        null as weekofyear,
        null as yearofweek,
        'Yearly' as date_view
    FROM  EDW_PROD.DATA_MODEL_SXF.DIM_DATE as dd
    WHERE year>=2018 AND year<=year(current_date())
)
;

ALTER DYNAMIC TABLE REPORTING_PROD.SXF.VIEW_DATE_VIEW REFRESH;

GRANT SELECT ON REPORTING_PROD.SXF.VIEW_DATE_VIEW TO ROLE __REPORTING_PROD_SXF_R;
GRANT SELECT ON REPORTING_PROD.SXF.VIEW_DATE_VIEW TO ROLE __REPORTING_PROD_SXF_RW;
GRANT SELECT ON REPORTING_PROD.SXF.VIEW_DATE_VIEW TO ROLE __REPORTING_PROD_SXF_RWC;
GRANT OWNERSHIP ON REPORTING_PROD.SXF.VIEW_DATE_VIEW TO ROLE SYSADMIN COPY CURRENT GRANTS;
