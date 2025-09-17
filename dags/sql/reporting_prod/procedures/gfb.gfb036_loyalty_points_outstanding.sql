create or replace temporary table _month_date as
select distinct
    dd.MONTH_DATE
from EDW_PROD.DATA_MODEL_JFB.DIM_DATE dd
where
    dd.FULL_DATE >= date_trunc(year, dateadd(year, -2, current_date()))
    and dd.FULL_DATE < current_date();


create or replace temporary table _loyalty_points_outanding_detail as
select
    lpa.BUSINESS_UNIT
    ,lpa.REGION
    ,lpa.COUNTRY
    ,md.MONTH_DATE
    ,lpa.CUSTOMER_ID

    ,sum(lpa.POINTS) as outanding_loyalty_points
from REPORTING_PROD.GFB.GFB_LOYALTY_POINTS_ACTIVITY lpa
join _month_date md
    on md.MONTH_DATE >= date_trunc(month, lpa.ACTIVITY_DATE)
group by
    lpa.BUSINESS_UNIT
    ,lpa.REGION
    ,lpa.COUNTRY
    ,md.MONTH_DATE
    ,lpa.CUSTOMER_ID;


CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.gfb036_loyalty_points_outanding as
select
    lpa.BUSINESS_UNIT
    ,lpa.REGION
    ,lpa.COUNTRY
    ,lpa.MONTH_DATE

    ,sum(lpa.outanding_loyalty_points) as outanding_loyalty_points
    ,count(distinct lpa.CUSTOMER_ID) as customer_count
from _loyalty_points_outanding_detail lpa
where
    lpa.outanding_loyalty_points != 0
group by
    lpa.BUSINESS_UNIT
    ,lpa.REGION
    ,lpa.COUNTRY
    ,lpa.MONTH_DATE;
