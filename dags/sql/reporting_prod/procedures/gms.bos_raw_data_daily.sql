create or replace temp table _bos_raw_data_daily as
with day1 as (
select
    STOREFRONT,
    to_date(BILLING_MONTH) as BILLING_MONTH,
    sum(TOTAL_TOTAL_PLANNED) as TOTAL_PLANNED,
    sum(DAY_1_PLANNED) as PLANNED,
    sum(DAY_1_ACTUALS) as ACTUALS,
    sum(DAY_1_TO_BE_BILLED) as BILLING_COUNT,
    sum(PREV_3_MONTH_DAY_1_ACTUALS) as PREV_3_MONTH_ACTUALS,
    sum(PREV_3_MONTH_DAY_1_TO_BE_BILLED) as PREV_3_MONTH_BILLING_COUNT,
    'Day 1' as DAY
    from REPORTING_PROD.GMS.BOS_RAW_DATA_FINAL
    group by STOREFRONT, BILLING_MONTH
),
day2 as (
select
    STOREFRONT,
    to_date(BILLING_MONTH) as BILLING_MONTH,
    sum(0) as TOTAL_PLANNED,
    sum(DAY_2_PLANNED) as PLANNED,
    sum(DAY_2_ACTUALS) as ACTUALS,
    sum(DAY_2_TO_BE_BILLED) as BILLING_COUNT,
    sum(PREV_3_MONTH_DAY_2_ACTUALS) as PREV_3_MONTH_ACTUALS,
    sum(PREV_3_MONTH_DAY_2_TO_BE_BILLED) as PREV_3_MONTH_BILLING_COUNT,
    'Day 2' as DAY
from REPORTING_PROD.GMS.BOS_RAW_DATA_FINAL
group by STOREFRONT, BILLING_MONTH
),
day3 as (
select
    STOREFRONT,
    to_date(BILLING_MONTH) as BILLING_MONTH,
    sum(0) as TOTAL_PLANNED,
    sum(DAY_3_PLANNED) as PLANNED,
    sum(DAY_3_ACTUALS) as ACTUALS,
    sum(DAY_3_TO_BE_BILLED) as BILLING_COUNT,
    sum(PREV_3_MONTH_DAY_3_ACTUALS) as PREV_3_MONTH_ACTUALS,
    sum(PREV_3_MONTH_DAY_3_TO_BE_BILLED) as PREV_3_MONTH_BILLING_COUNT,
    'Day 3' as DAY
from REPORTING_PROD.GMS.BOS_RAW_DATA_FINAL
group by STOREFRONT, BILLING_MONTH
),
day4 as (
select
    STOREFRONT,
    to_date(BILLING_MONTH) as BILLING_MONTH,
    sum(0) as TOTAL_PLANNED,
    sum(DAY_4_PLANNED) as PLANNED,
    sum(DAY_4_ACTUALS) as ACTUALS,
    sum(DAY_4_TO_BE_BILLED) as BILLING_COUNT,
    sum(PREV_3_MONTH_DAY_4_ACTUALS) as PREV_3_MONTH_ACTUALS,
    sum(PREV_3_MONTH_DAY_4_TO_BE_BILLED) as PREV_3_MONTH_BILLING_COUNT,
    'Day 4' as DAY
from REPORTING_PROD.GMS.BOS_RAW_DATA_FINAL
group by STOREFRONT, BILLING_MONTH
),
day5 as (
select
    STOREFRONT,
    to_date(BILLING_MONTH) as BILLING_MONTH,
    sum(0) as TOTAL_PLANNED,
    sum(DAY_5_PLANNED) as PLANNED,
    sum(DAY_5_ACTUALS) as ACTUALS,
    sum(DAY_5_TO_BE_BILLED) as BILLING_COUNT,
    sum(PREV_3_MONTH_DAY_5_ACTUALS) as PREV_3_MONTH_ACTUALS,
    sum(PREV_3_MONTH_DAY_5_TO_BE_BILLED) as PREV_3_MONTH_BILLING_COUNT,
    'Day 5' as DAY
from REPORTING_PROD.GMS.BOS_RAW_DATA_FINAL
group by STOREFRONT, BILLING_MONTH
),
day6 as (
select
    STOREFRONT,
    to_date(BILLING_MONTH) as BILLING_MONTH,
    sum(0) as TOTAL_PLANNED,
    sum(DAY_6_PLANNED) as PLANNED,
    sum(DAY_6_ACTUALS) as ACTUALS,
    sum(DAY_6_TO_BE_BILLED) as BILLING_COUNT,
    sum(PREV_3_MONTH_DAY_6_ACTUALS) as PREV_3_MONTH_ACTUALS,
    sum(PREV_3_MONTH_DAY_6_TO_BE_BILLED) as PREV_3_MONTH_BILLING_COUNT,
    'Day 6' as DAY
from REPORTING_PROD.GMS.BOS_RAW_DATA_FINAL
group by STOREFRONT, BILLING_MONTH
),
day7 as (
select
    STOREFRONT,
    to_date(BILLING_MONTH) as BILLING_MONTH,
    sum(0) as TOTAL_PLANNED,
    sum(DAY_7_PLANNED) as PLANNED,
    sum(DAY_7_ACTUALS) as ACTUALS,
    sum(DAY_7_TO_BE_BILLED) as BILLING_COUNT,
    sum(PREV_3_MONTH_DAY_7_ACTUALS) as PREV_3_MONTH_ACTUALS,
    sum(PREV_3_MONTH_DAY_7_TO_BE_BILLED) as PREV_3_MONTH_BILLING_COUNT,
    'Day 7' as DAY
from REPORTING_PROD.GMS.BOS_RAW_DATA_FINAL
group by STOREFRONT, BILLING_MONTH
),
day8 as (
select
    STOREFRONT,
    to_date(BILLING_MONTH) as BILLING_MONTH,
    sum(0) as TOTAL_PLANNED,
    sum(DAY_8_PLANNED) as PLANNED,
    sum(DAY_8_ACTUALS) as ACTUALS,
    sum(DAY_8_TO_BE_BILLED) as BILLING_COUNT,
    sum(PREV_3_MONTH_DAY_8_ACTUALS) as PREV_3_MONTH_ACTUALS,
    sum(PREV_3_MONTH_DAY_8_TO_BE_BILLED) as PREV_3_MONTH_BILLING_COUNT,
    'Day 8' as DAY
from REPORTING_PROD.GMS.BOS_RAW_DATA_FINAL
group by STOREFRONT, BILLING_MONTH
),
day9 as (
select
    STOREFRONT,
    to_date(BILLING_MONTH) as BILLING_MONTH,
    sum(0) as TOTAL_PLANNED,
    sum(DAY_9_PLANNED) as PLANNED,
    sum(DAY_9_ACTUALS) as ACTUALS,
    sum(DAY_9_TO_BE_BILLED) as BILLING_COUNT,
    sum(PREV_3_MONTH_DAY_9_ACTUALS) as PREV_3_MONTH_ACTUALS,
    sum(PREV_3_MONTH_DAY_9_TO_BE_BILLED) as PREV_3_MONTH_BILLING_COUNT,
    'Day 9' as DAY
from REPORTING_PROD.GMS.BOS_RAW_DATA_FINAL
group by STOREFRONT, BILLING_MONTH
),
all_days as (
                select * from day1
                union
                select * from day2
                union
                select * from day3
                union
                select * from day4
                union
                select * from day5
                union
                select * from day6
                union
                select * from day7
                union
                select * from day8
                union
                select * from day9
)
select * from all_days;

BEGIN;

DELETE FROM reporting_prod.gms.bos_raw_data_daily;

MERGE INTO reporting_prod.gms.bos_raw_data_daily t
    USING (SELECT *
                FROM (select *,
                        HASH(*) as meta_row_hash,
                        row_number()
                            OVER (PARTITION BY billing_month,storefront,day
                                order by NULL
                                 ) AS rn
                        FROM _bos_raw_data_daily)
                where rn = 1) s
        ON equal_null(t.billing_month, s.billing_month)
        AND equal_null(t.storefront, s.storefront)
        AND equal_null(t.day, s.day)
    WHEN NOT MATCHED THEN INSERT
        (storefront,billing_month,total_planned,planned,actuals,billing_count,prev_3_month_actuals,
    prev_3_month_billing_count,day, meta_row_hash)
        VALUES  (storefront,billing_month,total_planned,planned,actuals,billing_count,prev_3_month_actuals,
    prev_3_month_billing_count,day,meta_row_hash)
    WHEN MATCHED AND s.meta_row_hash != t.meta_row_hash
        THEN
        UPDATE
            SET t.total_planned = s.total_planned
                ,t.planned = s.planned
                ,t.actuals = s.actuals
                ,t.billing_count = s.billing_count
                ,t.prev_3_month_actuals = s.prev_3_month_actuals
                ,t.prev_3_month_billing_count = s.prev_3_month_billing_count
                ,t.day = s.day
                ,t.meta_row_hash = s.meta_row_hash
                ,t.meta_update_datetime = CURRENT_TIMESTAMP;

COMMIT;
