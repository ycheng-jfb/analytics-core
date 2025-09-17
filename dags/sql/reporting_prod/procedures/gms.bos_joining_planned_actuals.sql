CREATE OR REPLACE TEMPORARY TABLE _join_planned_and_actuals AS
select
        ar."Current Date" as ar_current_date,
        ar."Billing Day" as ar_billing_day,
        ar."billing hour" as ar_billing_hour,
        ar."Billing Month" as ar_billing_month,
        ar."Total Credits Billed" as ar_total_credits_billed,
        ar."Billing Date" as ar_billing_date,
        ar."Total Actuals" as ar_total_actuals,
        ar."Total Retries" as ar_total_retries,
        ar."Storefront" as ar_storefront,
        ar."Curr_Datehour" as ar_curr_datehour,
        pb."Current Date" as pb_current_date,
        pb."Billing Day" as pb_billing_day,
        pb."Storefront" as pb_storefront,
        pb."Total Planned" as pb_total_planned,
        pb."Billing Month" as pb_billing_month,
        pb."Billing Date" as pb_billing_date,
        pb."billing hour" as pb_billing_hour,
        pb."Curr_Datehour" as pb_curr_datehour
from reporting_prod.gms.BOSS_ACTUALS_RETRIES_v2_dataset ar
full join reporting_prod.gms.boss_planned_billings_dataset pb
 on pb."billing hour"=ar."billing hour" and pb."Billing Month"=ar."Billing Month" and pb."Storefront"=ar."Storefront";

CREATE OR REPLACE TEMPORARY TABLE _join_planned_and_actuals1 AS
SELECT
        ar_current_date AS curr_date,
        ar_billing_day AS billing_day,
        ar_billing_hour AS billing_hour,
        ar_billing_month AS billing_month,
        ar_total_credits_billed AS total_credits_billed,
        ar_billing_date AS billing_date,
        ar_total_actuals AS total_actuals,
        ar_total_retries AS total_retries,
        ar_storefront AS storefront,
        ar_curr_datehour AS curr_datehour,
        pb_current_date AS current_date1,
        pb_billing_day AS billing_day1,
        pb_storefront AS storefront1,
        pb_total_planned AS total_planned,
        pb_billing_month AS billing_month1,
        pb_billing_date AS billing_date1,
        pb_billing_hour AS billing_hour1,
        pb_curr_datehour AS curr_datehour1
FROM _join_planned_and_actuals
WHERE (ar_curr_datehour >= ar_billing_hour OR pb_curr_datehour >= pb_billing_hour)
        AND (ar_storefront IS NOT NULL AND pb_storefront IS NOT NULL);

CREATE OR REPLACE TEMPORARY TABLE _join_planned_and_actuals2 AS
SELECT
        pb_current_date AS curr_date,
        pb_billing_day AS billing_day,
        pb_billing_hour AS billing_hour,
        pb_billing_month AS billing_month,
        ar_total_credits_billed AS total_credits_billed,
        pb_billing_date AS billing_date,
        ar_total_actuals AS total_actuals,
        ar_total_retries AS total_retries,
        ifnull(ar_storefront, pb_storefront) AS storefront,
        ar_curr_datehour AS curr_datehour,
        pb_current_date AS current_date1,
        pb_billing_day AS billing_day1,
        pb_storefront AS storefront1,
        pb_total_planned AS total_planned,
        pb_billing_month AS billing_month1,
        pb_billing_date AS billing_date1,
        pb_billing_hour AS billing_hour1,
        pb_curr_datehour AS curr_datehour1
FROM _join_planned_and_actuals
WHERE (ar_curr_datehour >= ar_billing_hour OR pb_curr_datehour >= pb_billing_hour)
      AND ar_storefront IS NULL;

CREATE OR REPLACE TEMPORARY TABLE _join_planned_and_actuals3 AS
SELECT
        ifnull(pb_current_date, ar_current_date) AS curr_date,
        ifnull(pb_billing_day, ar_billing_day) AS billing_day,
        ifnull(pb_billing_hour, ar_billing_hour) AS billing_hour,
        ifnull(pb_billing_month, ar_billing_month) AS billing_month,
        ar_total_credits_billed AS total_credits_billed,
        ifnull(pb_billing_date, ar_billing_date) AS billing_date,
        ar_total_actuals AS total_actuals,
        ar_total_retries AS total_retries,
        ifnull(pb_storefront, ar_storefront) AS storefront,
        pb_curr_datehour AS curr_datehour,
        ar_current_date AS current_date1,
        ar_billing_day AS billing_day1,
        ar_storefront AS storefront1,
        pb_total_planned AS total_planned,
        ar_billing_month AS billing_month1,
        ar_billing_date AS billing_date1,
        ar_billing_hour AS billing_hour1,
        ar_curr_datehour AS curr_datehour1
FROM _join_planned_and_actuals
WHERE (ar_curr_datehour >= ar_billing_hour OR pb_curr_datehour >= pb_billing_hour)
      AND pb_storefront IS NULL;

CREATE OR REPLACE TEMPORARY TABLE _join_planned_and_actuals4 AS
SELECT * FROM _join_planned_and_actuals2
UNION
SELECT * FROM _join_planned_and_actuals3;

CREATE OR REPLACE TEMPORARY TABLE _join_planned_and_actuals5 AS
SELECT * FROM _join_planned_and_actuals1
UNION
SELECT * FROM _join_planned_and_actuals4;

CREATE OR REPLACE TEMPORARY TABLE _planned_and_actuals_calc1 AS
SELECT
        billing_month,
        storefront,
        curr_date,
        sum(total_actuals) AS total_actuals,
        sum(total_retries) AS total_retries,
        sum(total_planned) AS total_planned
FROM _join_planned_and_actuals5
GROUP BY 1, 2, 3;

CREATE OR REPLACE TEMPORARY TABLE _planned_and_actuals_calc2 AS
SELECT
        *,
        total_planned - total_actuals AS total_pa_pct_numerator
FROM _planned_and_actuals_calc1;

CREATE OR REPLACE TEMPORARY TABLE _planned_and_actuals_calc3 AS
SELECT
        *,
        iff(total_planned = 0, 0, total_pa_pct_numerator / total_planned) as total_pa_pct
        --div0("total: p:a % numerator", "total planned") as "total: p:a %"
FROM _planned_and_actuals_calc2;

CREATE OR REPLACE TEMPORARY TABLE _planned_and_actuals_calc4 AS
SELECT
        billing_month,
        storefront,
        curr_date,
        total_actuals,
        total_retries,
        total_planned,
        total_pa_pct_numerator,
        --"total: p:a %"
        total_pa_pct * -1 AS total_pa_pct
FROM _planned_and_actuals_calc3
WHERE total_pa_pct < 0 OR total_pa_pct IS NULL;

CREATE OR REPLACE TEMPORARY TABLE _planned_and_actuals_calc5 AS
SELECT
        billing_month,
        storefront,
        curr_date,
        total_actuals,
        total_retries,
        total_planned,
        total_pa_pct_numerator,
        total_pa_pct
FROM _planned_and_actuals_calc3
WHERE total_pa_pct >= 0;

CREATE OR REPLACE TEMPORARY TABLE _planned_and_actuals_final AS
SELECT * FROM _planned_and_actuals_calc4
UNION
SELECT * FROM _planned_and_actuals_calc5;

CREATE OR REPLACE TEMPORARY TABLE _join_planned_actuals_and_credits_billed AS
select
    t.billing_month,
    t.billing_date,
    t.storefront,
    t.billing_day,
    sum(t.total_actuals) as total_1st_time_actuals,
    sum(t.total_planned) as total_planned,
    sum(c."Total_Credits_Try_To_Bill") as total_credits_try_to_bill
from _join_planned_and_actuals5 t
full join gms.boss_credits_to_be_billed_v2_dataset c
    on t.billing_hour=c."billing hour"
    and t.billing_date=c."Billing Date"
    and t.billing_month=c."Billing Month"
    and t.storefront=c."Storefront"
    and t.billing_day=c."Billing Day"
where t.total_actuals is not null
group by 1, 2, 3, 4;

CREATE OR REPLACE TEMPORARY TABLE _join_planned_actuals_and_credits_billed_prep AS
SELECT
    *
FROM _join_planned_actuals_and_credits_billed
WHERE total_1st_time_actuals IS NOT NULL;

CREATE OR REPLACE TEMPORARY table _billing_day6 as
SELECT
        billing_month,
        storefront,
        sum(total_planned) AS day_1_planned,
        sum(total_1st_time_actuals) AS day_1_actuals,
        sum(total_credits_try_to_bill) AS day_1_to_be_billed
FROM _join_planned_actuals_and_credits_billed_prep
WHERE billing_day = 6
GROUP BY 1, 2;

CREATE OR REPLACE TEMPORARY TABLE _billing_day6_calc1 AS
SELECT
        *,
        day_1_planned - day_1_actuals as day_1_pa_pct_numerator
FROM _billing_day6;

CREATE OR REPLACE TEMPORARY TABLE _billing_day6_calc2 AS
SELECT
        *,
        iff(day_1_planned = 0, 0, day_1_pa_pct_numerator/day_1_planned) AS day_1_pa_pct
        --div0("day 1: p:a % numerator", "day 1 planned") as "day 1: p:a %"
FROM _billing_day6_calc1;

CREATE OR REPLACE TEMPORARY TABLE _billing_day6_filter1 AS
SELECT
        *
FROM _billing_day6_calc2
WHERE day_1_pa_pct >= 0;

CREATE OR REPLACE TEMPORARY TABLE _billing_day6_filter2 AS
SELECT
        billing_month,
        storefront,
        day_1_planned,
        day_1_actuals,
        day_1_to_be_billed,
        day_1_pa_pct_numerator,
        day_1_pa_pct * -1 AS day_1_pa_pct
        --"day 1: p:a %" * -1 as "day 1: p:a fix"
FROM _billing_day6_calc2
WHERE day_1_pa_pct < 0 OR day_1_pa_pct IS NULL;

CREATE OR REPLACE TEMPORARY TABLE _billing_day6_final AS
SELECT * FROM _billing_day6_filter1
UNION
SELECT * FROM _billing_day6_filter2;


CREATE OR REPLACE TEMPORARY TABLE _billing_day7 AS
SELECT
        billing_month,
        storefront,
        sum(total_planned) as day_2_planned,
        sum(total_1st_time_actuals) as day_2_actuals,
        sum(total_credits_try_to_bill) as day_2_to_be_billed
FROM _join_planned_actuals_and_credits_billed_prep
WHERE billing_day = 7
GROUP BY 1, 2;

CREATE OR REPLACE TEMPORARY TABLE _billing_day7_calc1 AS
SELECT
        *,
        day_2_planned - day_2_actuals AS day_2_pa_pct_numerator
FROM _billing_day7;

CREATE OR REPLACE TEMPORARY TABLE _billing_day7_calc2 AS
SELECT
        *,
        iff(day_2_planned = 0, 0, day_2_pa_pct_numerator/day_2_planned) as day_2_pa_pct
        --div0("day 2: p:a % numerator", "day 2 planned") as "day 2: p:a %"
FROM _billing_day7_calc1;

CREATE OR REPLACE TEMPORARY TABLE _billing_day7_filter1 AS
SELECT
        *
FROM _billing_day7_calc2
WHERE day_2_pa_pct >= 0;

CREATE OR REPLACE TEMPORARY TABLE _billing_day7_filter2 AS
SELECT
        billing_month,
        storefront,
        day_2_planned,
        day_2_actuals,
        day_2_to_be_billed,
        day_2_pa_pct_numerator,
        day_2_pa_pct * -1 as day_2_pa_pct
        --"day 2: p:a %" * -1 as "day 2: p:a fix"
FROM _billing_day7_calc2
WHERE day_2_pa_pct < 0 or day_2_pa_pct is null;

create or replace temporary table _billing_day7_final as
select * from _billing_day7_filter1
union
select * from _billing_day7_filter2;

create or replace temporary table _billing_day8 as
select
        billing_month,
        storefront,
        sum(total_planned) as day_3_planned,
        sum(total_1st_time_actuals) as day_3_actuals,
        sum(total_credits_try_to_bill) as day_3_to_be_billed
from _join_planned_actuals_and_credits_billed_prep
where billing_day = 8
group by 1, 2;

create or replace temporary table _billing_day8_calc1 as
select
        *,
        day_3_planned - day_3_actuals as day_3_pa_pct_numerator
from _billing_day8;

CREATE OR REPLACE TEMPORARY TABLE _billing_day8_calc2 AS
SELECT
        *,
        iff(day_3_planned = 0, 0, day_3_pa_pct_numerator/day_3_planned) as day_3_pa_pct
        --div0("day 3: p:a % numerator", "day 3 planned") as "day 3: p:a %"
FROM _billing_day8_calc1;

CREATE OR REPLACE TEMPORARY TABLE _billing_day8_filter1 AS
SELECT
        *
FROM _billing_day8_calc2
WHERE day_3_pa_pct >= 0;

CREATE OR REPLACE TEMPORARY TABLE _billing_day8_filter2 AS
SELECT
        billing_month,
        storefront,
        day_3_planned,
        day_3_actuals,
        day_3_to_be_billed,
        day_3_pa_pct_numerator,
        day_3_pa_pct * -1 AS day_3_pa_pct
        --"day 3: p:a %" * -1 as "day 3: p:a fix"
FROM _billing_day8_calc2
WHERE day_3_pa_pct < 0 OR day_3_pa_pct IS NULL;

CREATE OR REPLACE TEMPORARY TABLE _billing_day8_final AS
SELECT * FROM _billing_day8_filter1
UNION
SELECT * FROM _billing_day8_filter2;

CREATE OR REPLACE TEMPORARY TABLE _billing_day9 AS
SELECT
        billing_month,
        storefront,
        sum(total_planned) as day_4_planned,
        sum(total_1st_time_actuals) as day_4_actuals,
        sum(total_credits_try_to_bill) as day_4_to_be_billed
FROM _join_planned_actuals_and_credits_billed_prep
WHERE billing_day = 9
GROUP BY 1, 2;

CREATE OR REPLACE TEMPORARY TABLE _billing_day9_calc1 AS
SELECT
        *,
        day_4_planned - day_4_actuals AS day_4_pa_pct_numerator
FROM _billing_day9;

CREATE OR REPLACE TEMPORARY TABLE _billing_day9_calc2 AS
SELECT
        *,
        iff(day_4_planned = 0, 0, day_4_pa_pct_numerator/day_4_planned) AS day_4_pa_pct
        --div0("day 4: p:a % numerator", "day 4 planned") as "day 4: p:a %"
FROM _billing_day9_calc1;

CREATE OR REPLACE TEMPORARY TABLE _billing_day9_filter1 AS
SELECT
        *
FROM _billing_day9_calc2
WHERE day_4_pa_pct >= 0;

CREATE OR REPLACE TEMPORARY TABLE _billing_day9_filter2 AS
SELECT
        billing_month,
        storefront,
        day_4_planned,
        day_4_actuals,
        day_4_to_be_billed,
        day_4_pa_pct_numerator,
        day_4_pa_pct * -1 AS day_4_pa_pct
        --"day 4: p:a %" * -1 as "day 4: p:a fix"
FROM _billing_day9_calc2
WHERE day_4_pa_pct < 0 OR day_4_pa_pct IS NULL;

CREATE OR replace TEMPORARY TABLE _billing_day9_final AS
SELECT * FROM _billing_day9_filter1
UNION
SELECT * FROM _billing_day9_filter2;


CREATE OR replace TEMPORARY TABLE _billing_day10 AS
SELECT
        billing_month,
        storefront,
        sum(total_planned) AS day_5_planned,
        sum(total_1st_time_actuals) AS day_5_actuals,
        sum(total_credits_try_to_bill) AS day_5_to_be_billed
FROM _join_planned_actuals_and_credits_billed_prep
WHERE billing_day = 10
GROUP BY 1, 2;

CREATE OR REPLACE temporary table _billing_day10_calc1 AS
SELECT
        *,
        day_5_planned - day_5_actuals as day_5_pa_pct_numerator
FROM _billing_day10;

CREATE OR REPLACE TEMPORARY TABLE _billing_day10_calc2 AS
SELECT
        *,
        iff(day_5_planned = 0, 0, day_5_pa_pct_numerator/day_5_planned) AS day_5_pa_pct
        --div0("day 5: p:a % numerator", "day 5 planned") as "day 5: p:a %"
FROM _billing_day10_calc1;

CREATE OR REPLACE TEMPORARY TABLE _billing_day10_filter1 AS
SELECT
        *
FROM _billing_day10_calc2
WHERE day_5_pa_pct >= 0;

CREATE OR REPLACE TEMPORARY TABLE _billing_day10_filter2 AS
SELECT
        billing_month,
        storefront,
        day_5_planned,
        day_5_actuals,
        day_5_to_be_billed,
        day_5_pa_pct_numerator,
        day_5_pa_pct * -1 AS day_5_pa_pct
        --"day 5: p:a %" * -1 as "day 5: p:a fix"
FROM _billing_day10_calc2
WHERE day_5_pa_pct < 0 OR day_5_pa_pct IS NULL;

CREATE OR REPLACE TEMPORARY TABLE _billing_day10_final AS
SELECT * FROM _billing_day10_filter1
UNION
SELECT * FROM _billing_day10_filter2;

CREATE OR REPLACE TEMPORARY TABLE _billing_day11 AS
    SELECT
        billing_month,
        storefront,
        sum(total_planned) AS day_6_planned,
        sum(total_1st_time_actuals) AS day_6_actuals,
        sum(total_credits_try_to_bill) AS day_6_to_be_billed
FROM _join_planned_actuals_and_credits_billed_prep
WHERE billing_day = 11
GROUP BY 1, 2;

CREATE OR REPLACE TEMPORARY TABLE _billing_day11_calc1 AS
SELECT
        *,
        day_6_planned - day_6_actuals AS day_6_pa_pct_numerator
FROM _billing_day11;

CREATE OR REPLACE TEMPORARY TABLE _billing_day11_calc2 AS
SELECT
        *,
        iff(day_6_planned = 0, 0, day_6_pa_pct_numerator/day_6_planned) AS day_6_pa_pct
        --div0("day 6: p:a % numerator", "day 6 planned") as "day 6: p:a %"
FROM _billing_day11_calc1;

CREATE OR REPLACE TEMPORARY TABLE _billing_day11_filter1 AS
SELECT
        *
FROM _billing_day11_calc2
WHERE day_6_pa_pct >= 0;

CREATE OR REPLACE TEMPORARY TABLE _billing_day11_filter2 AS
SELECT
        billing_month,
        storefront,
        day_6_planned,
        day_6_actuals,
        day_6_to_be_billed,
        day_6_pa_pct_numerator,
        day_6_pa_pct * -1 AS day_6_pa_pct
        --"day 6: p:a %" * -1 as "day 6: p:a fix"
FROM _billing_day11_calc2
WHERE day_6_pa_pct < 0 OR day_6_pa_pct IS NULL;

CREATE OR REPLACE TEMPORARY TABLE _billing_day11_final AS
SELECT * FROM _billing_day11_filter1
UNION
SELECT * FROM _billing_day11_filter2;

CREATE OR REPLACE TEMPORARY TABLE _billing_day12 AS
SELECT
        billing_month,
        storefront,
        sum(total_planned) AS day_7_planned,
        sum(total_1st_time_actuals) AS day_7_actuals,
        sum(total_credits_try_to_bill) AS day_7_to_be_billed
FROM _join_planned_actuals_and_credits_billed_prep
WHERE billing_day = 12
GROUP BY 1, 2;

CREATE OR REPLACE TEMPORARY TABLE _billing_day12_calc1 AS
SELECT
        *,
        day_7_planned - day_7_actuals AS day_7_pa_pct_numerator
FROM _billing_day12;

CREATE OR REPLACE TEMPORARY TABLE _billing_day12_calc2 AS
SELECT
        *,
        iff(day_7_planned = 0, 0, day_7_pa_pct_numerator/day_7_planned) AS day_7_pa_pct
        --div0("day 7: p:a % numerator", "day 7 planned") as "day 7: p:a %"
FROM _billing_day12_calc1;

CREATE OR REPLACE TEMPORARY TABLE _billing_day12_filter1 AS
SELECT
        *
FROM _billing_day12_calc2
WHERE day_7_pa_pct >= 0;

CREATE OR REPLACE TEMPORARY table _billing_day12_filter2 AS
SELECT
        billing_month,
        storefront,
        day_7_planned,
        day_7_actuals,
        day_7_to_be_billed,
        day_7_pa_pct_numerator,
        day_7_pa_pct * -1 AS day_7_pa_pct
        --"day 7: p:a %" * -1 as "day 7: p:a fix"
FROM _billing_day12_calc2
WHERE day_7_pa_pct < 0 OR day_7_pa_pct IS NULL;

CREATE OR REPLACE TEMPORARY TABLE _billing_day12_final AS
SELECT * FROM _billing_day12_filter1
UNION
SELECT * FROM _billing_day12_filter2;

-- day13
CREATE OR REPLACE TEMPORARY TABLE _billing_day13 AS
SELECT
        billing_month,
        storefront,
        sum(total_planned) AS day_8_planned,
        sum(total_1st_time_actuals) AS day_8_actuals,
        sum(total_credits_try_to_bill) AS day_8_to_be_billed
FROM _join_planned_actuals_and_credits_billed_prep
WHERE billing_day = 13
GROUP BY 1, 2;

CREATE OR REPLACE TEMPORARY TABLE _billing_day13_calc1 AS
SELECT
        *,
        day_8_planned - day_8_actuals AS day_8_pa_pct_numerator
FROM _billing_day13;

CREATE OR REPLACE TEMPORARY TABLE _billing_day13_calc2 AS
SELECT
        *,
        iff(day_8_planned = 0, 0, day_8_pa_pct_numerator/day_8_planned) AS day_8_pa_pct
        --div0("day 8: p:a % numerator", "day 8 planned") as "day 8: p:a %"
FROM _billing_day13_calc1;

CREATE OR REPLACE TEMPORARY TABLE _billing_day13_filter1 AS
SELECT
        *
FROM _billing_day13_calc2
WHERE day_8_pa_pct >= 0;

CREATE OR REPLACE TEMPORARY TABLE _billing_day13_filter2 AS
SELECT
        billing_month,
        storefront,
        day_8_planned,
        day_8_actuals,
        day_8_to_be_billed,
        day_8_pa_pct_numerator,
        day_8_pa_pct * -1 AS day_8_pa_pct
        --"day 8: p:a %" * -1 as "day 8: p:a fix"
FROM _billing_day13_calc2
WHERE day_8_pa_pct < 0 OR day_8_pa_pct IS NULL;

CREATE OR REPLACE TEMPORARY TABLE _billing_day13_final AS
SELECT * FROM _billing_day13_filter1
UNION
SELECT * FROM _billing_day13_filter2;

-- day14
CREATE OR REPLACE TEMPORARY TABLE _billing_day14 AS
SELECT
        billing_month,
        storefront,
        sum(total_planned) AS day_9_planned,
        sum(total_1st_time_actuals) AS day_9_actuals,
        sum(total_credits_try_to_bill) AS day_9_to_be_billed
FROM _join_planned_actuals_and_credits_billed_prep
WHERE billing_day = 14
GROUP BY 1, 2;

CREATE OR REPLACE TEMPORARY TABLE _billing_day14_calc1 AS
SELECT
        *,
        day_9_planned - day_9_actuals AS day_9_pa_pct_numerator
FROM _billing_day14;

CREATE OR REPLACE TEMPORARY TABLE _billing_day14_calc2 AS
SELECT
        *,
        iff(day_9_planned = 0, 0, day_9_pa_pct_numerator/day_9_planned) AS day_9_pa_pct
        --div0("day 9: p:a % numerator", "day 9 planned") as "day 9: p:a %"
FROM _billing_day14_calc1;

CREATE OR REPLACE TEMPORARY TABLE _billing_day14_filter1 AS
SELECT
        *
FROM _billing_day14_calc2
WHERE day_9_pa_pct >= 0;

CREATE OR REPLACE TEMPORARY TABLE _billing_day14_filter2 AS
SELECT
        billing_month,
        storefront,
        day_9_planned,
        day_9_actuals,
        day_9_to_be_billed,
        day_9_pa_pct_numerator,
        day_9_pa_pct * -1 AS day_9_pa_pct
        --"day 9: p:a %" * -1 as "day 9: p:a fix"
FROM _billing_day14_calc2
WHERE day_9_pa_pct < 0 OR day_9_pa_pct IS NULL;

CREATE OR REPLACE TEMPORARY TABLE _billing_day14_final AS
SELECT * FROM _billing_day14_filter1
UNION
SELECT * FROM _billing_day14_filter2;

CREATE OR REPLACE TEMPORARY TABLE _temp1 AS
SELECT
        p.billing_month, p.storefront, curr_date, total_actuals, total_retries,total_planned, total_pa_pct_numerator, total_pa_pct,
        day_1_planned, day_1_actuals, day_1_to_be_billed, day_1_pa_pct_numerator, day_1_pa_pct,
        b.billing_month AS billing_month1, b.storefront AS storefront1
FROM _planned_and_actuals_final p
FULL JOIN _billing_day6_final b ON to_date(b.billing_month)=to_date(p.billing_month)
    AND b.storefront=p.storefront;

CREATE OR replace TEMPORARY TABLE _temp2 AS
SELECT
        t.billing_month, t.storefront, curr_date, total_actuals, total_retries,total_planned, total_pa_pct_numerator, total_pa_pct,
        day_1_planned, day_1_actuals, day_1_to_be_billed, day_1_pa_pct_numerator, day_1_pa_pct,
        day_2_planned, day_2_actuals, day_2_to_be_billed, day_2_pa_pct_numerator, day_2_pa_pct,
        t.billing_month1, t.storefront1,
        b.billing_month as billing_month2, b.storefront AS storefront2
FROM _temp1 t
FULL JOIN _billing_day7_final b ON to_date(b.billing_month)=to_date(t.billing_month)
    AND b.storefront=t.storefront;

CREATE OR REPLACE TEMPORARY TABLE _temp3 AS
SELECT
        t.billing_month, t.storefront, curr_date,total_actuals, total_retries,total_planned, total_pa_pct_numerator, total_pa_pct,
        day_1_planned, day_1_actuals, day_1_to_be_billed, day_1_pa_pct_numerator, day_1_pa_pct,
        day_2_planned, day_2_actuals, day_2_to_be_billed, day_2_pa_pct_numerator, day_2_pa_pct,
        day_3_planned, day_3_actuals, day_3_to_be_billed, day_3_pa_pct_numerator, day_3_pa_pct,
        t.billing_month1, t.storefront1,
        t.billing_month2, t.storefront2,
        b.billing_month AS billing_month3, b.storefront AS storefront3
FROM _temp2 t
FULL JOIN _billing_day8_final b ON to_date(b.billing_month)=to_date(t.billing_month)
    AND b.storefront=t.storefront;

CREATE OR REPLACE TEMPORARY TABLE _temp4 AS
SELECT
        t.billing_month, t.storefront,curr_date,total_actuals, total_retries,total_planned, total_pa_pct_numerator, total_pa_pct,
        day_1_planned, day_1_actuals, day_1_to_be_billed, day_1_pa_pct_numerator, day_1_pa_pct,
        day_2_planned, day_2_actuals, day_2_to_be_billed, day_2_pa_pct_numerator, day_2_pa_pct,
        day_3_planned, day_3_actuals, day_3_to_be_billed, day_3_pa_pct_numerator, day_3_pa_pct,
        day_4_planned, day_4_actuals, day_4_to_be_billed, day_4_pa_pct_numerator, day_4_pa_pct,
        t.billing_month1, t.storefront1,
        t.billing_month2, t.storefront2,
        t.billing_month3, t.storefront3,
        b.billing_month AS billing_month4, b.storefront AS storefront4
FROM _temp3 t
FULL JOIN _billing_day9_final b ON to_date(b.billing_month)=to_date(t.billing_month)
    AND b.storefront=t.storefront;

CREATE OR REPLACE TEMPORARY TABLE _temp5 AS
SELECT
        t.billing_month, t.storefront,curr_date,total_actuals, total_retries,total_planned, total_pa_pct_numerator, total_pa_pct,
        day_1_planned, day_1_actuals, day_1_to_be_billed, day_1_pa_pct_numerator, day_1_pa_pct,
        day_2_planned, day_2_actuals, day_2_to_be_billed, day_2_pa_pct_numerator, day_2_pa_pct,
        day_3_planned, day_3_actuals, day_3_to_be_billed, day_3_pa_pct_numerator, day_3_pa_pct,
        day_4_planned, day_4_actuals, day_4_to_be_billed, day_4_pa_pct_numerator, day_4_pa_pct,
        day_5_planned, day_5_actuals, day_5_to_be_billed, day_5_pa_pct_numerator, day_5_pa_pct,
        t.billing_month1, t.storefront1,
        t.billing_month2, t.storefront2,
        t.billing_month3, t.storefront3,
        t.billing_month4, t.storefront4,
        b.billing_month AS billing_month5, b.storefront AS storefront5
FROM _temp4 t
FULL JOIN _billing_day10_final b ON to_date(b.billing_month)=to_date(t.billing_month)
    AND b.storefront=t.storefront;

CREATE OR replace TEMPORARY TABLE _temp6 AS
SELECT
        t.billing_month, t.storefront,curr_date,total_actuals, total_retries,total_planned, total_pa_pct_numerator, total_pa_pct,
        day_1_planned, day_1_actuals, day_1_to_be_billed, day_1_pa_pct_numerator, day_1_pa_pct,
        day_2_planned, day_2_actuals, day_2_to_be_billed, day_2_pa_pct_numerator, day_2_pa_pct,
        day_3_planned, day_3_actuals, day_3_to_be_billed, day_3_pa_pct_numerator, day_3_pa_pct,
        day_4_planned, day_4_actuals, day_4_to_be_billed, day_4_pa_pct_numerator, day_4_pa_pct,
        day_5_planned, day_5_actuals, day_5_to_be_billed, day_5_pa_pct_numerator, day_5_pa_pct,
        day_6_planned, day_6_actuals, day_6_to_be_billed, day_6_pa_pct_numerator, day_6_pa_pct,
        t.billing_month1, t.storefront1,
        t.billing_month2, t.storefront2,
        t.billing_month3, t.storefront3,
        t.billing_month4, t.storefront4,
        t.billing_month5, t.storefront5,
        b.billing_month AS billing_month6, b.storefront AS storefront6
FROM _temp5 t
FULL JOIN _billing_day11_final b ON to_date(b.billing_month)=to_date(t.billing_month)
    AND b.storefront=t.storefront;

CREATE OR REPLACE TEMPORARY TABLE _temp7 AS
SELECT
        t.billing_month, t.storefront,curr_date,total_actuals, total_retries,total_planned, total_pa_pct_numerator, total_pa_pct,
        day_1_planned, day_1_actuals, day_1_to_be_billed, day_1_pa_pct_numerator, day_1_pa_pct,
        day_2_planned, day_2_actuals, day_2_to_be_billed, day_2_pa_pct_numerator, day_2_pa_pct,
        day_3_planned, day_3_actuals, day_3_to_be_billed, day_3_pa_pct_numerator, day_3_pa_pct,
        day_4_planned, day_4_actuals, day_4_to_be_billed, day_4_pa_pct_numerator, day_4_pa_pct,
        day_5_planned, day_5_actuals, day_5_to_be_billed, day_5_pa_pct_numerator, day_5_pa_pct,
        day_6_planned, day_6_actuals, day_6_to_be_billed, day_6_pa_pct_numerator, day_6_pa_pct,
        day_7_planned, day_7_actuals, day_7_to_be_billed, day_7_pa_pct_numerator, day_7_pa_pct,
        t.billing_month1, t.storefront1,
        t.billing_month2, t.storefront2,
        t.billing_month3, t.storefront3,
        t.billing_month4, t.storefront4,
        t.billing_month5, t.storefront5,
        t.billing_month6, t.storefront6,
        b.billing_month AS billing_month7, b.storefront AS storefront7
FROM _temp6 t
FULL JOIN _billing_day12_final b ON to_date(b.billing_month)=to_date(t.billing_month)
    AND b.storefront=t.storefront;

CREATE OR REPLACE TEMPORARY TABLE _temp8 AS
SELECT
        t.billing_month, t.storefront,curr_date,total_actuals, total_retries,total_planned, total_pa_pct_numerator, total_pa_pct,
        day_1_planned, day_1_actuals, day_1_to_be_billed, day_1_pa_pct_numerator, day_1_pa_pct,
        day_2_planned, day_2_actuals, day_2_to_be_billed, day_2_pa_pct_numerator, day_2_pa_pct,
        day_3_planned, day_3_actuals, day_3_to_be_billed, day_3_pa_pct_numerator, day_3_pa_pct,
        day_4_planned, day_4_actuals, day_4_to_be_billed, day_4_pa_pct_numerator, day_4_pa_pct,
        day_5_planned, day_5_actuals, day_5_to_be_billed, day_5_pa_pct_numerator, day_5_pa_pct,
        day_6_planned, day_6_actuals, day_6_to_be_billed, day_6_pa_pct_numerator, day_6_pa_pct,
        day_7_planned, day_7_actuals, day_7_to_be_billed, day_7_pa_pct_numerator, day_7_pa_pct,
        day_8_planned, day_8_actuals, day_8_to_be_billed, day_8_pa_pct_numerator, day_8_pa_pct,
       t.billing_month1, t.storefront1,
        t.billing_month2, t.storefront2,
        t.billing_month3, t.storefront3,
        t.billing_month4, t.storefront4,
        t.billing_month5, t.storefront5,
        t.billing_month6, t.storefront6,
        t.billing_month7, t.storefront7,
        b.billing_month AS billing_month8, b.storefront AS storefront8
FROM _temp7 t
FULL JOIN _billing_day13_final b ON to_date(b.billing_month)=to_date(t.billing_month)
    AND b.storefront=t.storefront;

CREATE OR REPLACE TEMPORARY TABLE _temp9 AS
SELECT
        t.curr_date,
        t.billing_month,
        t.storefront,
        b.day_9_planned, b.day_9_actuals, b.day_9_to_be_billed, b.day_9_pa_pct,
        t.day_8_planned, t.day_8_actuals, t.day_8_to_be_billed, t.day_8_pa_pct,
        t.day_7_planned, t.day_7_actuals, t.day_7_to_be_billed, t.day_7_pa_pct,
        t.day_6_planned, t.day_6_actuals, t.day_6_to_be_billed, t.day_6_pa_pct,
        t.day_5_planned, t.day_5_actuals, t.day_5_to_be_billed, t.day_5_pa_pct,
        t.day_4_planned, t.day_4_actuals, t.day_4_to_be_billed, t.day_4_pa_pct,
        t.day_3_planned, t.day_3_actuals, t.day_3_to_be_billed, t.day_3_pa_pct,
        t.day_2_planned, t.day_2_actuals, t.day_2_to_be_billed, t.day_2_pa_pct,
        t.day_1_planned, t.day_1_actuals, t.day_1_to_be_billed, t.day_1_pa_pct,
        t.total_actuals, t.total_retries, t.total_planned, t.total_pa_pct,
        t.billing_month1, t.storefront1,
        t.billing_month2, t.storefront2,
        t.billing_month3, t.storefront3,
        t.billing_month4, t.storefront4,
        t.billing_month5, t.storefront5,
        t.billing_month6, t.storefront6,
        t.billing_month7, t.storefront7,
        t.billing_month8, t.storefront8,
        b.billing_month AS billing_month9, b.storefront AS storefront9
FROM _temp8 t
FULL JOIN _billing_day14_final b ON to_date(b.billing_month)=to_date(t.billing_month)
    AND b.storefront=t.storefront;

CREATE OR REPLACE TEMPORARY TABLE _temp10 AS
SELECT
        t.curr_date,
        to_date(t.billing_month) AS billing_month,
        t.storefront AS storefront,
        t.day_9_planned, t.day_9_actuals, t.day_9_to_be_billed, t.day_9_pa_pct,
        t.day_8_planned, t.day_8_actuals, t.day_8_to_be_billed, t.day_8_pa_pct,
        t.day_7_planned, t.day_7_actuals, t.day_7_to_be_billed, t.day_7_pa_pct,
        t.day_6_planned, t.day_6_actuals, t.day_6_to_be_billed, t.day_6_pa_pct,
        t.day_5_planned, t.day_5_actuals, t.day_5_to_be_billed, t.day_5_pa_pct,
        t.day_4_planned, t.day_4_actuals, t.day_4_to_be_billed, t.day_4_pa_pct,
        t.day_3_planned, t.day_3_actuals, t.day_3_to_be_billed, t.day_3_pa_pct,
        t.day_2_planned, t.day_2_actuals, t.day_2_to_be_billed, t.day_2_pa_pct,
        t.day_1_planned, t.day_1_actuals, t.day_1_to_be_billed, t.day_1_pa_pct,
        t.total_actuals, t.total_retries, t.total_planned, t.total_pa_pct,
        t.billing_month1, t.storefront1,
        t.billing_month2, t.storefront2,
        t.billing_month3, t.storefront3,
        t.billing_month4, t.storefront4,
        t.billing_month5, t.storefront5,
        t.billing_month6, t.storefront6,
        t.billing_month7, t.storefront7,
        t.billing_month8, t.storefront8,
        t.billing_month9, t.storefront9
FROM _temp9 t;

--select * from temp9;
-- end of crazy joins
CREATE OR REPLACE TEMPORARY TABLE _planned_billing_calc1 AS
SELECT
        "Storefront" as storefront,
        "Billing Month" as billing_month,
        sum("Total Planned") AS total_total_planned
FROM gms.boss_planned_billings_dataset
GROUP by 1, 2;

CREATE OR REPLACE TEMPORARY TABLE _temp11 AS
SELECT
        t.curr_date,
        to_date(t.billing_month) AS billing_month,
        t.storefront,
        t.day_9_planned, t.day_9_actuals, t.day_9_to_be_billed, t.day_9_pa_pct,
        t.day_8_planned, t.day_8_actuals, t.day_8_to_be_billed, t.day_8_pa_pct,
        t.day_7_planned, t.day_7_actuals, t.day_7_to_be_billed, t.day_7_pa_pct,
        t.day_6_planned, t.day_6_actuals, t.day_6_to_be_billed, t.day_6_pa_pct,
        t.day_5_planned, t.day_5_actuals, t.day_5_to_be_billed, t.day_5_pa_pct,
        t.day_4_planned, t.day_4_actuals, t.day_4_to_be_billed, t.day_4_pa_pct,
        t.day_3_planned, t.day_3_actuals, t.day_3_to_be_billed, t.day_3_pa_pct,
        t.day_2_planned, t.day_2_actuals, t.day_2_to_be_billed, t.day_2_pa_pct,
        t.day_1_planned, t.day_1_actuals, t.day_1_to_be_billed, t.day_1_pa_pct,
        t.total_actuals, t.total_retries, t.total_planned, t.total_pa_pct,
        p.storefront AS storefront1,
        to_date(p.billing_month) AS billing_month1,
        p.total_total_planned
FROM _temp10 t
FULL JOIN _planned_billing_calc1 p ON to_date(p.billing_month)=to_date(t.billing_month)
    AND trim(p.storefront)=trim(t.storefront);

--select count(*) from join_planned_and_actuals2;
--select * from temp11;
CREATE OR REPLACE TEMPORARY TABLE _joining_bos_raw_data AS
SELECT
        t.curr_date,
        t.billing_month,
        t.storefront,
        day_9_planned, day_9_actuals, day_9_to_be_billed, day_9_pa_pct,
        day_8_planned, day_8_actuals, day_8_to_be_billed, day_8_pa_pct,
        day_7_planned, day_7_actuals, day_7_to_be_billed, day_7_pa_pct,
        day_6_planned, day_6_actuals, day_6_to_be_billed, day_6_pa_pct,
        day_5_planned, day_5_actuals, day_5_to_be_billed, day_5_pa_pct,
        day_4_planned, day_4_actuals, day_4_to_be_billed, day_4_pa_pct,
        day_3_planned, day_3_actuals, day_3_to_be_billed, day_3_pa_pct,
        day_2_planned, day_2_actuals, day_2_to_be_billed, day_2_pa_pct,
        day_1_planned, day_1_actuals, day_1_to_be_billed, day_1_pa_pct,
        total_actuals, total_retries, total_planned, total_pa_pct,
        storefront1, billing_month1, total_total_planned,
        b.first_successful_default,
        fpa_forecast_retries,
        fpa_forecast_first_time,
        b.billing_month AS billing_month2,
        b.storefront AS storefront2,
        m1_new_vips,
        first_successful_new_vips,
        m1_grace_vips,
        first_successful_grace_vips,
        first_successful_billed_credit_rate,
        first_successful_billed_credit_rate_trailing,
        retry_rate,
        retry_rate_trailing_6_months,
        m2_tenure_vips,
        m2_plus_vips,
        fpa_m2_credit_billings,
        fpa_m2_plus_credit_billings,
        m2_billings_credit_rate_fpa,
        m2_plus_billing_rate,
        total_billing_fpa_forecast
FROM _temp11 t
RIGHT JOIN gms.bos_raw_data b
    ON b.billing_month=t.billing_month AND b.storefront=t.storefront;

CREATE OR REPLACE TEMPORARY TABLE _filter1 AS
SELECT
        curr_date,
        billing_month2 AS billing_month,
        storefront2 AS storefront,
        day_9_planned, day_9_actuals, day_9_to_be_billed, day_9_pa_pct,
        day_8_planned, day_8_actuals, day_8_to_be_billed, day_8_pa_pct,
        day_7_planned, day_7_actuals, day_7_to_be_billed, day_7_pa_pct,
        day_6_planned, day_6_actuals, day_6_to_be_billed, day_6_pa_pct,
        day_5_planned, day_5_actuals, day_5_to_be_billed, day_5_pa_pct,
        day_4_planned, day_4_actuals, day_4_to_be_billed, day_4_pa_pct,
        day_3_planned, day_3_actuals, day_3_to_be_billed, day_3_pa_pct,
        day_2_planned, day_2_actuals, day_2_to_be_billed, day_2_pa_pct,
        day_1_planned, day_1_actuals, day_1_to_be_billed, day_1_pa_pct,
        total_actuals, total_retries, total_planned, total_pa_pct,
        storefront1, billing_month1, total_total_planned,
        first_successful_default, fpa_forecast_retries, fpa_forecast_first_time,
        billing_month2, storefront2, m1_new_vips, first_successful_new_vips, m1_grace_vips,
        first_successful_grace_vips, first_successful_billed_credit_rate, first_successful_billed_credit_rate_trailing,
        retry_rate, retry_rate_trailing_6_months, m2_tenure_vips, m2_plus_vips, fpa_m2_credit_billings,
        fpa_m2_plus_credit_billings, m2_billings_credit_rate_fpa, m2_plus_billing_rate, total_billing_fpa_forecast
FROM _joining_bos_raw_data
WHERE billing_month1 IS NULL;

CREATE OR REPLACE TEMPORARY TABLE _filter2 AS
SELECT
        curr_date,
        billing_month,
        storefront,
        day_9_planned, day_9_actuals, day_9_to_be_billed, day_9_pa_pct,
        day_8_planned, day_8_actuals, day_8_to_be_billed, day_8_pa_pct,
        day_7_planned, day_7_actuals, day_7_to_be_billed, day_7_pa_pct,
        day_6_planned, day_6_actuals, day_6_to_be_billed, day_6_pa_pct,
        day_5_planned, day_5_actuals, day_5_to_be_billed, day_5_pa_pct,
        day_4_planned, day_4_actuals, day_4_to_be_billed, day_4_pa_pct,
        day_3_planned, day_3_actuals, day_3_to_be_billed, day_3_pa_pct,
        day_2_planned, day_2_actuals, day_2_to_be_billed, day_2_pa_pct,
        day_1_planned, day_1_actuals, day_1_to_be_billed, day_1_pa_pct,
        total_actuals, total_retries, total_planned, total_pa_pct,
        storefront1, billing_month1, total_total_planned,
        first_successful_default, fpa_forecast_retries, fpa_forecast_first_time,
        billing_month2, storefront2, m1_new_vips, first_successful_new_vips, m1_grace_vips,
        first_successful_grace_vips, first_successful_billed_credit_rate, first_successful_billed_credit_rate_trailing,
        retry_rate, retry_rate_trailing_6_months, m2_tenure_vips, m2_plus_vips, fpa_m2_credit_billings,
        fpa_m2_plus_credit_billings, m2_billings_credit_rate_fpa, m2_plus_billing_rate, total_billing_fpa_forecast
FROM _joining_bos_raw_data a
WHERE billing_month1 IS NOT NULL;

CREATE OR REPLACE TEMPORARY TABLE _append_filter_1_and_2 AS
SELECT * FROM _filter1
UNION
SELECT * FROM _filter2;

CREATE OR REPLACE TEMPORARY TABLE _bos_raw_data_joining_planned_actuals AS
SELECT
        curr_date,
        billing_month,
        storefront,
        day_9_planned, day_9_actuals, day_9_to_be_billed, day_9_pa_pct,
        day_8_planned, day_8_actuals, day_8_to_be_billed, day_8_pa_pct,
        day_7_planned, day_7_actuals, day_7_to_be_billed, day_7_pa_pct,
        day_6_planned, day_6_actuals, day_6_to_be_billed, day_6_pa_pct,
        day_5_planned, day_5_actuals, day_5_to_be_billed, day_5_pa_pct,
        day_4_planned, day_4_actuals, day_4_to_be_billed, day_4_pa_pct,
        day_3_planned, day_3_actuals, day_3_to_be_billed, day_3_pa_pct,
        day_2_planned, day_2_actuals, day_2_to_be_billed, day_2_pa_pct,
        day_1_planned, day_1_actuals, day_1_to_be_billed, day_1_pa_pct,
        total_actuals, total_retries, total_planned, total_pa_pct, total_total_planned,
        first_successful_default,
        fpa_forecast_retries,
        fpa_forecast_first_time,
        m1_new_vips,
        first_successful_new_vips,
        m1_grace_vips,
        first_successful_grace_vips,
        first_successful_billed_credit_rate,
        first_successful_billed_credit_rate_trailing,
        retry_rate, retry_rate_trailing_6_months,
        m2_tenure_vips,
        m2_plus_vips,
        fpa_m2_credit_billings,
        fpa_m2_plus_credit_billings,
        m2_billings_credit_rate_fpa,
        m2_plus_billing_rate,
        total_billing_fpa_forecast
FROM _append_filter_1_and_2 a;

BEGIN;

DELETE FROM reporting_prod.gms.bos_joining_planned_actuals;

MERGE INTO reporting_prod.gms.bos_joining_planned_actuals t
    USING (SELECT *
                FROM (select *,
                        HASH(*) as meta_row_hash,
                        row_number() OVER (PARTITION BY billing_month,storefront order by curr_date) AS rn
                        FROM _append_filter_1_and_2) a
                where a.rn = 1) s
        ON equal_null(t.billing_month, s.billing_month)
        AND equal_null(t.storefront, s.storefront)
    WHEN NOT MATCHED THEN INSERT
        (curr_date,billing_month,storefront,day_9_planned,day_9_actuals,day_9_to_be_billed,day_9_pa_pct,day_8_planned,
    day_8_actuals,day_8_to_be_billed,day_8_pa_pct,day_7_planned,day_7_actuals,day_7_to_be_billed,day_7_pa_pct,
    day_6_planned,day_6_actuals,day_6_to_be_billed,day_6_pa_pct,day_5_planned,day_5_actuals,day_5_to_be_billed,
    day_5_pa_pct,day_4_planned,day_4_actuals,day_4_to_be_billed,day_4_pa_pct,day_3_planned,day_3_actuals,day_3_to_be_billed,
    day_3_pa_pct,day_2_planned,day_2_actuals,day_2_to_be_billed,day_2_pa_pct,day_1_planned,day_1_actuals,day_1_to_be_billed,
    day_1_pa_pct,total_actuals,total_retries,total_planned,total_pa_pct,total_total_planned,first_successful_default,
    fpa_forecast_retries,fpa_forecast_first_time,m1_new_vips,first_successful_new_vips,m1_grace_vips,
    first_successful_grace_vips,first_successful_billed_credit_rate,first_successful_billed_credit_rate_trailing,
    retry_rate,retry_rate_trailing_6_months,m2_tenure_vips,m2_plus_vips,fpa_m2_credit_billings,fpa_m2_plus_credit_billings,
    m2_billings_credit_rate_fpa,m2_plus_billing_rate,total_billing_fpa_forecast,meta_row_hash)

    VALUES  (curr_date,billing_month,storefront,day_9_planned,day_9_actuals,day_9_to_be_billed,day_9_pa_pct,day_8_planned,
    day_8_actuals,day_8_to_be_billed,day_8_pa_pct,day_7_planned,day_7_actuals,day_7_to_be_billed,day_7_pa_pct,
    day_6_planned,day_6_actuals,day_6_to_be_billed,day_6_pa_pct,day_5_planned,day_5_actuals,day_5_to_be_billed,
    day_5_pa_pct,day_4_planned,day_4_actuals,day_4_to_be_billed,day_4_pa_pct,day_3_planned,day_3_actuals,day_3_to_be_billed,
    day_3_pa_pct,day_2_planned,day_2_actuals,day_2_to_be_billed,day_2_pa_pct,day_1_planned,day_1_actuals,day_1_to_be_billed,
    day_1_pa_pct,total_actuals,total_retries,total_planned,total_pa_pct,total_total_planned,first_successful_default,
    fpa_forecast_retries,fpa_forecast_first_time,m1_new_vips,first_successful_new_vips,m1_grace_vips,
    first_successful_grace_vips,first_successful_billed_credit_rate,first_successful_billed_credit_rate_trailing,
    retry_rate,retry_rate_trailing_6_months,m2_tenure_vips,m2_plus_vips,fpa_m2_credit_billings,fpa_m2_plus_credit_billings,
    m2_billings_credit_rate_fpa,m2_plus_billing_rate,total_billing_fpa_forecast,meta_row_hash)
    WHEN MATCHED AND s.meta_row_hash != t.meta_row_hash
        AND coalesce(s.curr_date, '1900-01-01') > coalesce(t.curr_date, '1900-01-01')
        THEN
        UPDATE
            SET t.curr_date =s.curr_date
                ,t.day_9_planned = s.day_9_planned
                ,t.day_9_actuals = s.day_9_actuals
                ,t.day_9_to_be_billed = s.day_9_to_be_billed
                ,t.day_9_pa_pct = s.day_9_pa_pct
                ,t.day_8_planned = s.day_8_planned
                ,t.day_8_actuals = s.day_8_actuals
                ,t.day_8_to_be_billed = s.day_8_to_be_billed
                ,t.day_8_pa_pct = s.day_8_pa_pct
                ,t.day_7_planned = s.day_7_planned
                ,t.day_7_actuals = s.day_7_actuals
                ,t.day_7_to_be_billed = s.day_7_to_be_billed
                ,t.day_7_pa_pct = s.day_7_pa_pct
                ,t.day_6_planned = s.day_6_planned
                ,t.day_6_actuals = s.day_6_actuals
                ,t.day_6_to_be_billed = s.day_6_to_be_billed
                ,t.day_6_pa_pct = s.day_6_pa_pct
                ,t.day_5_planned = s.day_5_planned
                ,t.day_5_actuals = s.day_5_actuals
                ,t.day_5_to_be_billed = s.day_5_to_be_billed
                ,t.day_5_pa_pct = s.day_5_pa_pct
                ,t.day_4_planned = s.day_4_planned
                ,t.day_4_actuals = s.day_4_actuals
                ,t.day_4_to_be_billed = s.day_4_to_be_billed
                ,t.day_4_pa_pct = s.day_4_pa_pct
                ,t.day_3_planned = s.day_3_planned
                ,t.day_3_actuals = s.day_3_actuals
                ,t.day_3_to_be_billed = s.day_3_to_be_billed
                ,t.day_3_pa_pct = s.day_3_pa_pct
                ,t.day_2_planned = s.day_2_planned
                ,t.day_2_actuals = s.day_2_actuals
                ,t.day_2_to_be_billed = s.day_2_to_be_billed
                ,t.day_2_pa_pct = s.day_2_pa_pct
                ,t.day_1_planned = s.day_1_planned
                ,t.day_1_actuals = s.day_1_actuals
                ,t.day_1_to_be_billed = s.day_1_to_be_billed
                ,t.day_1_pa_pct = s.day_1_pa_pct
                ,t.total_actuals = s.total_actuals
                ,t.total_retries = s.total_retries
                ,t.total_planned = s.total_planned
                ,t.total_pa_pct = s.total_pa_pct
                ,t.total_total_planned = s.total_total_planned
                ,t.first_successful_default = s.first_successful_default
                ,t.fpa_forecast_retries = s.fpa_forecast_retries
                ,t.fpa_forecast_first_time = s.fpa_forecast_first_time
                ,t.m1_new_vips = s.m1_new_vips
                ,t.first_successful_new_vips = s.first_successful_new_vips
                ,t.m1_grace_vips = s.m1_grace_vips
                ,t.first_successful_grace_vips = s.first_successful_grace_vips
                ,t.first_successful_billed_credit_rate = s.first_successful_billed_credit_rate
                ,t.first_successful_billed_credit_rate_trailing = s.first_successful_billed_credit_rate_trailing
                ,t.retry_rate = s.retry_rate
                ,t.retry_rate_trailing_6_months = s.retry_rate_trailing_6_months
                ,t.m2_tenure_vips = s.m2_tenure_vips
                ,t.m2_plus_vips = s.m2_plus_vips
                ,t.fpa_m2_credit_billings = s.fpa_m2_credit_billings
                ,t.fpa_m2_plus_credit_billings = s.fpa_m2_plus_credit_billings
                ,t.m2_billings_credit_rate_fpa = s.m2_billings_credit_rate_fpa
                ,t.m2_plus_billing_rate = s.m2_plus_billing_rate
                ,t.total_billing_fpa_forecast = s.total_billing_fpa_forecast
                ,t.meta_row_hash = s.meta_row_hash
                ,t.meta_update_datetime = CURRENT_TIMESTAMP;

COMMIT;
