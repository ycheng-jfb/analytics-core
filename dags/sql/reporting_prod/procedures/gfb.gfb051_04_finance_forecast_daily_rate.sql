set end_date = date_trunc(month, dateadd(day, -1, current_date()));
set start_date = dateadd(year, -2, $end_date);


create or replace temporary table _credit_billing_rate as
select
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.COUNTRY
    ,a.ISSUED_DATE
    ,a.issued_amount
    ,sum(a.issued_amount) over (partition by a.BUSINESS_UNIT, a.REGION, a.COUNTRY, date_trunc(month, a.ISSUED_DATE)) as issued_amount_monthly
    ,a.issued_amount * 1.0/issued_amount_monthly as credit_billing_rate_daily
from
(
    select
        mca.BUSINESS_UNIT
        ,mca.REGION
        ,mca.COUNTRY
        ,mca.ISSUED_DATE

        ,sum(mca.ISSUED_AMOUNT_LOCAL) as issued_amount
    from REPORTING_PROD.GFB.GFB_MEMBERSHIP_CREDIT_ACTIVITY mca
    where
        mca.BUSINESS_UNIT in ('JUSTFAB', 'SHOEDAZZLE')
        and mca.REGION = 'NA'
        and mca.CREDIT_TYPE = 'Fixed Credit'
        and mca.CREDIT_REASON = 'Membership Credit'
        and mca.ISSUED_DATE >= $start_date
        and mca.ISSUED_DATE < $end_date
    group by
        mca.BUSINESS_UNIT
        ,mca.REGION
        ,mca.COUNTRY
        ,mca.ISSUED_DATE
) a;


create or replace temporary table _credit_redeem_rate as
select
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.COUNTRY
    ,a.REDEEMED_ORDER_DATE
    ,a.redeemed_amount
    ,sum(a.redeemed_amount) over (partition by a.BUSINESS_UNIT, a.REGION, a.COUNTRY, date_trunc(month, a.REDEEMED_ORDER_DATE)) as redeemed_amount_monthly
    ,a.redeemed_amount * 1.0/redeemed_amount_monthly as credit_redeemed_rate_daily
from
(
    select
        mca.BUSINESS_UNIT
        ,mca.REGION
        ,mca.COUNTRY
        ,mca.REDEEMED_ORDER_DATE

        ,sum(mca.REDEEMED_AMOUNT_LOCAL) as redeemed_amount
    from REPORTING_PROD.GFB.GFB_MEMBERSHIP_CREDIT_ACTIVITY mca
    where
        mca.BUSINESS_UNIT in ('JUSTFAB', 'SHOEDAZZLE')
        and mca.REGION = 'NA'
        and mca.CREDIT_TYPE = 'Fixed Credit'
        and mca.CREDIT_REASON = 'Membership Credit'
        and mca.REDEEMED_ORDER_DATE >= $start_date
        and mca.REDEEMED_ORDER_DATE < $end_date
    group by
        mca.BUSINESS_UNIT
        ,mca.REGION
        ,mca.COUNTRY
        ,mca.REDEEMED_ORDER_DATE
) a;


create or replace temporary table _credit_cancel_rate as
select
    b.BUSINESS_UNIT
    ,b.REGION
    ,b.COUNTRY
    ,b.cancel_date
    ,b.cancel_amount
    ,sum(b.cancel_amount) over (partition by b.BUSINESS_UNIT, b.REGION, b.COUNTRY, date_trunc(month, b.cancel_date)) as cancelled_amount_monthly
    ,b.cancel_amount * 1.0/cancelled_amount_monthly as credit_cancel_rate_daily
from
(
    select
        a.BUSINESS_UNIT
        ,a.REGION
        ,a.COUNTRY
        ,a.cancel_date
        ,sum(a.cancel_amount)  as cancel_amount
    from
    (
        select
            mca.BUSINESS_UNIT
            ,mca.REGION
            ,mca.country
            ,mca.REFUND_DATE as cancel_date
            ,sum(mca.TOTAL_REFUND_CASH_AMOUNT) as cancel_amount
        from REPORTING_PROD.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE mca
        where
            mca.BUSINESS_UNIT in ('JUSTFAB', 'SHOEDAZZLE')
            and mca.REGION = 'NA'
            and mca.ORDER_CLASSIFICATION = 'credit billing'
            and mca.REFUND_DATE >= $start_date
            and mca.REFUND_DATE < $end_date
        group by
            mca.BUSINESS_UNIT
            ,mca.REGION
            ,mca.country
            ,mca.REFUND_DATE

        union

        select
            mca.BUSINESS_UNIT
            ,mca.REGION
            ,mca.country
            ,mca.CHARGEBACK_DATE as cancel_date
            ,sum(mca.TOTAL_CHARGEBACK_AMOUNT) as cancel_amount
        from REPORTING_PROD.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE mca
        where
            mca.BUSINESS_UNIT in ('JUSTFAB', 'SHOEDAZZLE')
            and mca.REGION = 'NA'
            and mca.ORDER_CLASSIFICATION = 'credit billing'
            and mca.CHARGEBACK_DATE >= $start_date
            and mca.CHARGEBACK_DATE < $end_date
        group by
            mca.BUSINESS_UNIT
            ,mca.REGION
            ,mca.country
            ,mca.CHARGEBACK_DATE
    ) a
    group by
        a.BUSINESS_UNIT
        ,a.REGION
        ,a.COUNTRY
        ,a.cancel_date
) b;


create or replace transient table REPORTING_PROD.GFB.gfb051_04_finance_forecast_daily_rate as
select
    coalesce(cbr.BUSINESS_UNIT, crr.BUSINESS_UNIT, ccr.BUSINESS_UNIT) as BUSINESS_UNIT
    ,coalesce(cbr.REGION, crr.REGION, ccr.REGION) as region
    ,coalesce(cbr.COUNTRY, crr.COUNTRY, ccr.COUNTRY) as country
    ,coalesce(cbr.ISSUED_DATE, crr.REDEEMED_ORDER_DATE, ccr.cancel_date) as date

    ,sum(coalesce(cbr.credit_billing_rate_daily, 0)) as credit_billing_rate_daily
    ,sum(coalesce(crr.credit_redeemed_rate_daily, 0)) as credit_redeemed_rate_daily
    ,sum(coalesce(ccr.credit_cancel_rate_daily, 0)) as credit_cancel_rate_daily
from _credit_billing_rate cbr
full join _credit_redeem_rate crr
    on crr.BUSINESS_UNIT = cbr.BUSINESS_UNIT
    and crr.REGION = cbr.REGION
    and crr.COUNTRY = cbr.COUNTRY
    and crr.REDEEMED_ORDER_DATE = cbr.ISSUED_DATE
full join _credit_cancel_rate ccr
    on ccr.BUSINESS_UNIT = cbr.BUSINESS_UNIT
    and ccr.REGION = cbr.REGION
    and ccr.COUNTRY = cbr.COUNTRY
    and ccr.cancel_date = cbr.ISSUED_DATE
group by
    coalesce(cbr.BUSINESS_UNIT, crr.BUSINESS_UNIT, ccr.BUSINESS_UNIT)
    ,coalesce(cbr.REGION, crr.REGION, ccr.REGION)
    ,coalesce(cbr.COUNTRY, crr.COUNTRY, ccr.COUNTRY)
    ,coalesce(cbr.ISSUED_DATE, crr.REDEEMED_ORDER_DATE, ccr.cancel_date);
