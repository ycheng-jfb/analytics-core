set low_watermark_ltz = (
    select
        max(a.MONTH_DATE)
    from REPORTING_PROD.GFB.gfb_customer_dataset_base a
    );


CREATE OR REPLACE TEMP TABLE _promo_usage_base AS
SELECT
    cd.customer_id
    ,cd.month_date
    ,cd.cancel_local_datetime
    ,cd.activation_local_datetime
FROM REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_BASE cd
where
    cd.MONTH_DATE >= $low_watermark_ltz
    and cd.MONTH_DATE < date_trunc('MONTH', current_date());


CREATE OR REPLACE TEMPORARY TABLE _promo_usage_pre_stg AS
select
    a.customer_id
    ,a.month_date
    ,a.promo_orders
    ,a.repeat_promo_orders
    ,a.orders
    ,a.repeat_orders
    ,a.promo_orders / NULLIF(a.orders, 0) AS percent_orders_with_promo
    ,a.repeat_promo_orders / NULLIF(a.repeat_orders, 0) AS percent_repeat_orders_with_promo
from
(
    select
        base.customer_id
        ,base.month_date

        ,count(distinct olp.ORDER_ID) as orders
        ,count(distinct case
                when olp.ORDER_TYPE = 'vip repeat'
                then olp.ORDER_ID end) as repeat_orders
        ,count(distinct case
                when coalesce(PROMO_ID_1, PROMO_ID_2) is not null
                then olp.ORDER_ID end) as promo_orders
        ,count(distinct case
                when olp.ORDER_TYPE = 'vip repeat' and coalesce(PROMO_ID_1, PROMO_ID_2) is not null
                then olp.ORDER_ID end) as repeat_promo_orders
    from _promo_usage_base base
    join REPORTING_PROD.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE olp
        on olp.CUSTOMER_ID = base.customer_id
        and date_trunc(month, olp.ORDER_DATE) = base.month_date
        and olp.ORDER_CLASSIFICATION = 'product order'
    group by
        base.customer_id
        ,base.month_date
) a;


delete from REPORTING_PROD.GFB.gfb_customer_dataset_promo_usage a
where
    a.MONTH_DATE >= $low_watermark_ltz;


insert into REPORTING_PROD.GFB.gfb_customer_dataset_promo_usage
SELECT
    customer_id,
    month_date,
    promo_orders,
    repeat_promo_orders,
    orders,
    repeat_orders,
    percent_orders_with_promo,
    percent_repeat_orders_with_promo
FROM _promo_usage_pre_stg;
