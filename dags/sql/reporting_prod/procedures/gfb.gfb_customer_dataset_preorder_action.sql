set low_watermark_ltz = (
    select
        max(a.MONTH_DATE)
    from REPORTING_PROD.GFB.gfb_customer_dataset_base a
    );


CREATE OR REPLACE TEMP TABLE _pre_order_base AS
SELECT
    cd.customer_id
    ,cd.month_date
    ,cd.activation_local_datetime
    ,cd.cancel_local_datetime
    ,m.MEMBERSHIP_ID
FROM REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_BASE cd
join LAKE_JFB_VIEW.ULTRA_MERCHANT.MEMBERSHIP m
    on m.CUSTOMER_ID = cd.CUSTOMER_ID
where
    cd.MONTH_DATE >= $low_watermark_ltz
    and cd.MONTH_DATE < date_trunc('MONTH', current_date());


CREATE OR REPLACE TEMPORARY TABLE _pre_orders_customers AS
SELECT DISTINCT
    pob.customer_id,
    pob.month_date,
    wl.datetime_added::DATE AS pre_order_date
FROM _pre_order_base pob
JOIN  lake_jfb_view.ultra_merchant.membership_product_wait_list wl
    ON pob.membership_id = wl.membership_id
WHERE
    wl.statuscode IN (3890, 3891, 3892)
    AND wl.membership_product_wait_list_type_id = 3
    AND pob.month_date = DATE_TRUNC('MONTH', pre_order_date);


delete from REPORTING_PROD.GFB.gfb_customer_dataset_preorder_action a
where
    a.MONTH_DATE >= $low_watermark_ltz;


insert into REPORTING_PROD.GFB.gfb_customer_dataset_preorder_action
SELECT DISTINCT
    vip.customer_id,
    vip.month_date,
    COUNT(DISTINCT p.pre_order_date) AS pre_order_count
FROM REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_BASE vip
JOIN _pre_orders_customers p
    ON p.customer_id = vip.customer_id
    AND vip.month_date = p.month_date
GROUP BY
    vip.customer_id,
    vip.month_date;
