set low_watermark_ltz = (
    select
        max(a.MONTH_DATE)
    from REPORTING_PROD.GFB.gfb_customer_dataset_base a
    );


CREATE OR REPLACE TEMP TABLE _customer_base AS
SELECT
    cd.customer_id
    ,cd.month_date
    ,cd.activation_local_datetime
    ,cd.cancel_local_datetime
FROM REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_BASE cd
where
    cd.MONTH_DATE >= $low_watermark_ltz
    and cd.MONTH_DATE < date_trunc('MONTH', current_date());


CREATE OR REPLACE TEMPORARY TABLE _pdp_views AS
SELECT
    b.customer_id,
    b.month_date,
    b.activation_local_datetime,
    count(distinct a.messageId) AS pdp_views
FROM lake.SEGMENT_GFB.JAVASCRIPT_JUSTFAB_PRODUCT_VIEWED a
JOIN _customer_base b
    ON a.PROPERTIES_CUSTOMER_ID = cast(b.customer_id as varchar(200))
    AND DATE_TRUNC('MONTH', a.timestamp::date) = b.month_date
    AND a.timestamp::date <= COALESCE(b.cancel_local_datetime::DATE, current_date())
GROUP BY
    b.month_date,
    b.customer_id,
    b.activation_local_datetime

union

SELECT
    b.customer_id,
    b.month_date,
    b.activation_local_datetime,
    count(distinct a.messageId) AS pdp_views
FROM lake.SEGMENT_GFB.JAVASCRIPT_FABKIDS_PRODUCT_VIEWED a
JOIN _customer_base b
    ON a.PROPERTIES_CUSTOMER_ID = cast(b.customer_id as varchar(200))
    AND DATE_TRUNC('MONTH', a.timestamp::date) = b.month_date
    AND a.timestamp::date <= COALESCE(b.cancel_local_datetime::DATE, current_date())
GROUP BY
    b.month_date,
    b.customer_id,
    b.activation_local_datetime

union

SELECT
    b.customer_id,
    b.month_date,
    b.activation_local_datetime,
    count(distinct a.messageId) AS pdp_views
FROM lake.SEGMENT_GFB.JAVASCRIPT_SHOEDAZZLE_PRODUCT_VIEWED a
JOIN _customer_base b
    ON a.PROPERTIES_CUSTOMER_ID = cast(b.customer_id as varchar(200))
    AND DATE_TRUNC('MONTH', a.timestamp::date) = b.month_date
    AND a.timestamp::date <= COALESCE(b.cancel_local_datetime::DATE, current_date())
GROUP BY
    b.month_date,
    b.customer_id,
    b.activation_local_datetime;


CREATE OR REPLACE TEMPORARY TABLE _favorites AS
SELECT
    cb.customer_id,
    cb.month_date,
    cb.activation_local_datetime,
    SUM(ifnull(cb.product_count, 0)) as fav_count
FROM
(
    SELECT
        cb.customer_id,
        cb.month_date,
        cb.activation_local_datetime,
        mp.datetime_added,
        count(distinct mp.product_id) as product_count
    FROM lake_jfb_view.ultra_merchant.membership_product mp
    JOIN lake_jfb_view.ultra_merchant.membership_product_type mpt
        ON mp.membership_product_type_id = mpt.membership_product_type_id
        AND mpt.label = 'Wish List'
--         AND mp.active = 1
    JOIN lake_jfb_view.ultra_merchant.membership mr
        ON mr.membership_id = mp.membership_id
    JOIN _customer_base cb
        ON cb.customer_id = mr.customer_id
        AND date_trunc('MONTH', mp.datetime_added::DATE)= cb.month_date
        AND mp.datetime_added::DATE <= COALESCE(TO_DATE(cb.cancel_local_datetime), CURRENT_DATE())
        AND mp.datetime_added::DATE >= cb.activation_local_datetime::DATE
    GROUP BY
        cb.customer_id,
        cb.month_date,
        cb.activation_local_datetime,
        mp.datetime_added
) as cb
    group by
        cb.customer_id,
        cb.month_date,
        cb.activation_local_datetime;


delete from REPORTING_PROD.GFB.gfb_customer_dataset_loyalty_action a
where
    a.MONTH_DATE >= $low_watermark_ltz;


insert into REPORTING_PROD.GFB.gfb_customer_dataset_misc_action
SELECT
    cb.customer_id,
    cb.month_date,
    cb.activation_local_datetime,
    cb.cancel_local_datetime,
    pv.pdp_views,
    iff(f.customer_id is not null, 1, 0) AS is_favorites,
    f.fav_count as favorites_count
FROM _customer_base cb
LEFT JOIN _pdp_views pv
    ON pv.customer_id = cb.customer_id
    AND pv.month_date = cb.month_date
    AND pv.activation_local_datetime = cb.activation_local_datetime
LEFT JOIN _favorites f
    ON f.customer_id = cb.customer_id
    AND f.month_date = cb.month_date
    AND f.activation_local_datetime = cb.activation_local_datetime
WHERE
    f.customer_id is not null
    OR pv.customer_id is not null
