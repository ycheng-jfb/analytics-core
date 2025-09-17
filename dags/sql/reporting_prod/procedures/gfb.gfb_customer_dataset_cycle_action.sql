set low_watermark_ltz = (
    select
        max(a.MONTH_DATE)
    from REPORTING_PROD.GFB.gfb_customer_dataset_base a
    );


CREATE OR REPLACE TEMP TABLE _customer_actions_base AS
SELECT
    cd.customer_id
    ,cd.month_date
    ,st.time_zone
    ,cd.activation_local_datetime
    ,cd.cancel_local_datetime
FROM REPORTING_PROD.GFB.gfb_customer_dataset_base AS cd
JOIN edw_prod.reference.store_timezone AS st
    ON st.store_id = cd.store_id
where
    cd.MONTH_DATE >= $low_watermark_ltz
    and cd.MONTH_DATE < date_trunc('MONTH', current_date());


delete from REPORTING_PROD.GFB.gfb_customer_dataset_cycle_action a
where
    a.MONTH_DATE >= $low_watermark_ltz;


insert into REPORTING_PROD.GFB.gfb_customer_dataset_cycle_action
SELECT
    vip.customer_id
    ,vip.month_date
    ,DATEDIFF(MONTHS, vip.activation_local_datetime, COALESCE(vip.cancel_local_datetime, vip.month_date)) AS months_active
    ,COUNT(DISTINCT CASE
            WHEN clvm.is_successful_billing = 'TRUE'
            THEN clvm.month_date
            ELSE NULL END) AS successful_billing_count
    ,COUNT(DISTINCT CASE
            WHEN clvm.is_failed_billing = 'TRUE'
            THEN clvm.month_date
            ELSE NULL END) AS failed_billing_count
    ,COUNT(DISTINCT CASE
            WHEN clvm.is_skip = 'TRUE'
            THEN clvm.month_date ELSE NULL END) AS skip_count
    ,COUNT(DISTINCT CASE
            WHEN clvm.is_merch_purchaser = 'TRUE' THEN clvm.month_date
            ELSE NULL END) AS merch_purchase_count
FROM _customer_actions_base AS vip
JOIN edw_prod.analytics_base.customer_lifetime_value_monthly AS clvm
    ON edw_prod.stg.udf_unconcat_brand(clvm.CUSTOMER_ID) = vip.customer_id
    AND clvm.month_date = vip.month_date
GROUP BY
    vip.customer_id
    ,months_active
    ,vip.month_date;
