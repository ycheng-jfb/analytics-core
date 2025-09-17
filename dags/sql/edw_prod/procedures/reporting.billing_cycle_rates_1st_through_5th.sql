SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMP TABLE _billing_cycle_days AS
SELECT DISTINCT
            month_date,
            full_date
FROM edw_prod.data_model_jfb.dim_date
WHERE month_date >= TO_DATE(DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()))
    AND month_date < TO_DATE(DATEADD(MONTH, 1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP())))
    AND DAY(full_date) <= iff(DAY(DATEADD(DAY, -1, CURRENT_TIMESTAMP())) > 5, 5, DAY(DATEADD(DAY, -1, CURRENT_TIMESTAMP())));

CREATE OR REPLACE TEMP TABLE _bop_vips AS
SELECT CASE
        WHEN is_scrubs_customer = TRUE AND st.store_brand = 'Fabletics' AND UPPER(ltv.gender) != 'M' THEN 'Scrubs Womens'
        WHEN is_scrubs_customer = TRUE AND st.store_brand = 'Fabletics' AND UPPER(ltv.gender) = 'M' THEN 'Scrubs Mens'
        WHEN st.store_brand = 'Fabletics' AND UPPER(ltv.gender) != 'M' THEN 'Fabletics Womens'
        WHEN st.store_brand = 'Fabletics' AND UPPER(ltv.gender) = 'M' THEN 'Fabletics Mens'
        ELSE st.store_brand END AS store_brand,
    st.store_country,
    ltv.gender                  AS customer_gender,
    st.store_region,
    ltv.is_retail_vip,
    ltv.month_date              AS period_month_date,
    p.period_id,
    edw_prod.stg.udf_unconcat_brand(ltv.customer_id) as customer_id,
    m.membership_id,
    'M' || (1 + DATEDIFF(MONTH,  ltv.vip_cohort_month_date, ltv.month_date))       AS tenure
FROM edw_prod.analytics_base.customer_lifetime_value_monthly ltv
JOIN lake_jfb_view.ultra_merchant.period p
    ON p.date_period_start = ltv.month_date
JOIN edw_prod.data_model_jfb.dim_store st
    ON st.store_id = ltv.store_id
JOIN lake_jfb_view.ultra_merchant.membership m
    ON m.customer_id = edw_prod.stg.udf_unconcat_brand(ltv.customer_id)
WHERE is_bop_vip = 1
    AND p.type = 'monthly';

CREATE OR REPLACE TEMP TABLE _bop_vips_final AS
SELECT
    a.store_brand,
    a.store_country,
    a.customer_gender,
    a.store_region,
    a.is_retail_vip,
    a.period_month_date,
    a.period_id,
    a.customer_id,
    a.membership_id,
    a.tenure,
    DAY(full_date) AS max_day
FROM _bop_vips a
JOIN _billing_cycle_days d
    ON a.period_month_date = d.month_date;

CREATE OR REPLACE TEMP TABLE _skips AS
SELECT DISTINCT
        period_month_date,
        DAY(d.full_date) AS max_day,
        customer_id
FROM _bop_vips b
JOIN _billing_cycle_days d
    ON d.month_date = b.period_month_date
JOIN lake_jfb_view.ultra_merchant.membership_skip ms
    ON ms.membership_id = b.membership_id
    AND DATE_TRUNC(MONTH, ms.datetime_added::DATE) = b.period_month_date
    AND DAY(ms.datetime_added) <= DAY(d.full_date);

CREATE OR REPLACE TEMP TABLE _snooze AS
SELECT DISTINCT
 period_month_date,
 DAY(d.full_date) AS max_day,
 customer_id
FROM _bop_vips b
JOIN _billing_cycle_days d
 ON d.month_date = b.period_month_date
JOIN lake_jfb_view.ultra_merchant.membership_skip ms
 ON ms.membership_id = b.membership_id
 AND DATE_TRUNC(MONTH, ms.datetime_added::DATE) = b.period_month_date
 AND DAY(ms.datetime_added) <= DAY(d.full_date)
WHERE membership_skip_reason_id = 36;

CREATE OR REPLACE TEMP TABLE _product_orders AS
SELECT DISTINCT b.period_month_date,
        DAY(d.full_date) AS max_day,
        b.customer_id,
        sum(iff(d.full_date=fo.order_local_datetime::date,fo.product_gross_revenue_local_amount*fo.reporting_usd_conversion_rate,0)) as product_gross_revenue
FROM _bop_vips b
JOIN _billing_cycle_days d
    ON d.month_date = b.period_month_date
JOIN edw_prod.data_model_jfb.fact_order fo
    ON fo.CUSTOMER_ID = b.CUSTOMER_ID
JOIN edw_prod.data_model_jfb.dim_order_sales_channel doc
    ON doc.order_sales_channel_key = fo.order_sales_channel_key
JOIN edw_prod.data_model_jfb.dim_order_status os
    ON os.order_status_key = fo.order_status_key
WHERE doc.order_classification_l1 = 'Product Order'
    AND os.order_status IN ('Success', 'Pending')
    AND DAY(fo.order_local_datetime) <= DAY(d.full_date)
    AND DATE_TRUNC(MONTH, fo.order_local_datetime::DATE) = b.period_month_date
GROUP BY period_month_date,
         max_day,
         b.customer_id;

CREATE OR REPLACE TEMP TABLE _sessions AS
SELECT DISTINCT b.period_month_date,
        DAY(d.full_date) AS max_day,
        b.customer_id
FROM _bop_vips b
JOIN _billing_cycle_days d
    ON d.month_date = b.period_month_date
JOIN lake_jfb_view.ultra_merchant.session s
    ON s.CUSTOMER_ID = b.CUSTOMER_ID
    AND DATE_TRUNC(MONTH, s.datetime_added::DATE) = period_month_date
    AND DAY(s.datetime_added) <= DAY(d.full_date);

CREATE OR REPLACE TEMP TABLE _cancels AS
SELECT DISTINCT b.period_month_date,
        DAY(d.full_date) AS max_day,
        b.customer_id
FROM _bop_vips b
JOIN _billing_cycle_days d
    ON d.month_date = b.period_month_date
JOIN edw_prod.data_model_jfb.fact_activation v
    ON v.CUSTOMER_ID = b.CUSTOMER_ID
    AND DATE_TRUNC(MONTH, v.cancellation_local_datetime::DATE) = period_month_date
    AND DAY(v.cancellation_local_datetime) <= DAY(d.full_date);

CREATE OR REPLACE TEMP TABLE _billing_cycle_rates_1st_through_5th_stg AS
SELECT b.store_brand,
       b.store_region,
       b.store_country,
       b.is_retail_vip,
       b.period_month_date,
       b.max_day,
       b.tenure,
       COUNT(DISTINCT se.customer_id) AS login_unique,
       COUNT(DISTINCT COALESCE(s.customer_id, sn.customer_id)) AS skip_unique,
       COUNT(DISTINCT sn.customer_id) AS snooze_unique,
       COUNT(DISTINCT IFF((se.customer_id IS NULL OR sn.customer_id IS NULL) AND se.customer_id IS NULL, s.customer_id,NULL))   AS skip_no_login,
       COUNT(DISTINCT po.customer_id) AS purchase_prod_order_unique,
       COUNT(IFF(s.customer_id IS NOT NULL OR sn.customer_id IS NOT NULL OR po.customer_id IS NOT NULL, b.customer_id,NULL))    AS skip_or_purchase_unique,
       COUNT(b.customer_id) AS bop_vips,
       COUNT(c.customer_id) AS cancels_unique,
       CASE
           WHEN (b.store_brand = 'Fabletics' AND b.store_region = 'NA' AND b.period_month_date < '2021-01-01')
               OR (b.store_brand = 'Fabletics' AND b.store_region = 'EU' AND b.period_month_date < '2021-11-01')
               OR (b.store_brand = 'Savage X' AND b.store_region = 'NA' AND b.period_month_date < '2022-01-01')
               OR (b.store_brand = 'JustFab' AND b.store_country = 'US' AND b.period_month_date < '2023-08-01')
                OR (b.store_brand = 'FabKids' AND b.store_country = 'US' AND b.period_month_date < '2023-10-01')
               OR (b.store_brand = 'ShoeDazzle' AND b.store_country = 'US' AND b.period_month_date < '2023-10-01')
               OR (b.store_brand = 'JustFab' AND b.store_country = 'CA' AND b.period_month_date < '2023-11-01')
             THEN
               COUNT(DISTINCT IFF(s.customer_id IS NULL AND sn.customer_id IS NULL AND po.customer_id IS NULL AND
                                  c.customer_id IS NULL, b.customer_id, NULL))
           ELSE
               COUNT(DISTINCT
                      IFF(s.customer_id IS NULL AND sn.customer_id IS NULL AND c.customer_id IS NULL, b.customer_id,
                          NULL))
       END AS eligible_to_be_billed,
       SUM(po.product_gross_revenue) AS product_gross_revenue
FROM _bop_vips_final b
LEFT JOIN _skips s
    ON b.customer_id = s.customer_id
    AND b.period_month_date = s.period_month_date
    AND b.max_day = s.max_day
LEFT JOIN _snooze sn
    ON b.customer_id = sn.customer_id
    AND b.period_month_date = sn.period_month_date
    AND b.max_day = sn.max_day
LEFT JOIN _product_orders po
    ON b.customer_id = po.customer_id
    AND b.period_month_date = po.period_month_date
    AND b.max_day = po.max_day
LEFT JOIN _sessions se
    ON b.customer_id = se.customer_id
    AND b.period_month_date = se.period_month_date
    AND b.max_day = se.max_day
LEFT JOIN _cancels c
    ON c.customer_id = b.customer_id
    AND b.period_month_date = c.period_month_date
    AND b.max_day = c.max_day
GROUP BY store_brand,
         store_region,
         store_country,
         is_retail_vip,
         b.period_month_date,
         b.max_day,
         b.tenure;

/* Snapshot of this table is available till Period Month: May, 2023. Table_name: edw_prod.snapshot.billing_cycle_rates_1st_through_5th*/

MERGE INTO edw_prod.reporting.billing_cycle_rates_1st_through_5th t
USING (
        select *, hash(*) as meta_row_hash
        from _billing_cycle_rates_1st_through_5th_stg
      ) s
    ON t.store_brand = s.store_brand AND
       t.store_region = s.store_region AND
       t.store_country = s.store_country AND
       t.is_retail_vip = s.is_retail_vip AND
       t.period_month_date = s.period_month_date AND
       t.max_day = s.max_day AND
       t.tenure = s.tenure
WHEN NOT MATCHED THEN
    INSERT (
        store_brand,
        store_region,
        store_country,
        is_retail_vip,
        period_month_date,
        max_day,
        tenure,
        login_unique,
        skip_unique,
        snooze_unique,
        skip_no_login,
        purchase_prod_order_unique,
        skip_or_purchase_unique,
        bop_vips,
        cancels_unique,
        eligible_to_be_billed,
        product_gross_revenue,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime)
    VALUES (
            store_brand,
            store_region,
            store_country,
            is_retail_vip,
            period_month_date,
            max_day,
            tenure,
            login_unique,
            skip_unique,
            snooze_unique,
            skip_no_login,
            purchase_prod_order_unique,
            skip_or_purchase_unique,
            bop_vips,
            cancels_unique,
            eligible_to_be_billed,
            product_gross_revenue,
            meta_row_hash,
            $execution_start_time,
            $execution_start_time)
WHEN MATCHED AND NOT equal_null(t.meta_row_hash, s.meta_row_hash)
THEN
    UPDATE
        SET t.login_unique =  s.login_unique,
            t.skip_unique =  s.skip_unique,
            t.snooze_unique =  s.snooze_unique,
            t.skip_no_login =  s.skip_no_login,
            t.purchase_prod_order_unique =  s.purchase_prod_order_unique,
            t.skip_or_purchase_unique =  s.skip_or_purchase_unique,
            t.bop_vips =  s.bop_vips,
            t.cancels_unique =  s.cancels_unique,
            t.eligible_to_be_billed =  s.eligible_to_be_billed,
            t.product_gross_revenue = s.product_gross_revenue,
            t.meta_row_hash = s.meta_row_hash,
            t.meta_update_datetime = $execution_start_time;
