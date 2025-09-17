set low_watermark_ltz = (
    select
        max(a.MONTH_DATE)
    from REPORTING_PROD.GFB.gfb_customer_dataset_base a
    );


CREATE OR REPLACE TEMP TABLE _last_order_base AS
SELECT
    cd.customer_id
    ,cd.activation_local_datetime
    ,cd.cancel_local_datetime
    ,cd.month_date
FROM REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_BASE cd
where
    cd.MONTH_DATE >= $low_watermark_ltz
    and cd.MONTH_DATE < date_trunc('MONTH', current_date());



CREATE OR REPLACE TEMP TABLE _order_details_base AS
SELECT
   vip.customer_id,
   vip.month_date,
   vip.activation_local_datetime,
   vip.cancel_local_datetime,
   to_date(fo.order_local_datetime) AS order_date,
   datediff('days', fo.order_local_datetime, fo.shipped_local_datetime) AS shipping_time,
   fo.order_id AS order_id,
   fo.unit_count AS unit_sales,
   IFNULL(fo.product_subtotal_local_amount, 0) * IFNULL(fo.order_date_usd_conversion_rate, 1) AS subtotal,
   IFNULL(fo.product_discount_local_amount, 0) * IFNULL(fo.order_date_usd_conversion_rate, 1) AS discount,
   IFNULL(fo.cash_credit_local_amount, 0) AS cashcredit,
   RANK() OVER (PARTITION BY vip.customer_id, vip.month_date, vip.activation_local_datetime, vip.cancel_local_datetime ORDER BY fo.order_local_datetime DESC) AS rank,
   CASE WHEN rank = 1 THEN 1 ELSE 0 END AS last_order_flag
FROM edw_prod.data_model_jfb.fact_order fo
JOIN _last_order_base  vip
    ON vip.customer_id = fo.customer_id
    AND TO_DATE(fo.order_local_datetime) >= TO_DATE(vip.activation_local_datetime)
    AND TO_DATE(fo.order_local_datetime) <= COALESCE(TO_DATE(vip.cancel_local_datetime), current_date())
    AND DATE_TRUNC('MONTH', fo.order_local_datetime::DATE) = vip.month_date
JOIN edw_prod.data_model_jfb.dim_store ds
    ON ds.store_id = fo.store_id
JOIN edw_prod.data_model_jfb.dim_order_sales_channel doc
     ON fo.order_sales_channel_key = doc.order_sales_channel_key
JOIN edw_prod.data_model_jfb.dim_order_status dos
    ON fo.order_status_key = dos.order_status_key
JOIN edw_prod.data_model_jfb.dim_order_processing_status ops
        ON ops.order_processing_status_key = fo.order_processing_status_key
WHERE
    doc.order_classification_l1 IN ('Product Order')
    AND (doc.is_border_free_order = FALSE OR doc.is_ps_order = FALSE)
    AND ops.order_processing_status <> 'Cancelled (Incomplete Auth Redirect)'
    AND (
          dos.order_status = 'Success'
           OR ( dos.order_status = 'Pending'
                AND ops.order_processing_status IN ('FulFillment (Batching)', 'FulFillment (In Progress)', 'Placed')
              )
       );


create or replace temporary table _credit_redemption_count as
select
    oc.ORDER_ID
    ,count(distinct oc.STORE_CREDIT_ID) as credit_redemption_count
from LAKE_JFB_VIEW.ULTRA_MERCHANT.ORDER_CREDIT oc
join _order_details_base odb
    on odb.order_id = oc.ORDER_ID
    and odb.last_order_flag = 1
group by
    oc.ORDER_ID;


CREATE OR REPLACE TEMP TABLE _last_order AS
SELECT
    a.customer_id as lo_customer_id,
    a.month_date as lo_month_date,
    a.activation_local_datetime as lo_activation_local_datetime,
    a.cancel_local_datetime as lo_cancel_local_datetime,
    a.order_date AS last_order_date,
    COUNT(a.order_id) AS last_order_count,
    SUM(a.unit_sales) AS last_order_units,
    SUM(a.subtotal - a.discount) AS last_order_revenue,
    SUM(ROUND(crc.credit_redemption_count)) AS last_order_credit_redemptions,
    MAX(a.shipping_time) AS last_order_shipping_time
FROM _order_details_base a
left join _credit_redemption_count crc
    on crc.ORDER_ID = a.order_id
WHERE last_order_flag = 1
GROUP BY
    a.customer_id,
    a.month_date,
    a.activation_local_datetime,
    a.cancel_local_datetime,
    a.order_date;


CREATE OR REPLACE TEMP TABLE _last_credit_billing_order AS
SELECT
    vip.customer_id as cb_customer_id,
    vip.month_date as cb_month_date,
    vip.activation_local_datetime as cb_activation_local_datetime,
    vip.cancel_local_datetime as cb_cancel_local_datetime,
    to_date(max(fo.order_local_datetime)) AS last_credit_billing_date
FROM edw_prod.data_model_jfb.fact_order fo
JOIN edw_prod.data_model_jfb.dim_customer dc
    ON fo.customer_ID = dc.customer_ID
JOIN _last_order_base  vip
    ON vip.customer_id = fo.customer_id
    AND TO_DATE(fo.order_local_datetime) >= TO_DATE(vip.activation_local_datetime)
    AND TO_DATE(fo.order_local_datetime) <= COALESCE(TO_DATE(vip.cancel_local_datetime), current_date())
    AND DATE_TRUNC('MONTH', fo.order_local_datetime::DATE) = vip.month_date
JOIN edw_prod.data_model_jfb.dim_order_sales_channel doc
    ON fo.order_sales_channel_key = doc.order_sales_channel_key
WHERE doc.order_classification_l2 ILIKE 'Credit Billing'
GROUP BY
    vip.customer_id,
    vip.month_date,
    vip.activation_local_datetime,
    vip.cancel_local_datetime;


delete from REPORTING_PROD.GFB.gfb_customer_dataset_last_order a
where
    a.MONTH_DATE >= $low_watermark_ltz;


insert into REPORTING_PROD.GFB.gfb_customer_dataset_last_order
SELECT
    nvl(l.lo_customer_id, c.cb_customer_id) as customer_id,
    nvl(l.lo_month_date, c.cb_month_date) as month_date,
    nvl(l.lo_activation_local_datetime, c.cb_activation_local_datetime) as activation_local_datetime,
    nvl(l.lo_cancel_local_datetime, c.cb_cancel_local_datetime) as cancel_local_datetime,
    l.last_order_date,
    l.last_order_count,
    l.last_order_units,
    l.last_order_revenue,
    l.last_order_credit_redemptions,
    l.last_order_shipping_time,
    c.last_credit_billing_date
FROM _last_order l
full join _last_credit_billing_order c
    on l.lo_customer_id = c.cb_customer_id
    and l.lo_month_date = c.cb_month_date
    and l.lo_activation_local_datetime = c.cb_activation_local_datetime;
