SET target_table = 'reporting_prod.gms.mismatch_address_monitoring_dataset';
SET current_datetime = current_timestamp();

MERGE INTO public.meta_table_dependency_watermark AS t
    USING (
        SELECT
            'reporting_prod.gms.mismatch_address_monitoring_dataset' AS table_name,
            NULLIF(dependent_table_name, 'reporting_prod.gms.mismatch_address_monitoring_dataset') AS dependent_table_name,
            high_watermark_datetime AS new_high_watermark_datetime
        FROM (
            SELECT
                'EDW_PROD.DATA_MODEL.FACT_ORDER' AS dependent_table_name,
                max(ORDER_LOCAL_DATETIME)::timestamp_ltz(3) AS high_watermark_datetime
                FROM EDW_PROD.DATA_MODEL.FACT_ORDER
            UNION
            SELECT
                'EDW_PROD.DATA_MODEL.FACT_ACTIVATION' AS dependent_table_name,
                max(activation_local_datetime)::timestamp_ltz(3) AS high_watermark_datetime
                FROM EDW_PROD.DATA_MODEL.FACT_ACTIVATION
            UNION
            SELECT
                'LAKE_CONSOLIDATED_VIEW.ultra_merchant.case_customer' AS dependent_table_name,
                max(meta_update_datetime)::timestamp_ltz(3) AS high_watermark_datetime
                FROM LAKE_CONSOLIDATED_VIEW.ultra_merchant.case_customer
            UNION
            SELECT
                'LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_LOG' AS dependent_table_name,
                max(meta_update_datetime)::timestamp_ltz(3) AS high_watermark_datetime
                FROM LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_LOG
            ) AS h
        ) AS s
    ON t.table_name = s.table_name
        AND equal_null(t.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED
        AND NOT equal_null(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
    THEN UPDATE SET
        t.new_high_watermark_datetime = s.new_high_watermark_datetime,
        t.meta_update_datetime = current_timestamp::timestamp_ltz(3)
    WHEN NOT MATCHED THEN
    INSERT (
        table_name,
        dependent_table_name,
        high_watermark_datetime,
        new_high_watermark_datetime
    )
    VALUES (
        s.table_name,
        s.dependent_table_name,
        '2023-06-01'::timestamp_ltz, -- Replaced from 1900-01-01 since data is only being processed since 2023-06-01
        s.new_high_watermark_datetime
        )
;

SET wm_edw_prod_data_model_fact_order = public.udf_get_watermark($target_table,'EDW_PROD.DATA_MODEL.FACT_ORDER');
SET wm_edw_prod_data_model_fact_activation = public.udf_get_watermark($target_table,'EDW_PROD.DATA_MODEL.FACT_ACTIVATION');
SET wm_lake_consolidated_view_ultra_merchant_case_customer = public.udf_get_watermark($target_table,'LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CASE_CUSTOMER');
SET wm_lake_consolidated_view_ultra_merchant_customer_log = public.udf_get_watermark($target_table,'LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_LOG');


CREATE OR REPLACE TEMPORARY TABLE _customer_base AS
SELECT customer_id
FROM edw_prod.data_model.fact_order
WHERE order_local_datetime >= $wm_edw_prod_data_model_fact_order

UNION

SELECT customer_id
FROM edw_prod.data_model.fact_activation
WHERE activation_local_datetime >= $wm_edw_prod_data_model_fact_activation

UNION

SELECT customer_id
FROM lake_consolidated_view.ultra_merchant.case_customer
WHERE meta_update_datetime >= $wm_lake_consolidated_view_ultra_merchant_case_customer

UNION
SELECT customer_id
FROM lake_consolidated_view.ultra_merchant.customer_log
WHERE meta_update_datetime >= $wm_lake_consolidated_view_ultra_merchant_customer_log;


CREATE OR REPLACE TEMPORARY TABLE _fact_order_temp AS
SELECT
    order_id,
    fo.customer_id,
    edw_prod.stg.udf_unconcat_brand(fo.order_id) AS meta_original_order_id,
    product_subtotal_local_amount,
    product_discount_local_amount,
    cash_credit_local_amount,
    non_cash_credit_local_amount,
    non_cash_credit_count,
    amount_to_pay,
    payment_transaction_local_amount,
    order_local_datetime,
    order_processing_status_key,
    billing_address_id,
    shipping_address_id,
    session_id,
    store_id
FROM edw_prod.data_model.fact_order fo
JOIN _customer_base cb ON fo.customer_id = cb.customer_id
WHERE order_local_datetime >= '2023-06-01';

CREATE OR REPLACE TEMPORARY TABLE _fact_activation_temp AS
SELECT
    fa.customer_id,
    cancellation_local_datetime,
    activation_local_datetime,
    order_id,
    is_reactivated_vip,
    is_current
FROM edw_prod.data_model.fact_activation fa
JOIN _customer_base cb ON fa.customer_id = cb.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _session_temp AS
SELECT session_id,
    ip
FROM lake_consolidated_view.ultra_merchant.session s
JOIN _customer_base cb ON cb.customer_id = s.customer_id;

create or replace TEMPORARY table  _mismatch_address_temp as (
select *, hash(*)  as meta_row_hash from
(
select
    c.CUSTOMER_ID as cid,
    c.meta_original_customer_id,
    fo.ORDER_ID as oid,
    fo.meta_original_order_id,
    case when fa.order_id is not null then 'yes' else 'no' end as activating_order,
    c.EMAIL,
    ml.label as membership,
    s.label as store,
    to_date(fo.ORDER_LOCAL_DATETIME) as ordered_date,
    a.FIRSTNAME as billing_first,
    a.LASTNAME as billing_last,
    a.ADDRESS1 as billing_address1,
    a.ADDRESS2 as billing_address2,
    a.city as billing_city,
    a.STATE as billing_state,
    a.ZIP as billing_zip,
    a1.FIRSTNAME as shipping_first,
    a1.LASTNAME as shipping_last,
    a1.ADDRESS1 as shipping_address1,
    a1.ADDRESS2 as shipping_address2,
    a1.city as shipping_city,
    a1.STATE as shipping_state,
    a1.ZIP as shipping_zip,
    p.PAYMENT_METHOD,
    fo.PRODUCT_SUBTOTAL_LOCAL_AMOUNT as order_subtotal,
    fo.PRODUCT_DISCOUNT_LOCAL_AMOUNT as discount_amount,
    fo.CASH_CREDIT_LOCAL_AMOUNT + fo.NON_CASH_CREDIT_LOCAL_AMOUNT as store_credit_used,
    fo.NON_CASH_CREDIT_COUNT as credit_used,
    fo.amount_to_pay,
    fo.payment_transaction_local_amount as total_paid,
    ip.IP,
    sc.ORDER_PROCESSING_STATUS as processing_status,
    os.TYPE as shipping_type,
    cl.name_change_last_date,
    cl.name_change_count,
    cl.password_change_count,
    cl.password_change_last_date,
    cl.email_change_count,
    cl.email_change_last_date,
    datediff(day, fo.ORDER_LOCAL_DATETIME::DATE, cl.email_change_last_date::DATE) as email_change_order_date_diff,
    datediff(day, fo.ORDER_LOCAL_DATETIME::DATE, cl.name_change_last_date::DATE) as name_change_order_date_diff,
    datediff(day, fo.ORDER_LOCAL_DATETIME::DATE, cl.password_change_last_date::DATE) as pw_change_order_date_diff,
    fa.activation_local_datetime,
    fa.cancellation_local_datetime,
    fa.prev_cancellation_local_datetime,
    fa.is_reactivated_vip,
    fa.is_current,
    case when prev_cancellation_local_datetime is null then null
        when ca.case_id is null then 'Online Cancel' else 'GMS Cancel' end as prev_cancel_type,
    datediff('day', to_date(fa.prev_cancellation_local_datetime), to_date(fa.activation_local_datetime)) as date_diff,
    case when date_diff between 0 and 30 then '0-30'
        when date_diff between 31 and 60 then '31-60'
        when date_diff between 61 and 90 then '61-90'
        when date_diff between 91 and 120 then '90-120'
        when date_diff > 120 then '120+'
        else null
    end as days_window,
    case when prev_cancel_type = 'Online Cancel' and is_reactivated_vip = 'True'
        and date_diff between 0 and 30 then 'Yes' else 'No' end as reactivated_mismatch_order_prev_online_cancel_within_30_days
from _fact_order_temp fo
left join (
    select
        *,
        lag(cancellation_local_datetime) over (partition by customer_id order by activation_local_datetime asc) as prev_cancellation_local_datetime
    from _fact_activation_temp
) fa on fa.order_id=fo.order_id
left join (
    select
        cc.customer_id,
        cc.case_id,
        c.datetime_added,
        c.closed_administrator_id
    from lake_consolidated_view.ultra_merchant.case_customer cc
    join _customer_base cb ON cb.customer_id = cc.customer_id
    left join lake_consolidated_view.ultra_merchant.case c on c.case_id=cc.case_id
    where c.datetime_closed is not null -- filter out ai anna cases
) ca on ca.customer_id=fo.customer_id and to_date(ca.datetime_added)=to_date(fa.prev_cancellation_local_datetime)
left join lake_consolidated_view.ultra_merchant.customer c on fo.CUSTOMER_ID=c.CUSTOMER_ID
left join lake_consolidated_view.ultra_merchant.membership m on c.CUSTOMER_ID=m.CUSTOMER_ID
left join lake_consolidated_view.ultra_merchant.membership_level ml on m.MEMBERSHIP_LEVEL_ID=ml.MEMBERSHIP_LEVEL_ID
left join edw_prod.data_model.dim_order_processing_status sc on fo.order_processing_status_key = sc.ORDER_PROCESSING_STATUS_KEY
left join lake_consolidated_view.ultra_merchant.payment p on fo.ORDER_ID=p.ORDER_ID
left join lake_consolidated_view.ultra_merchant.address a on fo.BILLING_ADDRESS_ID=a.ADDRESS_ID
left join lake_consolidated_view.ultra_merchant.address a1 on fo.SHIPPING_ADDRESS_ID=a1.ADDRESS_ID
left join lake_consolidated_view.ultra_merchant.store s on s.STORE_ID=fo.STORE_ID
left join _session_temp ip on fo.session_id=ip.session_id
left join lake_consolidated_view.ultra_merchant.order_shipping os on fo.ORDER_ID=os.ORDER_ID
left join (
    select
        cl.customer_id,
        max(case when lower(comment) like '%name changed%' then DATETIME_ADDED end) as name_change_last_date,
        sum(case when lower(comment) like '%name changed%' then 1 else 0 end) as name_change_count,
        sum(case when lower(comment) like 'password%' then 1 else 0 end) as password_change_count,
        max(case when lower(comment) like 'password%' then DATETIME_ADDED end) as password_change_last_date,
        sum(case when lower(comment) like '%email change%' then 1 else 0 end) as email_change_count,
        max(case when lower(comment) like '%email change%' then DATETIME_ADDED end) as email_change_last_date
    from lake_consolidated_view.ultra_merchant.customer_log cl
    join _customer_base cb ON cl.customer_id = cb.customer_id
    where lower(comment) like 'firstname changed%' or
          lower(comment) like 'lastname changed%' or
          lower(comment) like 'password%' or
          lower(comment) like '%email change%'
    group by cl.customer_id
    ) cl on cl.CUSTOMER_ID=c.CUSTOMER_ID
where --fo.ORDER_LOCAL_DATETIME::DATE >= $low_watermark and
     fo.BILLING_ADDRESS_ID != fo.SHIPPING_ADDRESS_ID
    and lower(os.TYPE) like '%rush%'
    and processing_status <> 'Hold (Test Order)'
    and c.EMAIL not like '%@savagex.com%'
    and c.EMAIL not like '%@fabletic%'
    and c.EMAIL not like '%@techstyle%'
    and (datediff(day, fo.ORDER_LOCAL_DATETIME::DATE, cl.email_change_last_date) between 0 and 10 or
         datediff(day, fo.ORDER_LOCAL_DATETIME::DATE, cl.name_change_last_date) between 0 and 10 or
         datediff(day, fo.ORDER_LOCAL_DATETIME::DATE, cl.password_change_last_date) between 0 and 10)
)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY oid, cid ORDER BY NULL) = 1
)
;

MERGE INTO reporting_prod.gms.mismatch_address_monitoring_dataset as tma
    using _mismatch_address_temp sma on tma.CID = sma.CID
    and tma.OID = sma.OID
WHEN MATCHED
    AND tma.meta_row_hash != sma.meta_row_hash then
UPDATE
    SET tma.CID = sma.CID,
    tma.META_ORIGINAL_CUSTOMER_ID = sma.META_ORIGINAL_CUSTOMER_ID,
    tma.OID = sma.OID,
    tma.META_ORIGINAL_ORDER_ID = sma.META_ORIGINAL_ORDER_ID,
    tma.ACTIVATING_ORDER = sma.ACTIVATING_ORDER,
    tma.EMAIL = sma.EMAIL,
    tma.MEMBERSHIP = sma.MEMBERSHIP,
    tma.STORE = sma.STORE,
    tma.ORDERED_DATE = sma.ORDERED_DATE,
    tma.BILLING_FIRST = sma.BILLING_FIRST,
    tma.BILLING_LAST = sma.BILLING_LAST,
    tma.BILLING_ADDRESS1 = sma.BILLING_ADDRESS1,
    tma.BILLING_ADDRESS2 = sma.BILLING_ADDRESS2,
    tma.BILLING_CITY = sma.BILLING_CITY,
    tma.BILLING_STATE = sma.BILLING_STATE,
    tma.BILLING_ZIP = sma.BILLING_ZIP,
    tma.SHIPPING_FIRST = sma.SHIPPING_FIRST,
    tma.SHIPPING_LAST = sma.SHIPPING_LAST,
    tma.SHIPPING_ADDRESS1 = sma.SHIPPING_ADDRESS1,
    tma.SHIPPING_ADDRESS2 = sma.SHIPPING_ADDRESS2,
    tma.SHIPPING_CITY = sma.SHIPPING_CITY,
    tma.SHIPPING_STATE = sma.SHIPPING_STATE,
    tma.SHIPPING_ZIP = sma.SHIPPING_ZIP,
    tma.PAYMENT_METHOD = sma.PAYMENT_METHOD,
    tma.ORDER_SUBTOTAL = sma.ORDER_SUBTOTAL,
    tma.DISCOUNT_AMOUNT = sma.DISCOUNT_AMOUNT,
    tma.STORE_CREDIT_USED = sma.STORE_CREDIT_USED,
    tma.CREDIT_USED = sma.CREDIT_USED,
    tma.AMOUNT_TO_PAY = sma.AMOUNT_TO_PAY,
    tma.TOTAL_PAID = sma.TOTAL_PAID,
    tma.IP = sma.IP,
    tma.PROCESSING_STATUS = sma.PROCESSING_STATUS,
    tma.SHIPPING_TYPE = sma.SHIPPING_TYPE,
    tma.NAME_CHANGE_LAST_DATE = sma.NAME_CHANGE_LAST_DATE,
    tma.NAME_CHANGE_COUNT = sma.NAME_CHANGE_COUNT,
    tma.PASSWORD_CHANGE_COUNT = sma.PASSWORD_CHANGE_COUNT,
    tma.PASSWORD_CHANGE_LAST_DATE = sma.PASSWORD_CHANGE_LAST_DATE,
    tma.EMAIL_CHANGE_COUNT = sma.EMAIL_CHANGE_COUNT,
    tma.EMAIL_CHANGE_LAST_DATE = sma.EMAIL_CHANGE_LAST_DATE,
    tma.EMAIL_CHANGE_ORDER_DATE_DIFF = sma.EMAIL_CHANGE_ORDER_DATE_DIFF,
    tma.NAME_CHANGE_ORDER_DATE_DIFF = sma.NAME_CHANGE_ORDER_DATE_DIFF,
    tma.PW_CHANGE_ORDER_DATE_DIFF = sma.PW_CHANGE_ORDER_DATE_DIFF,
    tma.ACTIVATION_LOCAL_DATETIME = sma.ACTIVATION_LOCAL_DATETIME,
    tma.CANCELLATION_LOCAL_DATETIME = sma.CANCELLATION_LOCAL_DATETIME,
    tma.PREV_CANCELLATION_LOCAL_DATETIME = sma.PREV_CANCELLATION_LOCAL_DATETIME,
    tma.IS_REACTIVATED_VIP = sma.IS_REACTIVATED_VIP,
    tma.IS_CURRENT = sma.IS_CURRENT,
    tma.PREV_CANCEL_TYPE = sma.PREV_CANCEL_TYPE,
    tma.DATE_DIFF = sma.DATE_DIFF,
    tma.DAYS_WINDOW = sma.DAYS_WINDOW,
    tma.REACTIVATED_MISMATCH_ORDER_PREV_ONLINE_CANCEL_WITHIN_30_DAYS = sma.REACTIVATED_MISMATCH_ORDER_PREV_ONLINE_CANCEL_WITHIN_30_DAYS,
    tma.meta_row_hash = sma.meta_row_hash,
    tma.meta_update_datetime = $current_datetime
    WHEN NOT MATCHED THEN
INSERT
    (
    	CID,
    	META_ORIGINAL_CUSTOMER_ID,
    	OID,
    	META_ORIGINAL_ORDER_ID,
    	ACTIVATING_ORDER,
    	EMAIL,
    	MEMBERSHIP,
    	STORE,
    	ORDERED_DATE,
    	BILLING_FIRST,
    	BILLING_LAST,
    	BILLING_ADDRESS1,
    	BILLING_ADDRESS2,
    	BILLING_CITY,
    	BILLING_STATE,
    	BILLING_ZIP,
    	SHIPPING_FIRST,
    	SHIPPING_LAST,
    	SHIPPING_ADDRESS1,
    	SHIPPING_ADDRESS2,
    	SHIPPING_CITY,
    	SHIPPING_STATE,
    	SHIPPING_ZIP,
    	PAYMENT_METHOD,
    	ORDER_SUBTOTAL,
    	DISCOUNT_AMOUNT,
    	STORE_CREDIT_USED,
    	CREDIT_USED,
    	AMOUNT_TO_PAY,
    	TOTAL_PAID,
    	IP,
    	PROCESSING_STATUS,
    	SHIPPING_TYPE,
    	NAME_CHANGE_LAST_DATE,
    	NAME_CHANGE_COUNT,
    	PASSWORD_CHANGE_COUNT,
    	PASSWORD_CHANGE_LAST_DATE,
    	EMAIL_CHANGE_COUNT,
    	EMAIL_CHANGE_LAST_DATE,
    	EMAIL_CHANGE_ORDER_DATE_DIFF,
    	NAME_CHANGE_ORDER_DATE_DIFF,
    	PW_CHANGE_ORDER_DATE_DIFF,
    	ACTIVATION_LOCAL_DATETIME,
    	CANCELLATION_LOCAL_DATETIME,
    	PREV_CANCELLATION_LOCAL_DATETIME,
    	IS_REACTIVATED_VIP,
    	IS_CURRENT,
    	PREV_CANCEL_TYPE,
    	DATE_DIFF,
    	DAYS_WINDOW,
    	REACTIVATED_MISMATCH_ORDER_PREV_ONLINE_CANCEL_WITHIN_30_DAYS,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
    )
values
(
        sma.CID,
        sma.META_ORIGINAL_CUSTOMER_ID,
        sma.OID,
        sma.META_ORIGINAL_ORDER_ID,
        sma.ACTIVATING_ORDER,
        sma.EMAIL,
        sma.MEMBERSHIP,
        sma.STORE,
        sma.ORDERED_DATE,
        sma.BILLING_FIRST,
        sma.BILLING_LAST,
        sma.BILLING_ADDRESS1,
        sma.BILLING_ADDRESS2,
        sma.BILLING_CITY,
        sma.BILLING_STATE,
        sma.BILLING_ZIP,
        sma.SHIPPING_FIRST,
        sma.SHIPPING_LAST,
        sma.SHIPPING_ADDRESS1,
        sma.SHIPPING_ADDRESS2,
        sma.SHIPPING_CITY,
        sma.SHIPPING_STATE,
        sma.SHIPPING_ZIP,
        sma.PAYMENT_METHOD,
        sma.ORDER_SUBTOTAL,
        sma.DISCOUNT_AMOUNT,
        sma.STORE_CREDIT_USED,
        sma.CREDIT_USED,
        sma.AMOUNT_TO_PAY,
        sma.TOTAL_PAID,
        sma.IP,
        sma.PROCESSING_STATUS,
        sma.SHIPPING_TYPE,
        sma.NAME_CHANGE_LAST_DATE,
        sma.NAME_CHANGE_COUNT,
        sma.PASSWORD_CHANGE_COUNT,
        sma.PASSWORD_CHANGE_LAST_DATE,
        sma.EMAIL_CHANGE_COUNT,
        sma.EMAIL_CHANGE_LAST_DATE,
        sma.EMAIL_CHANGE_ORDER_DATE_DIFF,
        sma.NAME_CHANGE_ORDER_DATE_DIFF,
        sma.PW_CHANGE_ORDER_DATE_DIFF,
        sma.ACTIVATION_LOCAL_DATETIME,
        sma.CANCELLATION_LOCAL_DATETIME,
        sma.PREV_CANCELLATION_LOCAL_DATETIME,
        sma.IS_REACTIVATED_VIP,
        sma.IS_CURRENT,
        sma.PREV_CANCEL_TYPE,
        sma.DATE_DIFF,
        sma.DAYS_WINDOW,
        sma.REACTIVATED_MISMATCH_ORDER_PREV_ONLINE_CANCEL_WITHIN_30_DAYS,
        sma.meta_row_hash,
        $current_datetime,
        $current_datetime
    );

update reporting_prod.public.meta_table_dependency_watermark
set high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime = current_timestamp::timestamp_ltz(3)
where table_name=$target_table;
