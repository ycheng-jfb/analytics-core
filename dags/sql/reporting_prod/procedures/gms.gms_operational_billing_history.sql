SET low_watermark_datetime = (select coalesce(max(meta_update_datetime),'2016-12-01':: TIMESTAMP_LTZ) from reporting_prod.gms.gms_operational_billing_history);
SET month_date = date_trunc('MONTH',$low_watermark_datetime);
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMPORARY TABLE _vw_dim_store AS
SELECT DISTINCT
    store_id,
	store_id AS reporting_store_id,
	store_brand_abbr,
	store_country,
    'F' AS gender,
    NULL AS retail_vip_ind
FROM edw_prod.data_model.dim_store
WHERE store_brand_abbr != 'FL'
UNION ALL
SELECT DISTINCT
    -26 as store_id,
	-26 as reporting_store_id,
	store_brand_abbr,
	store_country,
    NULL AS gender,
    NULL AS retail_vip_ind
FROM edw_prod.data_model.dim_store
WHERE store_id = 26
UNION ALL
SELECT DISTINCT
    store_id,
	(store_id * -100)-1 as reporting_store_id,
	store_brand_abbr,
	store_country,
    'M' AS gender,
    NULL AS retail_vip_ind
FROM edw_prod.data_model.dim_store
WHERE store_brand_abbr = 'FL'
UNION ALL
SELECT DISTINCT
    store_id,
	(store_id * -100)-2 as reporting_store_id,
	store_brand_abbr,
	store_country,
    'M' AS gender,
     1 AS retail_vip_ind
FROM edw_prod.data_model.dim_store
WHERE store_brand_abbr = 'FL'
UNION ALL
SELECT DISTINCT
    store_id,
	(store_id * -100)-3 as reporting_store_id,
	store_brand_abbr,
	store_country,
    NULL AS gender,
    NULL AS retail_vip_ind
FROM edw_prod.data_model.dim_store
WHERE store_brand_abbr = 'FL'
UNION ALL
SELECT DISTINCT
    store_id,
	(store_id * -100)-4 as reporting_store_id,
	store_brand_abbr,
	store_country,
    NULL AS gender,
    1 AS retail_vip_ind
FROM edw_prod.data_model.dim_store
WHERE store_brand_abbr = 'FL';

CREATE OR REPLACE TEMPORARY TABLE _reporting_store AS
SELECT
    store_id,
    reporting_store_id,
    store_brand_abbr,
    store_country,
    gender,
    retail_vip_ind
FROM _vw_dim_store
WHERE store_id != -26;

CREATE OR REPLACE TEMPORARY TABLE _tmp_month_range AS
SELECT DISTINCT month_date AS datetime_month
FROM edw_prod.data_model.dim_date
WHERE month_date BETWEEN $month_date AND LAST_DAY(ADD_MONTHS(GETDATE(),-1));

CREATE OR REPLACE TEMPORARY TABLE _credit_billings_on_retry AS
SELECT DISTINCT order_id AS order_id
FROM edw_prod.data_model.fact_order fo
JOIN _tmp_month_range CM ON date(payment_transaction_local_datetime) >= DATEADD(MONTH,-5,cm.datetime_month)
AND DATE(payment_transaction_local_datetime) <= LAST_DAY(cm.datetime_month)
WHERE fo.is_credit_billing_on_retry;

CREATE OR REPLACE TEMPORARY TABLE _orders_transaction_date AS
SELECT
    date(payment_transaction_local_datetime) as "date",
    CASE WHEN dosc.IS_BORDER_FREE_ORDER THEN -121 ELSE rs.reporting_store_id END as store_id,
         COUNT(CASE WHEN ds.store_group = 'ShoeDazzle' AND dosc.order_classification_l1 = 'Billing Order' AND fo.payment_transaction_local_amount = 9.95
         THEN NULL ELSE fo.order_id END) as orders,
    SUM(CASE WHEN c.order_id IS NULL AND (ds.store_group <> 'ShoeDazzle' OR dosc.order_classification_l1 <> 'Billing Order'
        OR fo.payment_transaction_local_amount <> 9.95) THEN 1 ELSE 0 END) as billed_first_attempt_count
FROM edw_prod.data_model.fact_order fo
    JOIN _tmp_month_range CM ON date(payment_transaction_local_datetime) >= DATEADD(MONTH,-5,cm.datetime_month)
    AND date(payment_transaction_local_datetime) <= LAST_DAY(cm.datetime_month)
    JOIN edw_prod.data_model.dim_customer dc ON fo.customer_id = dc.customer_id
    JOIN edw_prod.data_model.dim_order_status dos ON fo.order_status_key=dos.order_status_key
    JOIN edw_prod.data_model.dim_order_sales_channel dosc ON fo.order_sales_channel_key=dosc.order_sales_channel_key
    JOIN edw_prod.data_model.dim_store ds ON fo.store_id = ds.store_id
    JOIN _reporting_store rs ON ds.store_id = rs.store_id
    AND CASE WHEN ds.store_brand IN ('Fabletics') THEN  IFF(dc.gender = 'M','M','F') = COALESCE(rs.gender, 'F') ELSE 1 END
    AND IFF(ds.store_brand IN ('Fabletics','Savage X') AND ds.store_type = 'Retail',1,0) = COALESCE(rs.retail_vip_ind, 0)
    LEFT JOIN _credit_billings_on_retry c	ON fo.order_id = c.order_id
WHERE dosc.order_classification_l1 = 'Billing Order' AND
      dosc.order_classification_l2 != 'Gift Certificate' AND
      dos.order_status = 'Success' AND
      NOT(dosc.IS_PS_ORDER)
GROUP BY "date",
         CASE WHEN dosc.IS_BORDER_FREE_ORDER THEN -121 ELSE rs.reporting_store_id END;

CREATE OR REPLACE TEMPORARY TABLE _tmp_bop_vips AS
SELECT
    a.period_month_date,
    a.store_name,
    a.bop_vips,
    b.trailing_6_month_bop_vips
       FROM (
                (SELECT
                DATEADD(MONTH,1,DATEADD(MONTH, DATEDIFF(MONTH, '1900-01-01',cm.datetime_month), '1900-01-01')) AS period_month_date,
                CASE WHEN s.store_brand_abbr || ' ' || s.store_country='SX EUREM' THEN 'SX EU'
                     ELSE s.store_brand_abbr || ' ' || s.store_country END AS store_name,
                COUNT(customer_id) AS bop_vips
                FROM edw_prod.data_model.fact_membership_event fme
                join (SELECT DISTINCT month_date FROM edw_prod.data_model.dim_date) dd_months
                ON dd_months.month_date > DATE(fme.EVENT_START_LOCAL_DATETIME)
                AND  dd_months.month_date <= DATE(fme.EVENT_END_LOCAL_DATETIME)
                AND fme.MEMBERSHIP_STATE = 'VIP'
                JOIN edw_prod.data_model.dim_store s ON s.store_id = fme.store_id
                JOIN _tmp_month_range CM ON
                dd_months.month_date >= cm.datetime_month AND dd_months.month_date <= LAST_DAY(cm.datetime_month)
                GROUP BY cm.datetime_month,s.store_brand_abbr || ' ' || s.store_country) a
                JOIN
                (SELECT
                DATEADD(MONTH,1,DATEADD(MONTH, DATEDIFF(MONTH, '1900-01-01',cm.datetime_month), '1900-01-01')) as period_month_date,
                CASE WHEN s.store_brand_abbr || ' ' || s.store_country='SX EUREM' THEN 'SX EU'
                     ELSE s.store_brand_abbr || ' ' || s.store_country END as store_name,
                COUNT(customer_id)/6 as trailing_6_month_bop_vips
                FROM edw_prod.data_model.fact_membership_event fme
                JOIN (SELECT DISTINCT month_date FROM edw_prod.data_model.dim_date) dd_months
                ON dd_months.month_date > date(fme.EVENT_START_LOCAL_DATETIME) AND  dd_months.month_date <= date(fme.EVENT_END_LOCAL_DATETIME)
                AND fme.MEMBERSHIP_STATE = 'VIP'
                JOIN edw_prod.data_model.dim_store s ON s.store_id = fme.store_id
                JOIN _tmp_month_range CM ON
                dd_months.month_date >= DATEADD(MONTH,-5,cm.datetime_month) AND dd_months.month_date <= cm.datetime_month
                GROUP BY cm.datetime_month,s.store_brand_abbr || ' ' || s.store_country) b
                ON  a.store_name = b.store_name AND a.period_month_date=b.period_month_date);

CREATE OR REPLACE TEMPORARY TABLE _tmp_billed_first_attempt AS
SELECT
    DATEADD(MONTH,1,DATEADD(MONTH, DATEDIFF(MONTH, '1900-01-01',cm.datetime_month), '1900-01-01')) as period_month_date,
    CASE WHEN ds.store_brand_abbr || ' ' || ds.store_country='SX EUREM' THEN 'SX EU' ELSE ds.store_brand_abbr || ' ' || ds.store_country END as store_name,
    IFNULL(SUM(CASE WHEN ot."date" BETWEEN cm.datetime_month AND LAST_DAY(cm.datetime_month) THEN ot.billed_first_attempt_count  ELSE 0 END),0) as billed_first_attempt_count,
    IFNULL(SUM(ot.billed_first_attempt_count),0) as billed_first_attempt_count_trailing_6_month,
    IFNULL(SUM(CASE WHEN ot."date" BETWEEN cm.datetime_month AND LAST_DAY(cm.datetime_month) THEN orders ELSE 0 END),0) as success_credit_billing,
    IFNULL(SUM(orders),0) as success_credit_billing_count_trailing_6_month
FROM _orders_transaction_date  ot
JOIN _vw_dim_store ds ON ds.reporting_store_id = ot.store_id
JOIN _tmp_month_range CM ON ot."date">= DATEADD(MONTH,-5,cm.datetime_month)
    AND ot."date" <= LAST_DAY(cm.datetime_month)
GROUP BY cm.datetime_month,
        (ds.store_brand_abbr||' '||ds.store_country);

CREATE OR REPLACE TEMPORARY TABLE _tmp_lead_to_vip_cohorted AS
SELECT
   DATEADD(MONTH,1,DATE_TRUNC('month',fme.EVENT_START_LOCAL_DATETIME)::DATE) AS month,
    CASE WHEN ds.store_brand_abbr || ' ' || ds.store_country='SX EUREM'
        THEN 'SX EU' else ds.store_brand_abbr || ' ' || ds.store_country END AS store_name,
    SUM(IFF((fme.EVENT_START_LOCAL_DATETIME BETWEEN tmr.datetime_month AND DATEADD(D,-8,LAST_DAY(tmr.datetime_month)))
    AND DATEDIFF(MONTH,dc.REGISTRATION_LOCAL_DATETIME,fme.EVENT_START_LOCAL_DATETIME) = 0 ,
    1,0)) as m1_new_vips,
    NULL AS first_successful_new_vips,
    SUM(IFF((fme.EVENT_START_LOCAL_DATETIME BETWEEN DATEADD(D,-7,LAST_DAY(tmr.datetime_month)) AND LAST_DAY(tmr.datetime_month))
    AND DATEDIFF(MONTH,dc.REGISTRATION_LOCAL_DATETIME,fme.EVENT_START_LOCAL_DATETIME) = 0,
    1,0)) as m1_grace_vips,
    NULL AS first_successful_grace_vips,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM edw_prod.data_model.fact_membership_event fme
JOIN _tmp_month_range tmr on date_trunc('month',fme.EVENT_START_LOCAL_DATETIME)::date = tmr.datetime_month
JOIN edw_prod.data_model.dim_customer dc on dc.customer_id = fme.customer_id
JOIN edw_prod.data_model.dim_store ds on ds.store_id = fme.store_id
WHERE fme.MEMBERSHIP_EVENT_TYPE = 'Activation'
GROUP BY
    DATEADD(MONTH,1,DATE_TRUNC('month',fme.EVENT_START_LOCAL_DATETIME)::DATE),
    ds.STORE_BRAND_ABBR||' '||ds.STORE_COUNTRY;

CREATE OR REPLACE TEMPORARY TABLE _tmp_gms_operational_billing_history AS
SELECT
    CAST(ltvc.month as date) as month,
    ltvc.store_name as store_name,
    ltvc.m1_new_vips as m1_new_vips,
    ltvc.m1_new_vips * CASE WHEN tbv.bop_vips > 0
                        THEN CAST(tfa.billed_first_attempt_count as FLOAT)/CAST(tbv.bop_vips AS FLOAT)
                        ELSE 0
                        END as first_successful_new_vips,
    ltvc.m1_grace_vips AS m1_grace_vips,
    ltvc.m1_grace_vips * CASE WHEN tbv.bop_vips > 0
                          THEN CAST(tfa.billed_first_attempt_count as FLOAT)/CAST(tbv.bop_vips AS FLOAT)
                          ELSE 0
                          END as first_successful_grace_vips,
    CASE WHEN tbv.bop_vips>0
     THEN CAST(tfa.billed_first_attempt_count as FLOAT)/CAST(tbv.bop_vips AS FLOAT)
     ELSE 0
	 END as billed_credit,
    CASE WHEN tbv.bop_vips>0
     THEN CAST(tfa.billed_first_attempt_count_trailing_6_month as FLOAT)/CAST(tbv.trailing_6_month_bop_vips AS FLOAT)
     ELSE 0
     END as first_successful_billed_credit_trailing_6_month,
    CASE WHEN tfa.success_credit_billing>0
     THEN (CAST(tfa.success_credit_billing as FLOAT) - CAST(tfa.billed_first_attempt_count as FLOAT))/CAST(tfa.success_credit_billing AS FLOAT)
     ELSE 0
    END as retry_rate,
    CASE WHEN tfa.success_credit_billing_count_trailing_6_month>0
     THEN (CAST(tfa.success_credit_billing_count_trailing_6_month as FLOAT) - CAST(tfa.billed_first_attempt_count_trailing_6_month as FLOAT))/CAST(tfa.success_credit_billing_count_trailing_6_month AS FLOAT)
     ELSE 0
    END as retry_rate_trailing_6_month,
    meta_create_datetime,
    meta_update_datetime
FROM _tmp_lead_to_vip_cohorted ltvc
    LEFT JOIN _tmp_bop_vips tbv ON tbv.period_month_date=ltvc.month
    AND tbv.store_name=ltvc.store_name
    LEFT JOIN _tmp_billed_first_attempt tfa ON tfa.period_month_date=ltvc.month
    AND	tfa.store_name=ltvc.store_name;

DELETE FROM reporting_prod.gms.gms_operational_billing_history
WHERE month IN (SELECT DATEADD(MONTH,1,DATETIME_MONTH) FROM _tmp_month_range);

INSERT INTO reporting_prod.gms.gms_operational_billing_history
(
    month,
    store_name,
    m1_new_vips,
    first_successful_new_vips,
    m1_grace_vips,
    first_successful_grace_vips,
    billed_credit,
    first_successful_billed_credit_rate_trailing_6_months,
    retry_rate,
    retry_rate_trailing_6_months,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    month AS month,
    store_name AS store_name,
    COALESCE(m1_new_vips,0) AS m1_new_vips,
    COALESCE(m1_new_vips*(first_successful_billed_credit_trailing_6_month/6),0) AS first_successful_new_vips,
    COALESCE(m1_grace_vips,0) AS m1_grace_vips,
    COALESCE(first_successful_grace_vips,0) AS first_successful_grace_vips,
    COALESCE(billed_credit,0) AS billed_credit,
    COALESCE(first_successful_billed_credit_trailing_6_month/6,0) AS first_successful_billed_credit_rate_trailing_6_months,
    COALESCE(retry_rate,0) AS retry_rate,
    COALESCE(retry_rate_trailing_6_month,0) AS retry_rate_trailing_6_months,
    meta_create_datetime,
    meta_update_datetime
FROM _tmp_gms_operational_billing_history;
