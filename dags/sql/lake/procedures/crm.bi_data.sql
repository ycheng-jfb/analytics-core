SET max_month = (
    SELECT max(month_date)
    FROM edw_prod.analytics_base.customer_lifetime_value_monthly
    );
SET current_date = (SELECT CURRENT_DATE());

CREATE OR REPLACE TEMP TABLE _membership_status AS
SELECT
    a.customer_id,
    a.store_id,
    ds.store_group,
    ds.store_brand,
    ds.store_type,
    ds.store_group_id,
    a.membership_state,
    a.is_current,
    a.membership_event_type,
    a.event_start_local_datetime,
    a.event_end_local_datetime,
    a.session_id,
    a.recent_activation_local_datetime
FROM edw_prod.data_model.fact_membership_event AS a
JOIN edw_prod.data_model.dim_store AS ds
    ON a.store_id = ds.store_id
;

/* Track test and retail customers */
CREATE OR REPLACE TEMP TABLE _retail_records AS
SELECT DISTINCT dc.customer_id
FROM edw_prod.data_model.dim_customer AS dc
WHERE dc.email ILIKE ANY ('%@retail.fabletics.com', '%@test.com', '%@reacquired.local');

/* Track Hard Cancels */
CREATE OR REPLACE TEMP TABLE _hard_cancel_records AS
SELECT DISTINCT m.customer_id
FROM lake_consolidated.ultra_merchant.membership AS m
WHERE m.statuscode = 3940;

CREATE OR REPLACE TEMP TABLE _keep_records AS
SELECT DISTINCT customer_id
FROM (
    SELECT
        m.customer_id,
        m.store_id,
        ms.statuscode,
        ml.membership_level_group_id,
        c.datetime_added           as signup_date,
        max(cle.datetime_modified) as last_login,
        max(o.date_placed)            last_order
    FROM _membership_status AS m
    JOIN lake_consolidated.ultra_merchant.membership AS ms
        ON ms.customer_id = m.customer_id
    JOIN lake_consolidated.ultra_merchant.membership_level AS ml
        ON ms.membership_level_id = ml.membership_level_id
    JOIN lake_consolidated.ultra_merchant.customer AS c
        ON m.customer_id = c.customer_id
    JOIN lake_consolidated_view.ultra_merchant.customer_last_event AS cle
        ON m.customer_id = cle.CUSTOMER_ID
    LEFT JOIN lake_consolidated_view.ULTRA_MERCHANT."ORDER" AS o
        ON m.customer_id = o.CUSTOMER_ID
        AND o.processing_statuscode >= 2050
    GROUP BY m.customer_id,
        m.store_id,
        ms.statuscode,
        ml.membership_level_group_id,
        c.datetime_added
    ) AS A
WHERE statuscode <> 3925
    OR (
        statuscode = 3925
        AND membership_level_group_id NOT IN (100, 200, 300)
        )
    OR (
        statuscode = 3925
        AND membership_level_group_id = 100
        AND (
            signup_date > DATEADD(YEAR, -3, GETDATE())
            OR last_login > DATEADD(YEAR, -2, GETDATE())
            )
        ) -- 72819150
    OR (
        statuscode = 3925
        AND membership_level_group_id IN (200, 300)
        AND (
            signup_date > DATEADD(YEAR, -3, GETDATE())
            OR IFNULL(last_order, TO_DATE('1990-01-01')) > DATEADD(YEAR, -2, GETDATE())
            )
        )
;

/* For tracking Exclusion Reason in History */
CREATE OR REPLACE TEMP TABLE _exclusion_reason AS
SELECT DISTINCT
    A.customer_id,
    CASE WHEN B.customer_id IS NOT NULL
        THEN 'retail/test'
        WHEN C.customer_id IS NOT NULL
        THEN 'hard cancel'
        WHEN D.customer_id IS NULL
        THEN 'not in _keep_records'
        ELSE NULL
    END AS exclusion_reason
FROM _membership_status AS A
LEFT JOIN _retail_records AS B
    ON A.customer_id = B.customer_id
LEFT JOIN _hard_cancel_records AS C
    ON A.customer_id = C.customer_id
LEFT JOIN _keep_records AS D
    ON A.customer_id = D.customer_id;


DELETE FROM _membership_status AS m
WHERE m.customer_id NOT IN (
    SELECT DISTINCT customer_id
    FROM _keep_records
);


DELETE FROM _membership_status AS ms
USING (
    /* Delete test and retail customers */
    SELECT DISTINCT customer_id
    FROM _retail_records

    UNION
    /* Delete Hard Cancels */
    SELECT DISTINCT customer_id
    FROM _hard_cancel_records
    ) AS d
WHERE ms.customer_id = d.customer_id;


CREATE OR REPLACE TEMP TABLE _current_status AS
SELECT
    customer_id,
    store_id,
    store_group_id,
    store_group,
    store_brand,
    membership_state,
    membership_event_type,
    event_start_local_datetime
FROM _membership_status
WHERE is_current = 1;

CREATE OR REPLACE TEMP TABLE _vip_flag_recent_cohort AS
SELECT
    customer_id,
    event_start_local_datetime,
    store_id,
    session_id,
    store_type,
    recent_activation_local_datetime,
    TO_DATE(DATE_TRUNC('MONTH', DATEADD(MIN, -15, event_start_local_datetime))) as vip_cohort
from _membership_status
where membership_event_type in ('Failed Activation from Classic VIP', 'Activation')
QUALIFY ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY event_start_local_datetime DESC) = 1
;

CREATE OR REPLACE TEMP TABLE _guest_flag AS
SELECT
    customer_id,
    event_start_local_datetime,
    store_id,
    session_id,
    store_type,
    recent_activation_local_datetime,
    '1900-01-01' AS vip_cohort /*guest purchase, assign value of '1900-01-01' (vip cohort of guest purchases) */
FROM _membership_status
WHERE membership_event_type in ('Guest Purchasing Member', 'Failed Activation from Guest')
QUALIFY ROW_NUMBER() OVER(PARTITION BY customer_id, store_id ORDER BY event_start_local_datetime DESC) = 1
;

/* Combine VIP and Guest to pull a list of all purchasers */
CREATE OR REPLACE TEMP TABLE _vip_and_guest_recent_cohort AS
SELECT
    COALESCE(vip.customer_id, gf.customer_id) AS customer_id,
    COALESCE(vip.event_start_local_datetime, gf.event_start_local_datetime) AS event_start_local_datetime,
    COALESCE(vip.store_id, gf.store_id) AS store_id,
    COALESCE(vip.session_id, gf.session_id) AS session_id,
    COALESCE(vip.store_type, gf.store_type) AS store_type,
    vip.recent_activation_local_datetime,
    COALESCE(vip.vip_cohort, gf.vip_cohort) AS vip_cohort
FROM _vip_flag_recent_cohort AS vip
FULL JOIN _guest_flag AS gf
    ON vip.customer_id = gf.customer_id
;

CREATE OR REPLACE TEMP TABLE _always_lead AS
SELECT
    ms.customer_id,
    ms.event_start_local_datetime AS lead_registration_datetime
FROM _membership_status AS ms
JOIN _current_status AS cs
    ON ms.customer_id = cs.customer_id
    AND cs.membership_state = 'Lead' /* current as lead, could be always or from failed activation */
WHERE ms.membership_event_type ILIKE 'registration'
    AND cs.customer_id NOT IN (
        SELECT customer_id FROM _vip_flag_recent_cohort
    )
; /* never activate or have failed activation */

CREATE OR REPLACE TEMP TABLE _ecomm_flag AS
SELECT
    customer_id,
    event_start_local_datetime
FROM _membership_status
WHERE membership_state ILIKE 'guest';

CREATE OR REPLACE TEMPORARY TABLE _SKIP_REASON AS
WITH _store AS (
    SELECT *
    FROM EDW_PROD.DATA_MODEL.DIM_STORE AS st
    WHERE st.STORE_FULL_NAME NOT LIKE '%(DM)%'
        AND st.STORE_FULL_NAME NOT LIKE '%Wholesale%'
        AND st.STORE_FULL_NAME NOT LIKE '%Heels.com%'
        AND st.STORE_FULL_NAME NOT LIKE '%Retail%'
        AND st.STORE_FULL_NAME NOT LIKE '%Sample%'
        AND st.STORE_FULL_NAME NOT LIKE '%SWAG%'
        AND st.STORE_FULL_NAME NOT LIKE '%PS%'
    )
    ,_all_skips AS (
    SELECT
        st.STORE_BRAND || ' ' || st.STORE_REGION AS store,
        dc.CUSTOMER_ID,
        ms.MEMBERSHIP_SKIP_ID,
        ms.SESSION_ID,
        cast(ms.DATETIME_ADDED AS DATE) AS skip_date,
        p.DATE_PERIOD_START AS skip_month,
        msr.LABEL AS skip_reason,
        dayofmonth(skip_date) AS skip_day_of_month
    FROM lake_consolidated_view.ULTRA_MERCHANT.MEMBERSHIP_SKIP AS ms
    JOIN lake_consolidated_view.ULTRA_MERCHANT.MEMBERSHIP AS m
        ON m.MEMBERSHIP_ID = ms.MEMBERSHIP_ID
    JOIN lake_consolidated_view.ULTRA_MERCHANT.PERIOD AS p
        ON p.PERIOD_ID = ms.PERIOD_ID
    JOIN EDW_PROD.DATA_MODEL.DIM_CUSTOMER AS dc
        ON dc.CUSTOMER_ID = m.CUSTOMER_ID
        AND dc.IS_TEST_CUSTOMER = 0
    JOIN _store AS st
        ON st.STORE_ID = dc.STORE_ID
    LEFT JOIN lake_consolidated_view.ULTRA_MERCHANT.MEMBERSHIP_SKIP_REASON AS msr
        ON msr.MEMBERSHIP_SKIP_REASON_ID = ms.MEMBERSHIP_SKIP_REASON_ID
    WHERE p.DATE_PERIOD_START >= '1900-01-01'
    )

SELECT DISTINCT
    ms.CUSTOMER_ID,
	COALESCE(skip_reason, 'No Reason') AS skip_reason
FROM _membership_status AS ms
JOIN _all_skips AS sk
    ON ms.CUSTOMER_ID = sk.CUSTOMER_ID
	AND ms.SESSION_ID = sk.SESSION_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY ms.CUSTOMER_ID ORDER BY skip_date DESC) = 1;

CREATE OR REPLACE TEMP TABLE _membership_amount_saved_to_date AS
WITH _orders AS (
    SELECT DISTINCT
        dc.customer_id,
        o.order_id,
        IFF(oc.order_classification_l2 ILIKE 'product order', 1, 0) AS product_orders,
        IFF(LOWER(oc.order_classification_l2) IN (
                'token billing',
                'credit billing'
                ), 1, 0) AS credit_billings
    FROM _membership_status AS dc
    JOIN edw_prod.data_model.fact_order AS o
        ON o.customer_id = dc.customer_id
    JOIN edw_prod.data_model.dim_order_sales_channel AS oc
        ON oc.order_sales_channel_key = o.order_sales_channel_key
        AND LOWER(oc.order_classification_l2) IN (
            'token billing', 'credit billing', 'product order')
    ),
_order_line AS (
    SELECT
        o.order_id,
        SUM((COALESCE(retail_unit_price, 0) - COALESCE(purchase_unit_price, unit_cost, 0)) * quantity) AS saving
    FROM _orders AS o
    JOIN lake_consolidated_view.ultra_merchant.order_line AS ol
        ON ol.order_id = o.order_id
    WHERE ol.product_type_id <> 11
    GROUP BY o.order_id
    )

SELECT
    fo.customer_id,
	SUM(COALESCE(ol.saving, 0) + COALESCE(lo.discount, 0)) AS membership_amount_saved_to_date
FROM _orders AS fo
JOIN edw_prod.data_model.fact_order AS o
    ON o.order_id = fo.order_id
JOIN lake_consolidated_view.ultra_merchant."ORDER" AS lo
    ON lo.order_id = fo.order_id
JOIN edw_prod.data_model.dim_order_status AS os
    ON os.order_status_key = o.order_status_key
	AND os.order_status ILIKE 'success'
LEFT JOIN _order_line AS ol
    ON ol.order_id = fo.order_id
GROUP BY fo.customer_id;

create or replace temp table _EMP_OPTIN as
select customer_id,IFF(EMP_OPTIN_DATETIME is null,0,1) IS_EMP_OPTIN
from edw_prod.data_model.dim_customer c
join edw_prod.data_model.dim_store ds on c.store_id=ds.store_id
where store_brand_abbr in ('JF', 'SD', 'FK');

CREATE OR REPLACE TEMPORARY TABLE _membership_2995usd_credits AS
SELECT
    sc.customer_id,
	count(1) AS membership_2995usd_credits
FROM lake_consolidated.ultra_merchant.membership_store_credit AS msc
JOIN lake_consolidated.ultra_merchant.store_credit AS sc
    ON sc.store_credit_id = msc.store_credit_id
	AND sc.statuscode = 3240
LEFT JOIN edw_prod.data_model.dim_customer AS dc
    ON dc.customer_id = sc.customer_id
LEFT JOIN edw_prod.data_model.dim_store AS ds
    ON ds.store_id = dc.store_id
WHERE sc.amount = 29.95
	AND ds.store_brand_abbr = 'FK'
GROUP BY sc.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _membership_3995usd_credits AS
SELECT
    sc.customer_id,
	count(1) AS membership_3995usd_credits
FROM lake_consolidated.ultra_merchant.membership_store_credit AS msc
JOIN lake_consolidated.ultra_merchant.store_credit AS sc
    ON sc.store_credit_id = msc.store_credit_id
	AND sc.statuscode = 3240
LEFT JOIN edw_prod.data_model.dim_customer AS dc
    ON dc.customer_id = sc.customer_id
LEFT JOIN edw_prod.data_model.dim_store AS ds
    ON ds.store_id = dc.store_id
WHERE sc.amount = 39.95
	AND ds.store_brand_abbr = 'FK'
GROUP BY sc.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _pre_passive_cancel_2m_check AS
WITH _vips AS (
    SELECT concat(dv.CUSTOMER_ID,'10') as CUSTOMER_ID
    FROM REPORTING_PROD.GFB.GFB_DIM_VIP AS dv
    WHERE dv.CURRENT_MEMBERSHIP_STATUS = 'VIP'
    ),
_login_last_11_month AS (
    SELECT DISTINCT vc.CUSTOMER_ID
    FROM REPORTING_BASE_PROD.SHARED.SESSION AS s
    JOIN _vips AS vc
        ON vc.CUSTOMER_ID = s.CUSTOMER_ID
        AND s.SESSION_LOCAL_DATETIME BETWEEN DATEADD(MONTH, - 11, CURRENT_DATE ()) AND CURRENT_DATE ()
    ),
_purchase_billing_last_11_month AS (
    SELECT DISTINCT vc.CUSTOMER_ID
    FROM REPORTING_PROD.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE AS olp
    JOIN _vips AS vc
        ON vc.CUSTOMER_ID = olp.CUSTOMER_ID
        AND olp.ORDER_DATE BETWEEN dateadd(month, - 11, CURRENT_DATE ()) AND CURRENT_DATE ()
    WHERE olp.ORDER_CLASSIFICATION IN ('product order', 'credit billing')
        AND olp.ORDER_TYPE = 'vip repeat'
    ),
_skips_last_11_month AS (
    SELECT DISTINCT vc.CUSTOMER_ID
    FROM EDW_PROD.DATA_MODEL.FACT_CUSTOMER_ACTION AS fca
    JOIN EDW_PROD.DATA_MODEL.DIM_CUSTOMER_ACTION_TYPE AS cat
        ON cat.CUSTOMER_ACTION_TYPE_KEY = fca.CUSTOMER_ACTION_TYPE_KEY
        AND cat.CUSTOMER_ACTION_TYPE = 'Skipped Month'
    JOIN _vips AS vc
        ON vc.CUSTOMER_ID = fca.CUSTOMER_ID
        AND fca.CUSTOMER_ACTION_LOCAL_DATETIME BETWEEN dateadd(month, - 11, CURRENT_DATE()) AND CURRENT_DATE()
    )

SELECT
    v.customer_id,
	CASE WHEN v.customer_id IS NOT NULL THEN 1 ELSE 0 END AS pre_passive_cancel_2m_check
FROM _vips AS v
LEFT JOIN _login_last_11_month AS log
    ON log.CUSTOMER_ID = v.CUSTOMER_ID
LEFT JOIN _purchase_billing_last_11_month AS pb
    ON pb.CUSTOMER_ID = v.CUSTOMER_ID
LEFT JOIN _skips_last_11_month AS sk
    ON sk.CUSTOMER_ID = v.CUSTOMER_ID
WHERE log.CUSTOMER_ID IS NULL
	AND pb.CUSTOMER_ID IS NULL
	AND sk.CUSTOMER_ID IS NULL;

--SXF CRM BI Variables: Activating + Lifetime Purchase SubDepts, with and without gender
CREATE OR REPLACE TEMP TABLE _Activating_Lifetime_Purchase_SubDepts AS
WITH subdepartment_customsegments AS (
SELECT DISTINCT
    CUSTOMER_ID,
    IFF(custom_segment_category = 'Lifetime Sub Department', custom_segment, NULL)
        AS lifetime_sub_department,
    IFF(custom_segment_category = 'Activating Sub Department', custom_segment , NULL)
        AS activating_sub_department,
    IFF(custom_segment_category = 'Lifetime Sub Department without Gender', custom_segment , NULL)
        AS lifetime_sub_department_without_gender,
    IFF(custom_segment_category = 'Activating Sub Department without Gender', custom_segment , NULL)
        AS activating_sub_department_without_gender
FROM REPORTING_PROD.SXF.VIEW_CUSTOM_SEGMENT
WHERE custom_segment_category ILIKE '%sub department%'
ORDER BY CUSTOMER_ID
)

SELECT DISTINCT
    CONCAT(customer_id, '30') as customer_id,  -- concat with sxf company id
    LISTAGG(DISTINCT lifetime_sub_department, ' + ') WITHIN GROUP (ORDER BY lifetime_sub_department)
        AS SXF_Lifetime_Sub_Department,
    LISTAGG(DISTINCT activating_sub_department, ' + ') WITHIN GROUP (ORDER BY activating_sub_department)
        AS SXF_Activating_Sub_Department,
    LISTAGG(DISTINCT lifetime_sub_department_without_gender, ' + ') WITHIN GROUP (ORDER BY lifetime_sub_department_without_gender)
        AS SXF_Lifetime_Sub_Department_without_Gender,
    LISTAGG(DISTINCT activating_sub_department_without_gender, ' + ') WITHIN GROUP (ORDER BY activating_sub_department_without_gender)
        AS SXF_Activating_Sub_Department_without_Gender
FROM subdepartment_customsegments
GROUP BY customer_id;

-- Failed Billers: SXF failed billers, most recent full month
CREATE OR REPLACE TEMP TABLE _SX_failed_billers AS
WITH Failed_Billers AS (
SELECT DISTINCT
    customer_id,
    consecutive_failed_billings,
    months_since_last_successful_billing,
    months_since_last_skip,
    months_since_last_login,
    months_since_last_product_order,
    NUMBER_OF_FAILED_BILLINGS_IN_LAST_12_MONTHS,
    first_failed_billing
FROM REPORTING_PROD.SXF.view_failed_biller_activity_by_customer
WHERE month_date = DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1)
),

-- On track to passive cancel: criteria 12 months
PC12 as (
    SELECT DISTINCT customer_id
    FROM Failed_Billers
    WHERE consecutive_failed_billings >= 12
        AND (months_since_last_successful_billing >= 12 OR months_since_last_successful_billing IS NULL)
        AND (months_since_last_skip >= 12 OR months_since_last_skip IS NULL)
        AND months_since_last_login >= 12
        AND months_since_last_product_order >= 12
),

-- On track to passive cancel: criteria using 11 months
PC11 AS (
    SELECT DISTINCT customer_id
    FROM Failed_Billers
    WHERE consecutive_failed_billings >= 11
        AND (months_since_last_successful_billing >= 11 OR months_since_last_successful_billing IS NULL)
        AND (months_since_last_skip >= 11 OR months_since_last_skip IS NULL)
        AND months_since_last_login >= 11
        AND months_since_last_product_order >= 11
),

-- On track to passive cancel: criteria using 10 months
PC10 AS (
    SELECT DISTINCT customer_id
    FROM Failed_Billers
    WHERE consecutive_failed_billings >= 10
        AND (months_since_last_successful_billing >= 10 OR months_since_last_successful_billing IS NULL)
        AND (months_since_last_skip >= 10 OR months_since_last_skip IS NULL)
        AND months_since_last_login >= 10
        AND months_since_last_product_order >= 10
),

-- On track to passive cancel: criteria using 9 months
PC9 AS (
    SELECT DISTINCT customer_id
    FROM Failed_Billers
    WHERE consecutive_failed_billings >= 9
        AND (months_since_last_successful_billing >= 9 OR months_since_last_successful_billing IS NULL)
        AND (months_since_last_skip >= 9 OR months_since_last_skip IS NULL)
        AND months_since_last_login >= 9
        AND months_since_last_product_order >= 9
),

-- On track to passive cancel: criteria using 8 months
PC8 as (
    SELECT DISTINCT customer_id
    FROM Failed_Billers
    WHERE consecutive_failed_billings >= 8
        AND (months_since_last_successful_billing >= 8 OR months_since_last_successful_billing IS NULL)
        AND (months_since_last_skip >= 8 OR months_since_last_skip IS NULL)
        AND months_since_last_login >= 8
        AND months_since_last_product_order >= 8
),

-- On track to passive cancel: criteria using 7 months
PC7 AS (
    SELECT DISTINCT customer_id
    FROM Failed_Billers
    WHERE consecutive_failed_billings >= 7
        AND (months_since_last_successful_billing >= 7 OR months_since_last_successful_billing IS NULL)
        AND (months_since_last_skip >= 7 OR months_since_last_skip IS NULL)
        AND months_since_last_login >= 7
        AND months_since_last_product_order >= 7
),

-- On track to passive cancel: criteria using 6 months
PC6 AS (
    SELECT DISTINCT customer_id
    FROM Failed_Billers
    WHERE consecutive_failed_billings >= 6
        AND (months_since_last_successful_billing >= 6 OR months_since_last_successful_billing IS NULL)
        AND (months_since_last_skip >= 6 OR months_since_last_skip IS NULL)
        AND months_since_last_login >= 6
        AND months_since_last_product_order >= 6
),

-- On track to passive cancel: criteria using 5 months
PC5 AS (
    SELECT DISTINCT customer_id
    FROM Failed_Billers
    WHERE consecutive_failed_billings >= 5
        AND (months_since_last_successful_billing >= 5 OR months_since_last_successful_billing IS NULL)
        AND (months_since_last_skip >= 5 OR months_since_last_skip IS NULL)
        AND months_since_last_login >= 5
        AND months_since_last_product_order >= 5
)

-- Failed Billers BI Variables
SELECT DISTINCT
    fb.customer_id,
    consecutive_failed_billings AS SXF_Consecutive_Failed_Billing_Months,
    1 AS SXF_Is_Failed_Billing,
    NUMBER_OF_FAILED_BILLINGS_IN_LAST_12_MONTHS AS SXF_Total_Failed_Billing_Months_InPastYear,
    CASE WHEN first_failed_billing = 1 THEN 1 END AS SXF_Is_First_Time_Failed_Billing,
    CASE
        WHEN PC12.customer_id IS NOT NULL THEN 12
        WHEN PC11.customer_id IS NOT NULL THEN 11
        WHEN PC10.customer_id IS NOT NULL THEN 10
        WHEN PC9.customer_id IS NOT NULL THEN 9
        WHEN PC8.customer_id IS NOT NULL THEN 8
        WHEN PC7.customer_id IS NOT NULL THEN 7
        WHEN PC6.customer_id IS NOT NULL THEN 6
        WHEN PC5.customer_id IS NOT NULL THEN 5
    END AS SXF_On_Track_Passive_Cancel_Criteria_Months
FROM Failed_Billers fb
LEFT JOIN PC12 ON PC12.customer_id = fb.customer_id
LEFT JOIN PC11 ON PC11.customer_id = fb.customer_id
LEFT JOIN PC10 ON PC10.customer_id = fb.customer_id
LEFT JOIN PC9 ON PC9.customer_id = fb.customer_id
LEFT JOIN PC8 ON PC8.customer_id = fb.customer_id
LEFT JOIN PC7 ON PC7.customer_id = fb.customer_id
LEFT JOIN PC6 ON PC6.customer_id = fb.customer_id
LEFT JOIN PC5 ON PC5.customer_id = fb.customer_id;

CREATE OR REPLACE TEMP TABLE _is_new_TCs_opt_in AS
WITH _opt_in AS (
        SELECT a.customer_id
            ,value AS is_new_TCs_opt_in
        FROM LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP a
        JOIN lake_consolidated_view.ultra_merchant.membership_detail b ON a.MEMBERSHIP_ID = b.MEMBERSHIP_ID
        WHERE name ilike 'is_new_TCs_opt_in%'
        )
SELECT DISTINCT ms.customer_id
    ,IFF(is_new_TCs_opt_in IS NULL, 0, 1) is_new_TCs_opt_in
FROM _membership_status ms
LEFT JOIN _opt_in o ON ms.customer_id = o.customer_id
JOIN edw_prod.data_model.dim_store ds ON ms.store_id = ds.store_id
WHERE store_brand_abbr = 'JF';

CREATE OR REPLACE TEMP TABLE _previous_decile AS
SELECT
    CUSTOMER_ID,
	cumulative_cash_gross_profit_decile AS previous_decile
FROM (
	SELECT
	    cl.CUSTOMER_ID,
		cumulative_cash_gross_profit_decile,
		fa.cancellation_local_datetime,
		ROW_NUMBER() OVER(
			PARTITION BY cl.CUSTOMER_ID ORDER BY CANCELLATION_LOCAL_DATETIME DESC
			) AS r_no
	FROM edw_prod.analytics_base.customer_lifetime_value_monthly AS cl
	JOIN edw_prod.data_model.fact_activation AS fa
	    ON cl.customer_id = fa.customer_id
		AND cl.ACTIVATION_KEY = fa.ACTIVATION_KEY
	WHERE cl.IS_REACTIVATED_VIP = 'TRUE'
		AND CANCELLATION_LOCAL_DATETIME != '9999-12-31 00:00:00.000 -0800'
	) AS A
WHERE r_no = 1;

CREATE OR REPLACE TEMP TABLE _Is_In_Grace_Period AS
SELECT
    ms.customer_id,
	CASE
		WHEN (
		    ACTIVATION_LOCAL_DATETIME >= DATE_TRUNC(MONTH, CURRENT_DATE)
			AND ACTIVATION_LOCAL_DATETIME < DATEADD(DAYS, 5, DATE_TRUNC(MONTH, CURRENT_DATE))
			)
		AND (
            CURRENT_DATE >= DATEADD(DAYS, 21, DATEADD(MONTHS, - 1, DATE_TRUNC(MONTH, CURRENT_DATE)))
            AND CURRENT_DATE < DATE_TRUNC(MONTH, CURRENT_DATE)
            )
        THEN 1
		ELSE 0 END AS Is_In_Grace_Period
FROM _membership_status AS ms
JOIN edw_prod.data_model.fact_activation AS v
    ON ms.customer_id = v.customer_id;

CREATE OR REPLACE TEMP TABLE _Activating_UPT AS
SELECT
    fa.customer_id,
	CASE
		WHEN fo.UNIT_COUNT = 1 THEN 'Single'
		WHEN fo.UNIT_COUNT > 1 THEN 'Multi'
		END AS Activating_UPT
FROM edw_prod.data_model.fact_activation AS fa
JOIN edw_prod.data_model.fact_order AS fo
    ON fa.customer_id = fo.customer_id
    AND fa.order_id = fo.order_id
    AND fa.is_current = 1;

CREATE OR REPLACE TEMP TABLE _purchase_mens_only_activating_order AS
WITH _raw AS (
    SELECT DISTINCT
        fa.customer_id,
        fa.activation_local_datetime,
        fa.cancellation_local_datetime,
        fa.order_id,
        fol.order_line_id,
        CASE WHEN DEPARTMENT = 'Mens' THEN 1 ELSE 0 END AS MEN_UNIT
    FROM EDW_PROD.DATA_MODEL.FACT_ACTIVATION AS fa
    JOIN EDW_PROD.DATA_MODEL.FACT_ORDER_LINE AS fol
        ON fa.order_id = fol.order_id
    JOIN EDW_PROD.DATA_MODEL.DIM_PRODUCT AS dp
        ON fol.product_id = dp.product_id
    JOIN EDW_PROD.DATA_MODEL.FACT_ORDER AS fo
        ON fo.order_id = fol.order_id
    JOIN EDW_PROD.DATA_MODEL.DIM_ORDER_SALES_CHANNEL AS osc
        ON fo.order_sales_channel_key = osc.order_sales_channel_key
    JOIN EDW_PROD.DATA_MODEL.DIM_PRODUCT_TYPE AS pt
        ON pt.PRODUCT_TYPE_KEY = fol.PRODUCT_TYPE_KEY
    WHERE osc.ORDER_CLASSIFICATION_L1 = 'Product Order'
        AND pt.IS_FREE = 'FALSE'
    ),

_raw_agg AS (
    SELECT
        customer_id,
        activation_local_datetime,
        cancellation_local_datetime,
        COUNT(DISTINCT CASE WHEN MEN_UNIT = 1 THEN order_line_id END) as mens_order_line_id_count,
        COUNT(DISTINCT CASE WHEN MEN_UNIT = 0 THEN order_line_id END) as not_mens_order_line_id_count
    FROM _raw
    GROUP BY customer_id,
        activation_local_datetime,
        cancellation_local_datetime
    )

SELECT DISTINCT
    customer_id,
    activation_local_datetime,
    cancellation_local_datetime,
    CASE
        WHEN mens_order_line_id_count > 1 AND not_mens_order_line_id_count = 0
        THEN 1
        ELSE 0 END AS purchase_mens_only_activating_order
FROM _raw_agg
QUALIFY ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY activation_local_datetime DESC, cancellation_local_datetime DESC) = 1;

CREATE OR REPLACE TEMP TABLE _scrubs_interest_response AS
WITH BASE AS (
    SELECT
        CD.CUSTOMER_ID,
        CD.NAME,
        CD.DATETIME_ADDED,
        ROW_NUMBER() OVER(PARTITION BY CD.CUSTOMER_ID ORDER BY CD.DATETIME_ADDED ASC) AS RN
    FROM LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_DETAIL CD
WHERE CD.NAME IN ('scrubs_interest_yes', 'scrubs_interest_no')
    AND CD.VALUE IN ('YES')
    )

select
    CUSTOMER_ID,
    NAME
FROM BASE
WHERE RN = 1;

-- Existing BI Variable = most_recent_successful_credit_billing
-- Update logic to include only Paid billing orders / exclude Refunded orders using order payment status
CREATE OR REPLACE TEMP TABLE _most_recent_successful_credit_billing AS
SELECT DISTINCT
    o.customer_id,
    MAX(DATE(CONVERT_TIMEZONE('America/Los_Angeles', o.ORDER_LOCAL_DATETIME))) AS most_recent_successful_credit_billing
FROM edw_prod.data_model.fact_order AS o
JOIN edw_prod.data_model.dim_customer AS dc
    ON o.customer_id=dc.customer_id
JOIN EDW_PROD.DATA_MODEL.DIM_STORE AS ds
    ON ds.store_id = o.store_id
JOIN edw_prod.data_model.dim_order_sales_channel AS oc
    ON oc.order_sales_channel_key = o.order_sales_channel_key
JOIN edw_prod.data_model.dim_order_status AS os
    ON os.order_status_key = o.order_status_key
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_ORDER_PAYMENT_STATUS ops
    on ops.order_payment_status_key = o.order_payment_status_key
    and ops.order_payment_status = 'Paid'   --exclude Refunded billing orders
WHERE dc.IS_TEST_CUSTOMER = 0
    AND ds.STORE_BRAND = 'Savage X'
    AND ds.STORE_NAME NOT LIKE '%(DM)%'
    AND dc.CUSTOMER_ID NOT IN (503623531, 875180410)
    AND oc.ORDER_CLASSIFICATION_L1 ILIKE 'billing order'
    AND os.order_status ILIKE 'success'
GROUP BY o.customer_id;

CREATE OR REPLACE TEMP TABLE _redeemed_downgraded_prom AS
SELECT DISTINCT
    dc.CUSTOMER_ID,
    CASE
        WHEN p.code ILIKE '%downgrad%'
            OR p.code ILIKE '%reactiv%'
        THEN 1 ELSE 0 END AS Redeemed_Downgraded_Promo
FROM EDW_PROD.DATA_MODEL.FACT_MEMBERSHIP_EVENT AS mee
JOIN EDW_PROD.DATA_MODEL.DIM_CUSTOMER AS dc
    ON dc.customer_id = mee.customer_id
JOIN EDW_PROD.DATA_MODEL.DIM_STORE AS ds
    ON ds.store_id = mee.STORE_ID
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.ORDER_DISCOUNT AS od
    ON od.order_id = mee.order_id
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.PROMO AS p
    ON p.promo_id = od.promo_id
WHERE dc.IS_TEST_CUSTOMER = 0
    AND ds.STORE_GROUP = 'Savage X'
    AND ds.STORE_NAME NOT LIKE '%(DM)%'
    AND dc.CUSTOMER_ID NOT IN (503623531, 875180410)
    AND mee.membership_state = 'VIP'
    AND mee.Membership_event_type = 'Activation'
    AND (
        p.code ILIKE '%downgrad%'
        OR p.code ILIKE '%reactiv%')
;

CREATE OR REPLACE TEMP TABLE _ecomm_not_vip_flag AS
SELECT DISTINCT
    customer_id,
    store_id
FROM _membership_status
WHERE membership_state ILIKE 'guest'
    AND customer_id NOT IN (
        SELECT customer_id
        FROM _vip_flag_recent_cohort
    )
;

CREATE OR REPLACE TEMP TABLE _cancel_flag AS
SELECT DISTINCT
    customer_id,
    event_start_local_datetime
FROM _membership_status
WHERE membership_state ILIKE 'cancelled';

CREATE OR REPLACE TEMP TABLE _vip_w_cancel AS
SELECT
    vrc.customer_id,
    vrc.store_id,
    vrc.vip_cohort,
    vrc.event_start_local_datetime AS activation_local_datetime,
    cf.event_start_local_datetime AS cancel_local_datetime
FROM _vip_flag_recent_cohort AS vrc
LEFT JOIN _cancel_flag AS cf
    ON vrc.customer_id = cf.customer_id
    AND vrc.event_start_local_datetime <= cf.event_start_local_datetime
QUALIFY ROW_NUMBER() OVER(PARTITION BY vrc.customer_id
    ORDER BY vrc.event_start_local_datetime DESC, cf.event_start_local_datetime DESC
    ) = 1;

CREATE OR REPLACE TEMP TABLE _lead_registration_channel AS
SELECT
    a.customer_id,
    COALESCE(ss.channel, 'na') AS channel,
    COALESCE(ss.utm_source, 'na') AS source,
    COALESCE(ss.utm_medium, 'na') AS medium,
    CONCAT(source, ' | ', medium, ' | ', channel) AS registration_source_medium_channel
FROM _membership_status AS a
JOIN reporting_base_prod.staging.session_media_channel AS ss
    ON a.session_id = ss.session_id
WHERE a.membership_event_type ILIKE 'registration'
;

CREATE OR REPLACE TEMP TABLE _recent_vip_activation_channel AS
SELECT
    a.customer_id,
    COALESCE(ss.channel, 'na') AS channel,
    COALESCE(ss.utm_source, 'na') AS source,
    COALESCE(ss.utm_medium, 'na') AS medium,
    CONCAT(source, ' | ', medium, ' | ', channel) AS activation_source_medium_channel
FROM _vip_flag_recent_cohort a
LEFT JOIN reporting_base_prod.staging.session_media_channel AS ss
    ON a.session_id = ss.session_id;

CREATE OR REPLACE TEMP TABLE _app_vip_activation_flag AS
SELECT customer_id
FROM _vip_flag_recent_cohort
WHERE vip_cohort IS NOT NULL
    AND store_type = 'Mobile App';

CREATE OR REPLACE TEMP TABLE _status AS
SELECT DISTINCT
    a.customer_id,
    a.store_id,
    a.store_group,
    a.store_group_id,
    a.store_brand,
    a.membership_state,
    COALESCE(sr.skip_reason, 'unknown') AS skip_reason,
    gp.Is_In_Grace_Period,
    au.Activating_UPT,
    CAST(ma.membership_amount_saved_to_date AS NUMBER(38,12)) AS membership_amount_saved_to_date,
    ao.purchase_mens_only_activating_order,
    pd.previous_decile,
    COALESCE(pc.pre_passive_cancel_2m_check, 0) AS pre_passive_cancel_2m_check,
    COALESCE(vc.vip_cohort, '1900-01-01') AS vip_cohort,
    lt.lead_registration_datetime AS lead_registration_datetime,
    COALESCE(to_date(vc.activation_local_datetime), '1900-01-01') AS activation_local_date,
    COALESCE(to_date(vc.cancel_local_datetime), '1900-01-01') AS cancel_local_date,
    COALESCE(lac.registration_source_medium_channel, 'unknown') AS registration_source_medium_channel,
    COALESCE(rvac.activation_source_medium_channel,
        IFF(COALESCE(vc.vip_cohort, '1900-01-01') <> '1900-01-01',
            'unknown', NULL)) AS activation_source_medium_channel,
    IFF(mvaf.customer_id IS NOT NULL, 1, 0) AS mobile_vip_activation_flag
FROM _current_status AS a
LEFT JOIN _vip_w_cancel AS vc
    ON a.customer_id = vc.customer_id
    AND a.store_id = vc.store_id
LEFT JOIN _always_lead AS lt
    ON a.customer_id = lt.customer_id
LEFT JOIN _lead_registration_channel AS lac
    ON a.customer_id = lac.customer_id
LEFT JOIN _recent_vip_activation_channel AS rvac
    ON a.customer_id = rvac.customer_id
LEFT JOIN _app_vip_activation_flag AS mvaf
    ON a.customer_id = mvaf.customer_id
LEFT JOIN _skip_reason AS sr
    ON a.customer_id = sr.customer_id
LEFT JOIN _Is_In_Grace_Period AS gp
    ON a.customer_id = gp.customer_id
LEFT JOIN _Activating_UPT AS au
    ON a.customer_id = au.customer_id
LEFT JOIN _purchase_mens_only_activating_order AS ao
    ON a.customer_id = ao.customer_id
LEFT JOIN _previous_decile AS pd
    ON a.customer_id = pd.customer_id
LEFT JOIN _membership_amount_saved_to_date AS ma
    ON ma.customer_id = a.customer_id
LEFT JOIN _pre_passive_cancel_2m_check AS pc
    ON a.customer_id = pc.customer_id;

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE _max_showroom_ubt AS
SELECT DISTINCT
    ubt.sku,
    ubt.gender,
    ubt.us_vip_dollar,
    REGEXP_REPLACE(us_msrp_dollar::STRING, '[^a-zA-Z0-9]+')::FLOAT / 100 AS us_msrp_dollar_clean
FROM lake.excel.fl_merch_items_ubt AS ubt
JOIN (
    SELECT
        sku,
        MAX(current_showroom) AS max_current_showroom
    FROM lake.excel.fl_merch_items_ubt
    GROUP BY sku
    ) AS ms
    ON ubt.sku = ms.sku
    AND ubt.current_showroom = ms.max_current_showroom;

CREATE OR REPLACE TEMP TABLE _sf_showroom_ubt AS
SELECT DISTINCT
    sfubt.*
from reporting_prod.sxf.style_master sfubt
JOIN (
    SELECT DISTINCT
        color_sku_po,
        latest_showroom
    from reporting_prod.sxf.style_master
    ) AS ms
    ON sfubt.color_sku_po = ms.color_sku_po
    AND sfubt.latest_showroom = ms.latest_showroom;

CREATE OR REPLACE TEMP TABLE _product_purchase_attributes AS
SELECT
    fol.customer_id AS ppa_customer_id,
    ds.store_brand AS ppa_store_brand,
    MAX(IFF(ds.store_type ILIKE 'retail', 1, 0)) AS shopped_retail_flag,
    MAX(IFF(ds.store_type ILIKE 'online', 1, 0)) AS shopped_online_flag,
    MAX(IFF(ds.store_type ILIKE 'mobile app', 1, 0)) AS shopped_mobile_app_flag,
    CASE
        WHEN ds.store_brand = 'Fabletics' AND MAX(IFF(ubt.gender ILIKE 'women''s', 1, 0)) > 0 THEN 1
        WHEN ds.store_brand = 'Savage X' AND MAX(IFF(sfubt.gender ILIKE 'women', 1, 0)) > 0 THEN 1
    ELSE 0 END AS shopped_female_product,
    CASE
        WHEN ds.store_brand = 'Fabletics' AND MAX(IFF(ubt.gender ILIKE 'men''s', 1, 0)) > 0 THEN 1
        WHEN ds.store_brand = 'Savage X' AND MAX(IFF(sfubt.gender ILIKE 'men', 1, 0)) > 0 THEN 1
    ELSE 0 END AS shopped_male_product,
    CASE
        WHEN ds.store_brand = 'Fabletics'
            AND MAX(IFF(ubt.gender ILIKE 'men''s', 1, 0)) > 0
            AND MAX(IFF(ubt.gender ILIKE 'women''s', 1, 0)) = 0
        THEN 1
        WHEN ds.store_brand = 'Savage X'
            AND MAX(IFF(sfubt.gender ILIKE 'men', 1, 0)) > 0
            and MAX(IFF(sfubt.gender ILIKE 'women', 1, 0)) = 0
        THEN 1
    END AS shopped_male_product_only, --net new field
    0 AS shopped_unisex_product,    --net new field
    MAX(CASE WHEN ds.store_type ILIKE 'retail' THEN fol.order_local_datetime END) as last_retail_purchase_date,
    MAX(CASE WHEN ds.store_type ILIKE 'online' THEN fol.order_local_datetime END) AS last_online_purchase_date,
    MAX(CASE WHEN ds.store_type ILIKE 'mobile app' THEN fol.order_local_datetime END) AS last_mobile_app_purchase_date,
    MAX(fol.order_local_datetime) AS last_product_order_purchase_date,
    CASE
        WHEN store_brand = 'Fabletics'
        THEN SUM(NVL(ubt.us_msrp_dollar_clean, 0)) - SUM(NVL(ubt.us_vip_dollar, 0))
        WHEN store_brand = 'Savage X'
        THEN SUM(NVL(sfubt.msrp, 0)) - SUM(NVL(sfubt.vip_price, 0))
    END AS vip_lifetime_saving
FROM edw_prod.data_model.fact_order_line AS fol
JOIN edw_prod.data_model.fact_order AS fo
    ON fol.order_id = fo.order_id
JOIN edw_prod.data_model.dim_product AS dp
    ON dp.product_id = fol.product_id
JOIN edw_prod.data_model.dim_store AS ds
    ON fol.store_id = ds.store_id
JOIN edw_prod.data_model.dim_product_type AS dpt
    ON fol.product_type_key = dpt.product_type_key
    AND dpt.is_free = 0
JOIN edw_prod.data_model.dim_order_sales_channel AS oc
    ON oc.order_sales_channel_key = fol.order_sales_channel_key
    AND oc.order_classification_l2 ILIKE 'product order'
JOIN edw_prod.data_model.dim_order_status AS os
    ON os.order_status_key = fol.order_status_key
JOIN edw_prod.data_model.dim_order_processing_status AS ops
    ON ops.order_processing_status_key = fo.order_processing_status_key
    AND ops.order_processing_status <> 'Cancelled (Incomplete Auth Redirect)'
JOIN edw_prod.data_model.dim_order_line_status AS ols
    ON fol.order_line_status_key = ols.order_line_status_key
    AND ols.order_line_status <> 'Cancelled'
LEFT JOIN _vip_w_cancel AS v
    ON fol.customer_id = v.customer_id
    AND TO_DATE(fol.order_local_datetime) >= TO_DATE(v.activation_local_datetime)
    AND TO_DATE(fol.order_local_datetime) <=
        COALESCE(TO_DATE(v.cancel_local_datetime), CURRENT_DATE())
LEFT JOIN _ecomm_not_vip_flag AS env
    ON env.customer_id = fol.customer_id
LEFT JOIN _max_showroom_ubt AS ubt
    ON dp.product_sku = ubt.sku
LEFT JOIN reporting_prod.sxf.style_master AS sfubt
    ON dp.product_sku = sfubt.color_sku_po
WHERE 1 = 1
    AND oc.is_border_free_order = 0
    AND LOWER(os.order_status) IN ('success','pending')
    AND oc.is_ps_order = 0
    AND oc.is_test_order = FALSE
    AND (
        v.customer_id IS NOT NULL
        OR env.customer_id IS NOT NULL
        )
GROUP BY fol.customer_id,
    ds.store_brand;

CREATE OR REPLACE TEMP TABLE _orders_placed AS
SELECT DISTINCT
    o.customer_id,
    o.store_id,
    o.order_id,
    order_local_datetime::DATE AS placed_date,
    CAST(
        LEAD(order_local_datetime) OVER(PARTITION BY o.customer_id ORDER BY order_local_datetime ASC)
    AS DATE) AS next_placed_date
FROM edw_prod.data_model.fact_order AS o
join edw_prod.data_model.dim_order_sales_channel AS oc
    ON oc.order_sales_channel_key = o.order_sales_channel_key
    AND oc.order_classification_l2 ILIKE 'product order'
    AND oc.is_border_free_order = 0
    AND oc.is_ps_order = 0
    AND oc.is_test_order = FALSE
JOIN edw_prod.data_model.dim_order_status AS os
    ON os.order_status_key = o.order_status_key
JOIN edw_prod.data_model.dim_order_processing_status AS ops
    ON ops.order_processing_status_key = o.order_processing_status_key
LEFT JOIN _vip_w_cancel AS v
    ON o.customer_id = v.customer_id
    AND TO_DATE(o.order_local_datetime) >= TO_DATE(v.activation_local_datetime)
    AND TO_DATE(o.order_local_datetime) <=
        COALESCE(TO_DATE(v.cancel_local_datetime), CURRENT_DATE())
LEFT JOIN _ecomm_not_vip_flag AS env
    ON env.customer_id = o.customer_id
WHERE (
    os.order_status ILIKE 'success'
    OR (
        os.order_status ILIKE 'pending'
        AND lower(ops.order_processing_status) IN
            ('fulfillment (batching)', 'fulfillment (in progress)', 'placed')
        )
    )
    AND (
        v.customer_id IS NOT NULL
        OR env.customer_id IS NOT NULL
    )
;



CREATE OR REPLACE TEMP TABLE _cust_bday AS
SELECT distinct clvm.customer_id, 1 as IS_BIRTH_MONTH
FROM edw_prod.analytics_base.customer_lifetime_value_monthly clvm
JOIN edw_prod.data_model.dim_customer dc ON clvm.customer_id = dc.customer_id
WHERE clvm.is_bop_vip = 1
and dc.birth_month = month(current_date);


CREATE OR REPLACE TEMP TABLE _avg_days_between_purchase AS
SELECT
    customer_id,
    MAX(placed_date) AS last_placed_date,
    AVG(DATEDIFF(DAY, placed_date, next_placed_date)) AS avg_days_between_purchase
FROM _orders_placed
GROUP BY customer_id;

CREATE OR REPLACE TEMP TABLE _retail_store_drive_time AS
SELECT
    ms.customer_id,
    ms.store_group_id,
    ds.store_id AS nearest_retail_store_id,
    ds.store_full_name AS nearest_retail_store,
    rdd.duration AS retail_store_drive_time,
    rdd.distance AS retail_store_distance
FROM _current_status AS ms
JOIN edw_prod.data_model.dim_customer AS dc
    ON ms.customer_id = dc.customer_id
JOIN reporting_base_prod.data_science.fl_retail_driving_distance AS rdd
    ON rdd.vip_zip = (
        IFF(default_postal_code <> 'Unknown',
        LEFT(REPLACE(dc.default_postal_code, ' ', ''), 5),
        LEFT(REPLACE(dc.quiz_zip, ' ', ''), 5))
        )
JOIN edw_prod.data_model.dim_store AS ds
    ON ds.store_retail_zip_code = LEFT(rdd.store_zip, 5)
    AND DS.store_brand = 'Fabletics'   --join to only fabletics stores in dim store
WHERE 1 = 1
    AND ms.store_group = 'Fabletics'
    AND ds.store_type = 'Retail'
    AND ds.store_sub_type = 'Store'
    AND ds.store_retail_status ILIKE 'open%'
    AND ds.store_id NOT IN (215, 292, 288, 257)
        /*('Century City Spring Mrkt','555 Aviation','Oakstreet Beach-AVP','Manhattan Beach-AVP') */
QUALIFY ROW_NUMBER() OVER(PARTITION BY ms.customer_id, ms.store_brand ORDER BY distance ASC) = 1

UNION ALL

SELECT
    ms.customer_id,
    ms.store_group_id,
    ds.store_id AS nearest_retail_store_id,
    ds.store_full_name AS nearest_retail_store,
    srdd.duration AS retail_store_drive_time,
    srdd.distance AS retail_store_distance
FROM _current_status AS ms
JOIN edw_prod.data_model.dim_customer AS dc
    ON ms.customer_id=dc.customer_id
JOIN reporting_base_prod.data_science.sxf_retail_driving_distance AS srdd
      ON (
        IFF(default_postal_code <> 'Unknown',
        LEFT(REPLACE(default_postal_code, ' ', ''), 5),
        LEFT(REPLACE(dc.quiz_zip, ' ', ''), 5))
        ) = srdd.vip_zip
JOIN edw_prod.data_model.dim_store AS ds
    ON ds.store_retail_zip_code = LEFT(srdd.store_zip, 5)
    AND DS.store_brand = 'Savage X'  --join to only savage stores in dim store
WHERE ms.store_group='Savage X'
    AND ds.store_type = 'Retail'
    AND ds.store_sub_type <> 'Legging Bar'
    AND ds.store_sub_type <> 'Varsity'
    AND ds.store_full_name NOT IN ('Century City Spring Mrkt', '555 Aviation')
    AND ds.store_id NOT IN (144,145,156,157)
        /*('RTLSXF-Las Vegas','RTLSXF-Mall of America','RTLSXF-SOHO','RTLSXF-Valley Fair')*/
QUALIFY ROW_NUMBER() OVER(PARTITION BY ms.customer_id, ms.store_brand ORDER BY distance ASC) = 1;

CREATE OR REPLACE TEMP TABLE _nearest_retail_store AS
WITH GoogleMapsLinks AS (
    SELECT DISTINCT
        STORE_ID,
        CONFIGURATION_VALUE AS GoogleMapsLink
    FROM lake_view.ULTRA_WAREHOUSE.RETAIL_LOCATION AS rl
    JOIN lake_view.ULTRA_WAREHOUSE.RETAIL_LOCATION_CONFIGURATION AS rlc
        ON rlc.RETAIL_LOCATION_ID = rl.RETAIL_LOCATION_ID
    WHERE RETAIL_CONFIGURATION_ID = 45
    ORDER BY STORE_ID ASC
    ),
StoreLocatorInMall AS (
    SELECT DISTINCT
        STORE_ID,
        CONFIGURATION_VALUE AS StoreLocatorInMall
    FROM lake_view.ULTRA_WAREHOUSE.RETAIL_LOCATION AS rl
    JOIN lake_view.ULTRA_WAREHOUSE.RETAIL_LOCATION_CONFIGURATION AS rlc
        ON rlc.RETAIL_LOCATION_ID = rl.RETAIL_LOCATION_ID
    WHERE RETAIL_CONFIGURATION_ID = 64
    ORDER BY STORE_ID ASC
    )

SELECT DISTINCT
    rl.store_id AS nearest_retail_store_id,
	LABEL AS nearest_retail_store,
	address1 AS nearest_retail_store_address1,
	address2 AS nearest_retail_store_address2,
	city AS nearest_retail_store_city,
	STATE AS nearest_retail_store_state,
	zip AS nearest_retail_store_zip,
	country_code AS nearest_retail_store_country_code,
	GoogleMapsLink AS nearest_retail_store_google_maps_link,
	StoreLocatorInMall AS nearest_retail_store_location_in_mall
FROM lake_view.ULTRA_WAREHOUSE.RETAIL_LOCATION AS rl
JOIN lake_view.ULTRA_WAREHOUSE.RETAIL_LOCATION_CONFIGURATION AS rlc
    ON rlc.RETAIL_LOCATION_ID = rl.RETAIL_LOCATION_ID
LEFT JOIN GoogleMapsLinks AS gml
    ON rl.STORE_ID = gml.STORE_ID
LEFT JOIN StoreLocatorInMall AS slm
    ON rl.STORE_ID = slm.STORE_ID
JOIN EDW_PROD.DATA_MODEL.DIM_STORE AS DS
    ON DS.STORE_ID = RL.STORE_ID
WHERE DS.STORE_TYPE = 'Retail'
	AND DS.STORE_SUB_TYPE = 'Store'
	AND DS.STORE_GROUP IN (
		'Fabletics', 'Savage X'
		)
QUALIFY ROW_NUMBER() OVER(PARTITION BY rl.STORE_ID ORDER BY rlc.HVR_CHANGE_TIME ASC ) = 1
ORDER BY rl.store_id ASC;

CREATE OR REPLACE TEMP TABLE _nearest_retail_store_final AS
SELECT
    rsd.customer_id,
    nrs.*,
    rsd.retail_store_drive_time,
    rsd.retail_store_distance
FROM _retail_store_drive_time AS rsd
LEFT JOIN _nearest_retail_store AS nrs
    ON rsd.nearest_retail_store_id = nrs.nearest_retail_store_id;

CREATE OR REPLACE TEMP TABLE area_code_timezone_map AS
SELECT DISTINCT parsed_area_code, timezone
FROM (
    SELECT DISTINCT
        substr(trim(replace(phone_number, '+', '')), 2, 3) AS parsed_area_code,
        zcs.timezone,
        ROW_NUMBER() OVER (PARTITION BY substr(trim(replace(phone_number, '+', '')), 2, 3)
                           ORDER BY
                               CASE
                                   WHEN zcs.timezone = 'EST' THEN 1
                                   WHEN zcs.timezone = 'CST' THEN 2
                                   WHEN zcs.timezone = 'MST' THEN 3
                                   WHEN zcs.timezone = 'PST' THEN 4
                                   ELSE 5 -- Any unknown timezone gets lowest priority
                               END) AS timezone_rank
    FROM campaign_event_data.org_3223.users u
    JOIN lake_consolidated_view.ultra_merchant.zip_city_state zcs
        ON zcs.areacode = substr(trim(replace(u.phone_number, '+', '')), 2, 3)
    WHERE regexp_like(phone_number, '^\\+\\d{11,15}$')
)
WHERE timezone_rank = 1;  -- Keep only the westernmost timezone per area code

CREATE OR REPLACE TEMP TABLE _Customer_timezone AS
WITH zip_code AS (
    SELECT
        IFF(DEFAULT_POSTAL_CODE='Unknown', QUIZ_ZIP, DEFAULT_POSTAL_CODE) AS zip,
        customer_id
    FROM edw_prod.data_model.dim_customer
)
SELECT
    zc.customer_id,
    coalesce(actm.timezone, LEFT(zcs.timezone, 3)) AS TIMEZONE
FROM zip_code AS zc
JOIN lake_consolidated_view.ULTRA_MERCHANT.ZIP_CITY_STATE AS zcs
    ON zc.zip = zcs.zip
LEFT JOIN area_code_timezone_map actm
    ON zcs.areacode = actm.parsed_area_code;

CREATE OR REPLACE TEMP TABLE _status_orders AS
SELECT DISTINCT
    s.*,
    adbp.avg_days_between_purchase,
    adbp.last_placed_date,
    ppa.*,
    fr.nearest_retail_store,
    fr.retail_store_drive_time,
    fr.retail_store_distance,
    fr.nearest_retail_store_address1,
    fr.nearest_retail_store_address2,
    fr.nearest_retail_store_city,
    fr.nearest_retail_store_state,
    fr.nearest_retail_store_zip,
    fr.nearest_retail_store_country_code,
    fr.nearest_retail_store_google_maps_link,
    fr.nearest_retail_store_location_in_mall
FROM _status AS s
LEFT JOIN _product_purchase_attributes AS ppa
    ON ppa.ppa_customer_id = s.customer_id
    AND s.store_brand = ppa.ppa_store_brand
LEFT JOIN _avg_days_between_purchase AS adbp
    ON s.customer_id = adbp.customer_id
LEFT JOIN _nearest_retail_store_final AS fr
    ON s.customer_id = fr.customer_id;

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE _deciles AS
SELECT
    customer_id,
    vip_cohort_month_date,
    cumulative_cash_gross_profit_decile AS current_cash_margin_decile,
    cumulative_product_gross_profit_decile AS current_product_margin_decile
FROM edw_prod.analytics_base.customer_lifetime_value_monthly AS a
WHERE 1 = 1
    AND month_date = $max_month
QUALIFY ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY vip_cohort_month_date DESC) = 1
;

CREATE OR REPLACE TEMP TABLE _deciles_fabkids AS
SELECT
    a.customer_id,
    MAX(cumulative_cash_gross_profit_decile) AS max_cash_margin_decile,
    MAX(cumulative_product_gross_profit_decile) AS max_product_margin_decile
FROM edw_prod.analytics_base.customer_lifetime_value_monthly AS a
JOIN edw_prod.data_model.dim_store AS st
    ON a.store_id = st.store_id
WHERE 1 = 1
    AND month_date = $max_month
    AND st.store_brand = 'FabKids'
GROUP BY a.customer_id;

CREATE OR REPLACE TEMP TABLE _deciles_all AS
SELECT
    d1.customer_id,
    d1.vip_cohort_month_date,
    IFF(d2.customer_id IS NOT NULL, max_cash_margin_decile, current_cash_margin_decile) AS cash_margin_decile,
    IFF(d2.customer_id IS NOT NULL, max_product_margin_decile, current_product_margin_decile) AS product_margin_decile
FROM _deciles AS d1
LEFT JOIN _deciles_fabkids AS d2
    ON d1.customer_id = d2.customer_id;

CREATE OR REPLACE TEMP TABLE _billing_cycle_outcome AS
SELECT
    ltv.customer_id,
    ltv.store_id,
    ltv.vip_cohort_month_date,
    ltv.customer_action_category
FROM edw_prod.analytics_base.customer_lifetime_value_monthly AS ltv
WHERE ltv.MONTH_DATE = $max_month
QUALIFY ROW_NUMBER() OVER(PARTITION BY ltv.customer_id ORDER BY ltv.ACTIVATION_KEY DESC) = 1
;

CREATE OR REPLACE TEMP TABLE _bill_me_now AS
SELECT
    m.customer_id,
    v.vip_cohort,
    MAX(TO_DATE(fo.order_local_datetime)) AS latest_bill_me_now_date,
    COUNT(DISTINCT mb.membership_id) AS bill_me_now_count
FROM lake_consolidated_view.ultra_merchant.membership AS m
JOIN lake_consolidated_view.ultra_merchant.membership_billing AS mb
    ON m.membership_id = mb.membership_id
JOIN lake_consolidated_view.ultra_merchant.membership_billing_source AS mbs
    ON mb.membership_billing_source_id = mbs.membership_billing_source_id
    AND mbs.label = 'On-Demand Billing'
JOIN edw_prod.data_model.fact_order AS fo
    ON mb.order_id = fo.order_id
JOIN _vip_w_cancel AS v
    ON m.customer_id = v.customer_id
WHERE fo.order_local_datetime >= v.activation_local_datetime
GROUP BY m.customer_id,
    v.vip_cohort;

CREATE OR REPLACE TEMP TABLE _emp_enrolled_flag AS
SELECT
    cd.customer_id,
    v.vip_cohort
FROM lake_consolidated_view.ultra_merchant.customer_detail AS cd
JOIN _vip_w_cancel AS v
    ON v.customer_id = cd.customer_id
WHERE cd.name = 'nmp_opt_in_true'
    AND lower(cd.value) in ('true', 'yes') --search for both true and yes values to check emp opt-in
    AND cd.datetime_added >= v.activation_local_datetime;

CREATE OR REPLACE TEMP TABLE _lifetime_value AS
SELECT
    v.customer_id,
    ltv.vip_cohort_month_date,
    COUNT(CASE WHEN ltv.IS_SUCCESSFUL_BILLING = TRUE THEN 1 END) AS successful_credit_billings,
    COUNT(CASE WHEN ltv.IS_FAILED_BILLING = TRUE THEN 1 END) AS failed_credit_billings,
    IFF(COUNT(CASE WHEN ltv.IS_FAILED_BILLING = TRUE THEN 1 END) >= 2, 1, 0) AS membership_repeat_failed_billing,
    SUM(IFNULL(ltv.product_net_revenue, 0)) AS ltv_order_revenue,
    SUM(IFNULL(ltv.cash_net_revenue, 0)) AS ltv_total_revenue,
    SUM(IFNULL(ltv.cash_gross_profit, 0)) AS ltv_cash_gross_margin,
    ltv_cash_gross_margin / NULLIF(ltv_total_revenue, 0) * 100 AS ltv_gross_margin_pct,
    SUM(ltv.product_order_count) AS product_orders,
    SUM(ltv.product_gross_revenue) / NULLIF(product_orders, 0) AS avg_aov
FROM _vip_and_guest_recent_cohort AS v
JOIN edw_prod.analytics_base.customer_lifetime_value_monthly AS ltv
    ON v.customer_id = ltv.CUSTOMER_ID
    AND v.vip_cohort = ltv.VIP_COHORT_MONTH_DATE
GROUP BY v.customer_id,
    ltv.vip_cohort_month_date;

CREATE OR REPLACE TEMP TABLE _status_orders_billings AS
SELECT DISTINCT
    o.*,
    d.cash_margin_decile AS current_cash_margin_decile,
    d.product_margin_decile AS current_product_margin_decile,
    ltv.ltv_order_revenue,
    ltv.ltv_total_revenue,
    ltv.ltv_cash_gross_margin,
    ltv.ltv_gross_margin_pct,
    ltv.product_orders,
    ltv.successful_credit_billings AS credit_billings,
    ltv.avg_aov,
    COALESCE(ltv.membership_repeat_failed_billing, 0) AS membership_repeat_failed_billing_flag,
    COALESCE(ltv.failed_credit_billings, 0) AS consecutive_failed_billing_months,
    COALESCE(bmn.latest_bill_me_now_date, '1900-01-01') AS recent_bill_me_now_date,
    COALESCE(bco.customer_action_category, 'n/a') AS recent_customer_action,
    IFF(nef.customer_id IS NOT NULL, 1, 0) AS emp_enrollment_flag
FROM _status_orders AS o
LEFT JOIN _deciles_all AS d
    ON o.customer_id = d.customer_id
    AND o.vip_cohort = d.vip_cohort_month_date
LEFT JOIN _lifetime_value AS ltv
    ON o.customer_id = ltv.customer_id
    AND o.vip_cohort = ltv.vip_cohort_month_date
LEFT JOIN _bill_me_now AS bmn
    ON o.customer_id = bmn.customer_id
    AND o.vip_cohort = bmn.vip_cohort
LEFT JOIN _billing_cycle_outcome AS bco
    ON o.customer_id = bco.customer_id
    AND o.vip_cohort = bco.vip_cohort_month_date
LEFT JOIN _emp_enrolled_flag AS nef
    ON o.customer_id = nef.customer_id
    AND o.vip_cohort = nef.vip_cohort;

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE _brand_app_downloads_flag AS
SELECT
    dc.customer_id,
    ds.store_brand
FROM edw_prod.data_model.dim_customer AS dc
JOIN edw_prod.data_model.dim_store AS ds
    ON dc.store_id = ds.store_id
WHERE dc.mobile_app_cohort_month_date IS NOT NULL
;

CREATE OR REPLACE TEMP TABLE _hdyh AS
SELECT
    customer_id,
    store_id,
    HOW_DID_YOU_HEAR,
    birth_year,
    birth_month,
    birth_day
FROM edw_prod.data_model.dim_customer;

CREATE OR REPLACE TEMP TABLE _hydrow_purchaser AS
SELECT DISTINCT h.customer_id
FROM (
    SELECT b.customer_id
    FROM reporting_prod.fabletics.hydrow_bundle_list AS a
    JOIN (
        SELECT DISTINCT
            fhbl.customer_email,
            TO_DATE(fhbl.order_time) AS order_date,
            COALESCE(c.customer_id, c2.customer_id) AS customer_id
        FROM reporting_prod.fabletics.hydrow_bundle_list AS fhbl
        LEFT JOIN (
            SELECT
                fhbl.customer_email,
                customer_id
            FROM reporting_prod.fabletics.hydrow_bundle_list AS fhbl
            LEFT JOIN edw_prod.data_model.dim_customer c
                ON LOWER(fhbl.customer_email) = LOWER(c.email)
                /*this join gets the customer_ids for those who have the
                  same customer_email in Hydrow's system and in ours */
            LEFT JOIN edw_prod.data_model.dim_store AS st
                ON st.store_id = c.store_id
            WHERE fhbl.alternative_email IS NULL
                AND st.store_brand_abbr = 'FL'
                AND st.store_country = 'US'
            ) AS c
            ON c.customer_email = fhbl.customer_email
            LEFT JOIN (
                SELECT
                    fhbl.customer_email,
                    customer_id
                FROM reporting_prod.fabletics.hydrow_bundle_list AS fhbl
                LEFT JOIN edw_prod.data_model.dim_customer AS c
                    ON LOWER(fhbl.alternative_email) = LOWER(c.email)
                    /*this join gets the customer_ids for those who had a
                      different email in Hydrow's system than in ours */
                LEFT JOIN edw_prod.data_model.dim_store AS st
                    ON st.store_id = c.store_id
                WHERE fhbl.alternative_email IS NOT NULL
                    AND st.store_brand_abbr = 'FL'
                    AND st.store_country = 'US'
            ) AS c2
            on c2.customer_email = fhbl.customer_email
        ) AS b
        ON a.customer_email = b.customer_email
        AND a.order_time = b.order_date
    ) AS h
;

CREATE OR REPLACE TEMP TABLE _fitness_app_download AS
SELECT DISTINCT
    CONCAT(TRIM(SPLIT_PART(u.userid, '_', 2)),'20') AS customer_id
FROM lake.segment_fl.react_native_fabletics_fitness_app_application_opened AS a
JOIN lake_view.segment_fl.react_native_fabletics_fitness_app_users AS u
    ON a.context_device_id = u.context_device_id;

CREATE OR REPLACE TEMP TABLE _sxf_sizechart AS
SELECT
    dc.customer_id,
    CASE
        WHEN b.SIZE_CATEGORY = 'Curvy' THEN 'Curvy Top'
        WHEN b.SIZE_CATEGORY = 'Missy' THEN 'Missy Top'
        ELSE 'Unknown Bra Size'
    END AS sx_bra_size,
    CASE
        WHEN dc.undie_size IN ('XS', 'S', 'M', 'L', 'XL') THEN 'Missy Bottom'
        WHEN dc.undie_size IN ('1X', '2X', '3X', '2XL', '3XL', '4XL') THEN 'Curvy Bottom'
        ELSE 'Unknown Undie Size'
    END AS sx_undie_size
FROM edw_prod.data_model.dim_customer AS dc
JOIN edw_prod.data_model.dim_store AS ds
    ON ds.store_id = dc.store_id
JOIN _vip_w_cancel AS vp
    ON vp.customer_id = dc.customer_id
LEFT JOIN reporting_prod.sxf.bra_size_categorization AS b
    ON b.BRA_SIZE = dc.BRA_SIZE
    AND b.country = ds.store_country
WHERE dc.is_test_customer = 0
    AND ds.store_brand = 'Savage X'
    AND ds.store_name NOT LIKE '%(DM)%';

/* SXF Gamers */
CREATE OR REPLACE TEMP TABLE _vip_level_gamers_by_customer AS
SELECT
    customer_id,
    activating_phone_matched_customer_id,
    activating_cnh_matched_customer_id,
    activating_defaddress_matched_customer_id,
    activating_billingaddress_matched_customer_id,
    repeat_billing_matched_customer_id,
    repeat_cnh_matched_custimer_id
FROM reporting_prod.sxf.view_vip_level_gamers_by_customer;

-- table of all gamers both new customer ids and the matched customer ids

CREATE OR REPLACE TEMP TABLE _sxf_vip_gamers AS
SELECT DISTINCT CONCAT(customer_id, '30') as customer_id --concat with sxf company id
FROM (
    SELECT customer_id
    FROM _vip_level_gamers_by_customer

    UNION ALL
    SELECT activating_phone_matched_customer_id as customer_id --rename to union to customer_id
    FROM _vip_level_gamers_by_customer

    UNION ALL
    SELECT activating_cnh_matched_customer_id as customer_id
    FROM _vip_level_gamers_by_customer

    UNION ALL
    SELECT activating_defaddress_matched_customer_id as customer_id
    FROM _vip_level_gamers_by_customer

    UNION ALL
    SELECT activating_billingaddress_matched_customer_id as customer_id
    FROM _vip_level_gamers_by_customer

    UNION ALL
    SELECT repeat_billing_matched_customer_id as customer_id
    FROM _vip_level_gamers_by_customer

    UNION ALL
    SELECT repeat_cnh_matched_custimer_id as customer_id
    FROM _vip_level_gamers_by_customer
    ) AS A
WHERE customer_id IS NOT NULL;

-- SXF 5TH BILLING PERK OVERVIEW:
-- 5th Billing Perk: After every  5th billing during the VIPs active membership since August 2024, the VIP will be eligible for the free bra and undie pack for 90 days
-- 90 Days from Credit issue date
-- Billings must be during active membership - If VIP cancels and reactivates, the counter restarts
-- Aug 2024 = when SXF migrated to NMP (new membership program) and introduced new perk
-- EU is also eligible, no region/country exclusions
---- SXF NMP 5th Billing Perk BI Variables
-- 1. sxf_nmp_fifth_billing_perk_counter = # of successful billings since aug 2024
-- 2. sxf_nmp_fifth_billing_perk_flag = 1 if the VIP had their 5th successful billing, that billing was within the last 90 days, and they have not redeemed the perk promo code

-- Base List = List of all VIPs and most recent vip cohort month in CLVM with IS BOP Flag
-- Filter for Aug 2024 forward bc sxf 5th billing perk starts aug 2024
create or replace temp table _sxf_vips_latest_vip_cohort as
select clv.CUSTOMER_ID,
       max(clv.VIP_COHORT_MONTH_DATE) as LATEST_VIP_COHORT
from edw_prod.ANALYTICS_BASE.customer_lifetime_value_monthly clv
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE ds on ds.STORE_ID = clv.STORE_ID
where DS.STORE_BRAND_ABBR = 'SX'
and IS_BOP_VIP
and MONTH_DATE >= '2024-08-01'
group by 1
order by 1
;
-- for each VIP, list + count credits after vip cohort month where current status is not cancelled
create or replace temp table _sxf_successful_billing_credits_counter as
select distinct vip.customer_id
       , vip.LATEST_VIP_COHORT
       , dcr.CREDIT_ISSUED_LOCAL_DATETIME
       , row_number() over (partition by vip.CUSTOMER_ID, vip.LATEST_VIP_COHORT order by dcr.CREDIT_ISSUED_LOCAL_DATETIME asc) as billing_rnk
FROM _sxf_vips_latest_vip_cohort vip
JOIN EDW_PROD.DATA_MODEL_SXF.dim_credit dcr on concat(dcr.customer_id, '30') = vip.customer_id
JOIN EDW_PROD.DATA_MODEL_SXF.fact_credit_event fce on fce.credit_id = dcr.credit_id and dcr.credit_key = fce.credit_key
JOIN EDW_PROD.DATA_MODEL_SXF.dim_store ds on ds.store_id = dcr.store_id
WHERE ds.store_brand = 'Savage X'
AND ds.store_full_name not like '%(DM)%'
AND dcr.customer_id not in ('50362353130','87518041030')  -- exclude border free
and dcr.CREDIT_REPORT_MAPPING = 'Billed Credit'
--AND date(dcr.CREDIT_ISSUED_LOCAL_DATETIME) >= '2024-08-01'    -- sxf 5th billing perk count starts aug 2024
AND date(dcr.CREDIT_ISSUED_HQ_DATETIME) >= '2024-08-01'   -- sxf 5th billing perk count starts aug 2024
AND fce.credit_activity_type != 'Cancelled'       -- exclude cancelled credits
AND fce.is_current
AND dcr.CREDIT_ISSUED_LOCAL_DATETIME >= vip.LATEST_VIP_COHORT   -- credit issue date is after vip cohort - count only credits for most recent membership cohort
and dcr.CREDIT_REPORT_SUB_MAPPING = 'Billed Credit - Credit'  -- include only billed credits
group by 1,2,3
order by 1,2,3
;
-- SXF_Eligible_5th_Billing_Perk_Counter = for each vip, what is their current billing counter
create or replace temp table _sxf_nmp_fifth_billing_perk_counter_calc as
select distinct bc.customer_id
              , max(bc.billing_rnk) as sxf_nmp_fifth_billing_perk_counter
from _sxf_successful_billing_credits_counter bc
group by 1
;
-- customers who redeemed 5th billing perk promo and date of order datetime
-- promo code = 'EVERY5FREE'
create or replace temp table _sxf_redeemed_5th_billing_perk as
select distinct concat(customer_id, '30') as customer_id
              , fol.ORDER_LOCAL_DATETIME
FROM edw_prod.data_model_sxf.fact_order_line AS fol
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE st ON st.STORE_ID = fol.STORE_ID
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_ORDER_LINE_STATUS ols on ols.ORDER_LINE_STATUS_KEY = fol.ORDER_LINE_STATUS_KEY
                                                       and ols.ORDER_LINE_STATUS <> 'Cancelled'
join LAKE_SXF_VIEW.ULTRA_MERCHANT.ORDER_LINE_DISCOUNT d on ( fol.order_id = d.order_id
                                                             AND ( fol.order_line_id = d.order_line_id
                                                                OR fol.bundle_order_line_id = d.order_line_id ) )
join LAKE_SXF_VIEW.ULTRA_MERCHANT.PROMO pr ON (pr.promo_id = d.promo_id)
                                           and lower(pr.code) = lower('EVERY5FREE')
where st.STORE_BRAND = 'Savage X'
and fol.ORDER_LOCAL_DATETIME >= '2024-08-01'
;
-- SXF_Eligible_5th_Billing_Perk_Flag = from billing counter, list of vips who hit their 5th billing, issue date of billing was less than 90 days ago, and hasn't redeemed perk promo code after billing
-- Flag = 1 if vip is eligible. VIP does not exist in this table if not eligible
create or replace temp table _sxf_nmp_fifth_billing_perk_flag_calc  as
select distinct bcc.customer_id
            , 1 as sxf_nmp_fifth_billing_perk_flag
from _sxf_successful_billing_credits_counter bcc
left join _sxf_redeemed_5th_billing_perk rd on rd.customer_id = bcc.customer_id
                                           and rd.ORDER_LOCAL_DATETIME >= bcc.CREDIT_ISSUED_LOCAL_DATETIME  -- redeemed perk after billing date
where billing_rnk % 5 = 0   -- billing rank mod 5 = 0 = billing rank divided by 5 has no remainder
and datediff('day', CREDIT_ISSUED_LOCAL_DATETIME, current_date()) <= 90  -- 5th billing is within past 90 days
and rd.customer_id is null    -- customer has not redeemed promo code after 5th billing date
;
/* final variable logic - variables = 0 if no value
COALESCE(sxf_nmp_fifth_billing_perk_counter, 0),
COALESCE(sxf_nmp_fifth_billing_perk_flag, 0)
FROM _status_orders_billings AS b
JOIN edw_prod.data_model.dim_store AS ds ON ds.store_id = b.store_id
LEFT JOIN _sxf_nmp_fifth_billing_perk_counter_calc AS bpc ON b.customer_id = bpc.customer_id
LEFT JOIN _sxf_nmp_fifth_billing_perk_flag_calc AS bpf  ON b.customer_id = bpf.customer_id
*/

-- CRM BI Variable = Favorite Products
-- Variables to determine if a user has favorited products or not and if they do have favorites, what are the most recent two products?
-- 4 Variables Total:
-- 1. first_most_recent_favorited_item_site_name
-- 2. first_most_recent_favorited_item_image_url
-- 3. second_most_recent_favorited_item_site_name
-- 4. second_most_recent_favorited_item_image_url

-- Pull Image URL for every product alias where image url is not Unknown
create or replace temp table  _product_image_urls as
SELECT distinct  product_alias
              , image_url
FROM edw_prod.data_model.dim_product AS p
JOIN edw_prod.data_model.dim_store AS st on st.store_id = p.store_id
WHERE  p.IS_ACTIVE
AND IMAGE_URL <> 'Unknown'
qualify row_number() over (partition by st.store_brand, p.product_alias order by p.master_product_last_update_datetime desc) = 1  --bring in one image url per product
;
-- for each customer, pull most recent two favorited products
create or replace temp table _favorited_products_all_ as
select m.CUSTOMER_ID,
dp.product_ID,
dp.product_name,
dp.product_alias,
img.image_url,
mp.DATETIME_ADDED,
row_number() over (partition by m.CUSTOMER_ID order by mp.DATETIME_ADDED desc) as rank
from lake_consolidated_view.ULTRA_MERCHANT.MEMBERSHIP_PRODUCT mp
join lake_consolidated_view.ULTRA_MERCHANT.MEMBERSHIP m on m.MEMBERSHIP_ID = mp.MEMBERSHIP_ID
join edw_prod.data_model.dim_product dp on dp.product_id = mp.PRODUCT_ID
join _product_image_urls img on img.product_alias = dp.product_alias
where mp.active = 1
group by 1,2,3,4,5,6
qualify row_number() over (partition by m.CUSTOMER_ID order by mp.DATETIME_ADDED desc) <= 2  --bring in most 2 most recent products
;
-- 1. first_most_recent_favorited_item_site_name
-- 2. first_most_recent_favorited_item_image_url
-- 3. second_most_recent_favorited_item_site_name
-- 4. second_most_recent_favorited_item_image_url
-- for each customer, put most recent two favorited products in one row
create or replace temp table _customer_favorited_items_ as
select distinct CUSTOMER_ID,
        max(iff(rank = 1, product_name, null)) as first_most_recent_favorited_item_site_name,
        max(iff(rank = 1, image_url, null)) as first_most_recent_favorited_item_image_url,
        max(iff(rank = 2, product_name, null)) as second_most_recent_favorited_item_site_name,
        max(iff(rank = 2, image_url, null)) as second_most_recent_favorited_item_image_url
from _favorited_products_all_
group by 1
;
/* final variable logic - variables = 0 if no value
COALESCE(fav.first_most_recent_favorited_item_site_name, 0) as first_most_recent_favorited_item_site_name,
COALESCE(fav.first_most_recent_favorited_item_image_url, 0) as first_most_recent_favorited_item_image_url,
COALESCE(fav.second_most_recent_favorited_item_site_name, 0) as second_most_recent_favorited_item_site_name,
COALESCE(fav.second_most_recent_favorited_item_image_url, 0) as second_most_recent_favorited_item_image_url
FROM _status_orders_billings AS b
JOIN edw_prod.data_model.dim_store AS ds ON ds.store_id = b.store_id
LEFT JOIN _customer_favorited_items_ AS fav ON fav.customer_id = b.customer_id
*/

CREATE OR REPLACE TEMP TABLE _delta AS
SELECT DISTINCT
    b.customer_id,
    ds.store_brand || '_' || ds.store_country AS store,
    b.store_group,
    ebv.PROMO_GROUP,
    ebv.failed_biller_segment,
    ebv.skipper_segment,
    ebv.cancelled_vip_segment,
/* */
    b.lead_registration_datetime,
    b.activation_local_date,
    b.cancel_local_date,
    b.registration_source_medium_channel,
    b.activation_source_medium_channel,
    b.mobile_vip_activation_flag AS is_activated_on_mobile_app,
    ct.timezone,

/* membership status fields */
    b.vip_lifetime_saving,
    b.avg_days_between_purchase,
    b.last_placed_date,
    COALESCE(b.shopped_retail_flag, 0)                                  AS is_retail_shopper,
    COALESCE(b.shopped_online_flag, 0)                                  AS is_online_shopper,
    COALESCE(b.shopped_mobile_app_flag, 0)                              AS is_mobile_app_shopper,
    COALESCE(b.shopped_female_product, 0)                               AS is_female_product_shopper,
    COALESCE(b.shopped_male_product, 0)                                 AS is_male_product_shopper,
    COALESCE(b.shopped_unisex_product, 0)                               AS is_unisex_product_shopper,
    COALESCE(b.shopped_male_product_only, 0)                            AS is_male_product_only_shopper,
    COALESCE(TO_DATE(b.last_retail_purchase_date), '1900-01-01')        AS last_retail_purchase_date,
    COALESCE(TO_DATE(b.last_mobile_app_purchase_date), '1900-01-01')    AS last_mobile_app_purchase_date,
    COALESCE(TO_DATE(b.last_online_purchase_date), '1900-01-01')        AS last_online_purchase_date,
    COALESCE(TO_DATE(b.last_product_order_purchase_date), '1900-01-01') AS last_product_order_purchase_date,
    b.nearest_retail_store,
    b.retail_store_distance,
    b.retail_store_drive_time,
    b.nearest_retail_store_address1,
    b.nearest_retail_store_address2,
    b.nearest_retail_store_city,
    b.nearest_retail_store_state,
    b.nearest_retail_store_zip,
    b.nearest_retail_store_country_code,
    b.nearest_retail_store_google_maps_link,
    b.nearest_retail_store_location_in_mall,
    b.skip_reason,
    b.Is_In_Grace_Period,
    b.Activating_UPT,
    b.purchase_mens_only_activating_order,
    b.pre_passive_cancel_2m_check,

/* order metrics fields */
    b.current_cash_margin_decile,
    b.current_product_margin_decile,
    b.ltv_order_revenue,
    b.ltv_total_revenue,
    b.product_orders,
    b.credit_billings,
    b.ltv_cash_gross_margin,
    b.ltv_gross_margin_pct,
    b.avg_aov,
    b.recent_customer_action,
    b.recent_bill_me_now_date,
    b.previous_decile,
    b.membership_amount_saved_to_date,
    b.membership_repeat_failed_billing_flag                             AS is_repeat_failed_billing,
    b.consecutive_failed_billing_months,
    b.emp_enrollment_flag                                               AS is_emp_enrolled,
    opt.is_new_TCs_opt_in,
    EO.IS_EMP_OPTIN,

/* credit and ltv metrics */
    IFF(adf.customer_id IS NOT NULL, 1, 0)                              AS is_brand_app_downloaded,
    IFF(fad.customer_id IS NOT NULL, 1, 0)                              AS is_fitness_app_downloaded,
    IFF(hp.customer_id IS NOT NULL, 1, 0)                               AS is_hydrow_purchased,
    ssc.sx_bra_size                                                     AS sx_unique_top_size,
    ssc.sx_undie_size                                                   AS sx_unique_bottom_size,
    h.how_did_you_hear,
    mc.membership_2995usd_credits,
    mus.membership_3995usd_credits,
    most_recent_successful_credit_billing,
    Redeemed_Downgraded_Promo,
    IFF(sir.name = 'scrubs_interest_yes', 1, IFF(sir.name = 'scrubs_interest_no', 0, NULL))
        AS scrubs_interest_response,
    SXF_Lifetime_Sub_Department,
    SXF_Activating_Sub_Department,
    SXF_Lifetime_Sub_Department_without_Gender,
    SXF_Activating_Sub_Department_without_Gender,
    SXF_Consecutive_Failed_Billing_Months,
    SXF_Is_Failed_Billing,
    SXF_On_Track_Passive_Cancel_Criteria_Months,
    SXF_Total_Failed_Billing_Months_InPastYear,
    SXF_Is_First_Time_Failed_Billing,
    IFF(sxgmr.customer_id IS NOT NULL, 1, 0) AS sxf_vip_gamer,
/* customer details */
    CASE
        WHEN activation_local_date > '1900-01-01' THEN 'vip'
        WHEN activation_local_date = '1900-01-01' AND last_product_order_purchase_date > '1900-01-01' THEN 'payg'
        ELSE 'lead' END
    AS current_status,
    COALESCE(cb.IS_BIRTH_MONTH, 0) AS IS_BIRTH_MONTH,
    try_to_date(h.birth_year||'-'||h.birth_month||'-'||h.birth_day)  AS BIRTH_DAY,
    COALESCE(snfb.sxf_nmp_fifth_billing_perk_flag,0) as sxf_nmp_fifth_billing_perk_flag,
    first_most_recent_favorited_item_site_name,
    first_most_recent_favorited_item_image_url,
    second_most_recent_favorited_item_site_name,
    second_most_recent_favorited_item_image_url,
    sxf_nmp_fifth_billing_perk_counter,
    -- IS_ANNIVERSARY_MONTH variable to flag if activation date was in current month
    CASE
        WHEN b.activation_local_date <> '1900-01-01' -- activation date is a real date
            AND CURRENT_DATE <> b.activation_local_date -- activation date is not today / anniversary is first year after activation onward
            AND MONTH(b.activation_local_date) = MONTH(CURRENT_DATE) AND DAY(b.activation_local_date) = DAY(CURRENT_DATE) -- activation month = current month and activation day = current day to bring in anniversary regardless of year
        THEN 1
        ELSE 0
        END IS_ANNIVERSARY_TODAY,
    CASE
        WHEN b.activation_local_date <> '1900-01-01' -- activation date is a real date
            AND CURRENT_DATE <> b.activation_local_date -- activation date is not today / anniversary is first year after activation onward
            AND MONTH(b.activation_local_date) = MONTH(CURRENT_DATE) -- activation month = current month to bring in anniversary regardless of year
        THEN 1
        ELSE 0
        END IS_ANNIVERSARY_MONTH
FROM _status_orders_billings AS b
JOIN edw_prod.data_model.dim_store AS ds
    ON ds.store_id = b.store_id
LEFT JOIN _fitness_app_download AS fad
    ON b.customer_id = fad.customer_id
LEFT JOIN _brand_app_downloads_flag AS adf
    ON b.customer_id = adf.customer_id
LEFT JOIN _sxf_sizechart AS ssc
    ON ssc.customer_id = b.customer_id
LEFT JOIN _hydrow_purchaser AS hp
    ON adf.customer_id = hp.customer_id
LEFT JOIN _hdyh AS h
    ON b.customer_id = h.customer_id
LEFT JOIN _membership_2995usd_credits AS mc
    ON b.customer_id = mc.customer_id
LEFT JOIN _membership_3995usd_credits AS mus
    ON b.customer_id = mus.customer_id
LEFT JOIN _most_recent_successful_credit_billing AS mrscb
    ON mrscb.customer_id = b.customer_id
LEFT JOIN _redeemed_downgraded_prom AS rdp
    ON rdp.customer_id = b.customer_id
LEFT JOIN _scrubs_interest_response AS sir
    ON sir.customer_id = b.customer_id
LEFT JOIN _Activating_Lifetime_Purchase_SubDepts AS alps
    ON alps.customer_id = b.customer_id
LEFT JOIN _SX_failed_billers AS sfb
    ON sfb.customer_id = b.customer_id
LEFT JOIN _Customer_timezone AS ct
    ON ct.customer_id = b.customer_id
LEFT JOIN reporting_prod.fabletics.crm_bi_variables AS ebv
    ON edw_prod.stg.udf_unconcat_brand(b.customer_id) = ebv.customer_id
LEFT JOIN _sxf_vip_gamers AS sxgmr
    ON sxgmr.customer_id = b.customer_id
LEFT JOIN _is_new_TCs_opt_in opt
    ON opt.customer_id=b.customer_id
LEFT JOIN _EMP_OPTIN EO
    on EO.customer_id=b.customer_id
LEFT JOIN _cust_bday cb   --- New birthday var
    on cb.customer_id = b.customer_id
LEFT JOIN _sxf_nmp_fifth_billing_perk_flag_calc AS snfb
    ON snfb.customer_id = b.customer_id
LEFT JOIN _customer_favorited_items_ AS cfi
    ON cfi.customer_id = b.customer_id
LEFT JOIN _sxf_nmp_fifth_billing_perk_counter_calc AS b_counter
    ON b_counter.customer_id = b.customer_id
;

/* Excluded Customers need to be flagged with no other changes */
UPDATE lake.crm.bi_data AS A
SET
    A.EXCLUSION_REASON = XR.EXCLUSION_REASON,
    A.CUSTOMER_BI_UPDATED_DATETIME = CURRENT_TIMESTAMP()
FROM _exclusion_reason AS XR
WHERE A.customer_id = XR.customer_id
    AND COALESCE(A.EXCLUSION_REASON, '') <> COALESCE(XR.EXCLUSION_REASON, '');

UPDATE lake.crm.bi_data t
    SET IS_BIRTH_MONTH = 0,
        CUSTOMER_BI_UPDATED_DATETIME = CURRENT_TIMESTAMP()
WHERE t.IS_BIRTH_MONTH = 1
    and t.customer_id not in (select customer_id from _cust_bday)
;

UPDATE lake.crm.bi_data t
    SET IS_ANNIVERSARY_TODAY = 0,
        CUSTOMER_BI_UPDATED_DATETIME = CURRENT_TIMESTAMP()
FROM edw_prod.data_model.fact_membership_event fme
where fme.customer_id = t.customer_id
    and fme.is_current = 1
    and CASE
            WHEN fme.event_start_local_datetime::date <> '1900-01-01' -- activation date is a real date
                AND CURRENT_DATE <> fme.event_start_local_datetime::date -- activation date is not today / anniversary is first year after activation onward
                AND MONTH(fme.event_start_local_datetime::date) = MONTH(CURRENT_DATE) AND DAY(fme.event_start_local_datetime::date) = DAY(CURRENT_DATE) -- activation month = current month and activation day = current day to bring in anniversary regardless of year
            THEN 1
            ELSE 0 END = 0
    and t.IS_ANNIVERSARY_TODAY = 1
    and fme.membership_event_type in ('Failed Activation from Classic VIP', 'Activation')
;


UPDATE lake.crm.bi_data t
    SET IS_ANNIVERSARY_MONTH = 0,
        CUSTOMER_BI_UPDATED_DATETIME = CURRENT_TIMESTAMP()
FROM edw_prod.data_model.fact_membership_event fme
where fme.customer_id = t.customer_id
    and fme.is_current = 1
    and CASE
        WHEN fme.event_start_local_datetime::date <> '1900-01-01' -- activation date is a real date
            AND CURRENT_DATE <> fme.event_start_local_datetime::date -- activation date is not today / anniversary is first year after activation onward
            AND MONTH(fme.event_start_local_datetime::date) = MONTH(CURRENT_DATE) -- activation month = current month to bring in anniversary regardless of year
        THEN 1
        ELSE 0 END = 0
    and t.IS_ANNIVERSARY_MONTH = 1
    and fme.membership_event_type in ('Failed Activation from Classic VIP', 'Activation')
;


MERGE INTO lake.crm.bi_data AS T
USING _delta AS S
ON T.customer_id = S.customer_id
WHEN MATCHED AND (
    COALESCE(S.STORE, '') <> COALESCE(T.STORE, '')
    OR COALESCE(S.STORE_GROUP, '') <> COALESCE(T.STORE_GROUP, '')
    OR COALESCE(S.PROMO_GROUP, '') <> COALESCE(T.PROMO_GROUP, '')
    OR COALESCE(S.LEAD_REGISTRATION_DATETIME, '1900-01-01') <> COALESCE(T.LEAD_REGISTRATION_DATETIME, '1900-01-01')
    OR COALESCE(S.ACTIVATION_LOCAL_DATE, '1900-01-01') <> COALESCE(T.ACTIVATION_LOCAL_DATE, '1900-01-01')
    OR COALESCE(S.CANCEL_LOCAL_DATE, '1900-01-01') <> COALESCE(T.CANCEL_LOCAL_DATE, '1900-01-01')
    OR COALESCE(S.REGISTRATION_SOURCE_MEDIUM_CHANNEL, '') <> COALESCE(T.REGISTRATION_SOURCE_MEDIUM_CHANNEL, '')
    OR COALESCE(S.ACTIVATION_SOURCE_MEDIUM_CHANNEL, '') <> COALESCE(T.ACTIVATION_SOURCE_MEDIUM_CHANNEL, '')
    OR COALESCE(S.IS_ACTIVATED_ON_MOBILE_APP, -1) <> COALESCE(T.IS_ACTIVATED_ON_MOBILE_APP, -1)
    OR COALESCE(S.TIMEZONE, '') <> COALESCE(T.TIMEZONE, '')
    OR COALESCE(S.VIP_LIFETIME_SAVING, -1) <> COALESCE(T.VIP_LIFETIME_SAVING, -1)
    OR COALESCE(S.AVG_DAYS_BETWEEN_PURCHASE, -1) <> COALESCE(T.AVG_DAYS_BETWEEN_PURCHASE, -1)
    OR COALESCE(S.LAST_PLACED_DATE, '1900-01-01') <> COALESCE(T.LAST_PLACED_DATE, '1900-01-01')
    OR COALESCE(S.IS_RETAIL_SHOPPER, -1) <> COALESCE(T.IS_RETAIL_SHOPPER, -1)
    OR COALESCE(S.IS_ONLINE_SHOPPER, -1) <> COALESCE(T.IS_ONLINE_SHOPPER, -1)
    OR COALESCE(S.IS_MOBILE_APP_SHOPPER, -1) <> COALESCE(T.IS_MOBILE_APP_SHOPPER, -1)
    OR COALESCE(S.IS_FEMALE_PRODUCT_SHOPPER, -1) <> COALESCE(T.IS_FEMALE_PRODUCT_SHOPPER, -1)
    OR COALESCE(S.IS_MALE_PRODUCT_SHOPPER, -1) <> COALESCE(T.IS_MALE_PRODUCT_SHOPPER, -1)
    OR COALESCE(S.IS_UNISEX_PRODUCT_SHOPPER, -1) <> COALESCE(T.IS_UNISEX_PRODUCT_SHOPPER, -1)
    OR COALESCE(S.IS_MALE_PRODUCT_ONLY_SHOPPER, -1) <> COALESCE(T.IS_MALE_PRODUCT_ONLY_SHOPPER, -1)
    OR COALESCE(S.LAST_RETAIL_PURCHASE_DATE, '1900-01-01') <> COALESCE(T.LAST_RETAIL_PURCHASE_DATE, '1900-01-01')
    OR COALESCE(S.LAST_MOBILE_APP_PURCHASE_DATE, '1900-01-01')
           <> COALESCE(T.LAST_MOBILE_APP_PURCHASE_DATE, '1900-01-01')
    OR COALESCE(S.LAST_ONLINE_PURCHASE_DATE, '1900-01-01') <> COALESCE(T.LAST_ONLINE_PURCHASE_DATE, '1900-01-01')
    OR COALESCE(S.LAST_PRODUCT_ORDER_PURCHASE_DATE, '1900-01-01')
           <> COALESCE(T.LAST_PRODUCT_ORDER_PURCHASE_DATE, '1900-01-01')
    OR COALESCE(S.NEAREST_RETAIL_STORE, '') <> COALESCE(T.NEAREST_RETAIL_STORE, '')
    OR COALESCE(S.RETAIL_STORE_DISTANCE, -1) <> COALESCE(T.RETAIL_STORE_DISTANCE, -1)
    OR COALESCE(S.RETAIL_STORE_DRIVE_TIME, -1) <> COALESCE(T.RETAIL_STORE_DRIVE_TIME, -1)
    OR COALESCE(S.NEAREST_RETAIL_STORE_ADDRESS1, '') <> COALESCE(T.NEAREST_RETAIL_STORE_ADDRESS1, '')
    OR COALESCE(S.NEAREST_RETAIL_STORE_ADDRESS2, '') <> COALESCE(T.NEAREST_RETAIL_STORE_ADDRESS2, '')
    OR COALESCE(S.NEAREST_RETAIL_STORE_CITY, '') <> COALESCE(T.NEAREST_RETAIL_STORE_CITY, '')
    OR COALESCE(S.NEAREST_RETAIL_STORE_STATE, '') <> COALESCE(T.NEAREST_RETAIL_STORE_STATE, '')
    OR COALESCE(S.NEAREST_RETAIL_STORE_ZIP, '') <> COALESCE(T.NEAREST_RETAIL_STORE_ZIP, '')
    OR COALESCE(S.NEAREST_RETAIL_STORE_COUNTRY_CODE, '') <> COALESCE(T.NEAREST_RETAIL_STORE_COUNTRY_CODE, '')
    OR COALESCE(S.NEAREST_RETAIL_STORE_GOOGLE_MAPS_LINK, '') <> COALESCE(T.NEAREST_RETAIL_STORE_GOOGLE_MAPS_LINK, '')
    OR COALESCE(S.NEAREST_RETAIL_STORE_LOCATION_IN_MALL, '') <> COALESCE(T.NEAREST_RETAIL_STORE_LOCATION_IN_MALL, '')
    OR COALESCE(S.SKIP_REASON, '')<>COALESCE(T.SKIP_REASON, '')
    OR COALESCE(S.IS_IN_GRACE_PERIOD, -1)<>COALESCE(T.IS_IN_GRACE_PERIOD, -1)
    OR COALESCE(S.ACTIVATING_UPT, '') <> COALESCE(T.ACTIVATING_UPT, '')
    OR COALESCE(S.PURCHASE_MENS_ONLY_ACTIVATING_ORDER, -1) <> COALESCE(T.PURCHASE_MENS_ONLY_ACTIVATING_ORDER, -1)
    OR COALESCE(S.PRE_PASSIVE_CANCEL_2M_CHECK, -1) <> COALESCE(T.PRE_PASSIVE_CANCEL_2M_CHECK, -1)
    OR COALESCE(S.CURRENT_CASH_MARGIN_DECILE, -1) <> COALESCE(T.CURRENT_CASH_MARGIN_DECILE, -1)
    OR COALESCE(S.CURRENT_PRODUCT_MARGIN_DECILE, -1) <> COALESCE(T.CURRENT_PRODUCT_MARGIN_DECILE, -1)
    OR COALESCE(S.LTV_ORDER_REVENUE, -1) <> COALESCE(T.LTV_ORDER_REVENUE, -1)
    OR COALESCE(S.LTV_TOTAL_REVENUE, -1) <> COALESCE(T.LTV_TOTAL_REVENUE, -1)
    OR COALESCE(S.PRODUCT_ORDERS, -1) <> COALESCE(T.PRODUCT_ORDERS, -1)
    OR COALESCE(S.CREDIT_BILLINGS, -1) <> COALESCE(T.CREDIT_BILLINGS, -1)
    OR COALESCE(S.LTV_CASH_GROSS_MARGIN, -1) <> COALESCE(T.LTV_CASH_GROSS_MARGIN, -1)
    OR COALESCE(S.LTV_GROSS_MARGIN_PCT, -1) <> COALESCE(T.LTV_GROSS_MARGIN_PCT, -1)
    OR COALESCE(S.AVG_AOV, -1) <> COALESCE(T.AVG_AOV, -1)
    OR COALESCE(S.RECENT_CUSTOMER_ACTION, '') <> COALESCE(T.RECENT_CUSTOMER_ACTION, '')
    OR COALESCE(S.RECENT_BILL_ME_NOW_DATE, '1900-01-01') <> COALESCE(T.RECENT_BILL_ME_NOW_DATE, '1900-01-01')
    OR COALESCE(S.PREVIOUS_DECILE, -1) <> COALESCE(T.PREVIOUS_DECILE, -1)
    OR COALESCE(S.MEMBERSHIP_AMOUNT_SAVED_TO_DATE, -1) <> COALESCE(T.MEMBERSHIP_AMOUNT_SAVED_TO_DATE, -1)
    OR COALESCE(S.IS_REPEAT_FAILED_BILLING, -1) <> COALESCE(T.IS_REPEAT_FAILED_BILLING, -1)
    OR COALESCE(S.CONSECUTIVE_FAILED_BILLING_MONTHS, -1) <> COALESCE(T.CONSECUTIVE_FAILED_BILLING_MONTHS, -1)
    OR COALESCE(S.IS_EMP_ENROLLED, -1) <> COALESCE(T.IS_EMP_ENROLLED, -1)
    OR COALESCE(S.IS_BRAND_APP_DOWNLOADED, -1) <> COALESCE(T.IS_BRAND_APP_DOWNLOADED, -1)
    OR COALESCE(S.IS_FITNESS_APP_DOWNLOADED, -1) <> COALESCE(T.IS_FITNESS_APP_DOWNLOADED, -1)
    OR COALESCE(S.IS_HYDROW_PURCHASED, -1) <> COALESCE(T.IS_HYDROW_PURCHASED, -1)
    OR COALESCE(S.SX_UNIQUE_TOP_SIZE, '') <> COALESCE(T.SX_UNIQUE_TOP_SIZE, '')
    OR COALESCE(S.SX_UNIQUE_BOTTOM_SIZE, '') <> COALESCE(T.SX_UNIQUE_BOTTOM_SIZE, '')
    OR COALESCE(S.HOW_DID_YOU_HEAR, '') <> COALESCE(T.HOW_DID_YOU_HEAR, '')
    OR COALESCE(S.MEMBERSHIP_2995USD_CREDITS, -1) <> COALESCE(T.MEMBERSHIP_2995USD_CREDITS, -1)
    OR COALESCE(S.MEMBERSHIP_3995USD_CREDITS, -1) <> COALESCE(T.MEMBERSHIP_3995USD_CREDITS, -1)
    OR COALESCE(S.MOST_RECENT_SUCCESSFUL_CREDIT_BILLING, '1900-01-01') <> COALESCE(T.MOST_RECENT_SUCCESSFUL_CREDIT_BILLING, '1900-01-01')
    OR COALESCE(S.REDEEMED_DOWNGRADED_PROMO, -1) <> COALESCE(T.REDEEMED_DOWNGRADED_PROMO, -1)
    OR COALESCE(S.SCRUBS_INTEREST_RESPONSE, -1) <> COALESCE(T.SCRUBS_INTEREST_RESPONSE, -1)
    OR COALESCE(S.SXF_LIFETIME_SUB_DEPARTMENT, '') <> COALESCE(T.SXF_LIFETIME_SUB_DEPARTMENT, '')
    OR COALESCE(S.SXF_ACTIVATING_SUB_DEPARTMENT, '') <> COALESCE(T.SXF_ACTIVATING_SUB_DEPARTMENT, '')
    OR COALESCE(S.SXF_LIFETIME_SUB_DEPARTMENT_WITHOUT_GENDER, '') <> COALESCE(T.SXF_LIFETIME_SUB_DEPARTMENT_WITHOUT_GENDER, '')
    OR COALESCE(S.SXF_ACTIVATING_SUB_DEPARTMENT_WITHOUT_GENDER, '') <> COALESCE(T.SXF_ACTIVATING_SUB_DEPARTMENT_WITHOUT_GENDER, '')
    OR COALESCE(S.SXF_CONSECUTIVE_FAILED_BILLING_MONTHS, -1) <> COALESCE(T.SXF_CONSECUTIVE_FAILED_BILLING_MONTHS, -1)
    OR COALESCE(S.SXF_IS_FAILED_BILLING, -1) <> COALESCE(T.SXF_IS_FAILED_BILLING, -1)
    OR COALESCE(S.SXF_ON_TRACK_PASSIVE_CANCEL_CRITERIA_MONTHS, -1) <> COALESCE(T.SXF_ON_TRACK_PASSIVE_CANCEL_CRITERIA_MONTHS, -1)
    OR COALESCE(S.SXF_TOTAL_FAILED_BILLING_MONTHS_INPASTYEAR, -1) <> COALESCE(T.SXF_TOTAL_FAILED_BILLING_MONTHS_INPASTYEAR, -1)
    OR COALESCE(S.SXF_IS_FIRST_TIME_FAILED_BILLING, -1) <> COALESCE(T.SXF_IS_FIRST_TIME_FAILED_BILLING, -1)
    OR COALESCE(S.SXF_VIP_GAMER, -1) <> COALESCE(T.SXF_VIP_GAMER, -1)
    OR COALESCE(S.CURRENT_STATUS, '') <> COALESCE(T.CURRENT_STATUS, '')
    OR COALESCE(s.is_new_TCs_opt_in,-1) <> COALESCE(T.is_new_TCs_opt_in,-1)
    OR COALESCE(s.IS_EMP_OPTIN,-1) <> COALESCE(T.IS_EMP_OPTIN,-1)
    OR COALESCE(S.FAILED_BILLER_SEGMENT, '') <> COALESCE(T.FAILED_BILLER_SEGMENT, '')  -- fl specific column
    OR COALESCE(S.SKIPPER_SEGMENT, '') <> COALESCE(T.SKIPPER_SEGMENT, '')              -- fl specific column
    OR COALESCE(S.CANCELLED_VIP_SEGMENT, '') <> COALESCE(T.CANCELLED_VIP_SEGMENT, '')  -- fl specific column
    OR COALESCE(s.IS_BIRTH_MONTH,-1) <> COALESCE(T.IS_BIRTH_MONTH,-1)
    OR COALESCE(s.BIRTH_DAY,'1900-01-01') <> COALESCE(T.BIRTH_DAY,'1900-01-01')
    OR COALESCE(S.IS_ANNIVERSARY_TODAY,0) <> COALESCE(T.IS_ANNIVERSARY_TODAY,0)
    OR COALESCE(S.IS_ANNIVERSARY_MONTH,0) <> COALESCE(T.IS_ANNIVERSARY_MONTH,0)
    OR COALESCE(S.SXF_NMP_FIFTH_BILLING_PERK_FLAG,0) <> COALESCE(T.SXF_NMP_FIFTH_BILLING_PERK_FLAG,0)
    OR COALESCE(S.FIRST_MOST_RECENT_FAVORITED_ITEM_SITE_NAME, '') <> COALESCE(T.FIRST_MOST_RECENT_FAVORITED_ITEM_SITE_NAME, '')
    OR COALESCE(S.FIRST_MOST_RECENT_FAVORITED_ITEM_IMAGE_URL, '') <> COALESCE(T.FIRST_MOST_RECENT_FAVORITED_ITEM_IMAGE_URL, '')
    OR COALESCE(S.SECOND_MOST_RECENT_FAVORITED_ITEM_SITE_NAME, '') <> COALESCE(T.SECOND_MOST_RECENT_FAVORITED_ITEM_SITE_NAME, '')
    OR COALESCE(S.SECOND_MOST_RECENT_FAVORITED_ITEM_IMAGE_URL, '') <> COALESCE(T.SECOND_MOST_RECENT_FAVORITED_ITEM_IMAGE_URL, '')
    OR COALESCE(S.SXF_NMP_FIFTH_BILLING_PERK_COUNTER, 0) <> COALESCE(T.SXF_NMP_FIFTH_BILLING_PERK_COUNTER, 0)
)
THEN UPDATE SET
    T.STORE = S.STORE,
    T.STORE_GROUP = S.STORE_GROUP,
    T.PROMO_GROUP = S.PROMO_GROUP,
    T.LEAD_REGISTRATION_DATETIME = S.LEAD_REGISTRATION_DATETIME,
    T.ACTIVATION_LOCAL_DATE = S.ACTIVATION_LOCAL_DATE,
    T.CANCEL_LOCAL_DATE = S.CANCEL_LOCAL_DATE,
    T.REGISTRATION_SOURCE_MEDIUM_CHANNEL = S.REGISTRATION_SOURCE_MEDIUM_CHANNEL,
    T.ACTIVATION_SOURCE_MEDIUM_CHANNEL = S.ACTIVATION_SOURCE_MEDIUM_CHANNEL,
    T.IS_ACTIVATED_ON_MOBILE_APP = S.IS_ACTIVATED_ON_MOBILE_APP,
    T.TIMEZONE = S.TIMEZONE,
    T.VIP_LIFETIME_SAVING = S.VIP_LIFETIME_SAVING,
    T.AVG_DAYS_BETWEEN_PURCHASE = S.AVG_DAYS_BETWEEN_PURCHASE,
    T.LAST_PLACED_DATE = S.LAST_PLACED_DATE,
    T.IS_RETAIL_SHOPPER = S.IS_RETAIL_SHOPPER,
    T.IS_ONLINE_SHOPPER = S.IS_ONLINE_SHOPPER,
    T.IS_MOBILE_APP_SHOPPER = S.IS_MOBILE_APP_SHOPPER,
    T.IS_FEMALE_PRODUCT_SHOPPER = S.IS_FEMALE_PRODUCT_SHOPPER,
    T.IS_MALE_PRODUCT_SHOPPER = S.IS_MALE_PRODUCT_SHOPPER,
    T.IS_UNISEX_PRODUCT_SHOPPER = S.IS_UNISEX_PRODUCT_SHOPPER,
    T.IS_MALE_PRODUCT_ONLY_SHOPPER = S.IS_MALE_PRODUCT_ONLY_SHOPPER,
    T.LAST_RETAIL_PURCHASE_DATE = S.LAST_RETAIL_PURCHASE_DATE,
    T.LAST_MOBILE_APP_PURCHASE_DATE = S.LAST_MOBILE_APP_PURCHASE_DATE,
    T.LAST_ONLINE_PURCHASE_DATE = S.LAST_ONLINE_PURCHASE_DATE,
    T.LAST_PRODUCT_ORDER_PURCHASE_DATE = S.LAST_PRODUCT_ORDER_PURCHASE_DATE,
    T.NEAREST_RETAIL_STORE = S.NEAREST_RETAIL_STORE,
    T.RETAIL_STORE_DISTANCE = S.RETAIL_STORE_DISTANCE,
    T.RETAIL_STORE_DRIVE_TIME = S.RETAIL_STORE_DRIVE_TIME,
    T.NEAREST_RETAIL_STORE_ADDRESS1 = S.NEAREST_RETAIL_STORE_ADDRESS1,
    T.NEAREST_RETAIL_STORE_ADDRESS2 = S.NEAREST_RETAIL_STORE_ADDRESS2,
    T.NEAREST_RETAIL_STORE_CITY = S.NEAREST_RETAIL_STORE_CITY,
    T.NEAREST_RETAIL_STORE_STATE = S.NEAREST_RETAIL_STORE_STATE,
    T.NEAREST_RETAIL_STORE_ZIP = S.NEAREST_RETAIL_STORE_ZIP,
    T.NEAREST_RETAIL_STORE_COUNTRY_CODE = S.NEAREST_RETAIL_STORE_COUNTRY_CODE,
    T.NEAREST_RETAIL_STORE_GOOGLE_MAPS_LINK = S.NEAREST_RETAIL_STORE_GOOGLE_MAPS_LINK,
    T.NEAREST_RETAIL_STORE_LOCATION_IN_MALL = S.NEAREST_RETAIL_STORE_LOCATION_IN_MALL,
    T.SKIP_REASON = S.SKIP_REASON,
    T.IS_IN_GRACE_PERIOD = S.IS_IN_GRACE_PERIOD,
    T.ACTIVATING_UPT = S.ACTIVATING_UPT,
    T.PURCHASE_MENS_ONLY_ACTIVATING_ORDER = S.PURCHASE_MENS_ONLY_ACTIVATING_ORDER,
    T.PRE_PASSIVE_CANCEL_2M_CHECK = S.PRE_PASSIVE_CANCEL_2M_CHECK,
    T.CURRENT_CASH_MARGIN_DECILE = S.CURRENT_CASH_MARGIN_DECILE,
    T.CURRENT_PRODUCT_MARGIN_DECILE = S.CURRENT_PRODUCT_MARGIN_DECILE,
    T.LTV_ORDER_REVENUE = S.LTV_ORDER_REVENUE,
    T.LTV_TOTAL_REVENUE = S.LTV_TOTAL_REVENUE,
    T.PRODUCT_ORDERS = S.PRODUCT_ORDERS,
    T.CREDIT_BILLINGS = S.CREDIT_BILLINGS,
    T.LTV_CASH_GROSS_MARGIN = S.LTV_CASH_GROSS_MARGIN,
    T.LTV_GROSS_MARGIN_PCT = S.LTV_GROSS_MARGIN_PCT,
    T.AVG_AOV = S.AVG_AOV,
    T.RECENT_CUSTOMER_ACTION = S.RECENT_CUSTOMER_ACTION,
    T.RECENT_BILL_ME_NOW_DATE = S.RECENT_BILL_ME_NOW_DATE,
    T.PREVIOUS_DECILE = S.PREVIOUS_DECILE,
    T.MEMBERSHIP_AMOUNT_SAVED_TO_DATE = S.MEMBERSHIP_AMOUNT_SAVED_TO_DATE,
    T.IS_REPEAT_FAILED_BILLING = S.IS_REPEAT_FAILED_BILLING,
    T.CONSECUTIVE_FAILED_BILLING_MONTHS = S.CONSECUTIVE_FAILED_BILLING_MONTHS,
    T.IS_EMP_ENROLLED = S.IS_EMP_ENROLLED,
    T.IS_BRAND_APP_DOWNLOADED = S.IS_BRAND_APP_DOWNLOADED,
    T.IS_FITNESS_APP_DOWNLOADED = S.IS_FITNESS_APP_DOWNLOADED,
    T.IS_HYDROW_PURCHASED = S.IS_HYDROW_PURCHASED,
    T.SX_UNIQUE_TOP_SIZE = S.SX_UNIQUE_TOP_SIZE,
    T.SX_UNIQUE_BOTTOM_SIZE = S.SX_UNIQUE_BOTTOM_SIZE,
    T.HOW_DID_YOU_HEAR = S.HOW_DID_YOU_HEAR,
    T.MEMBERSHIP_2995USD_CREDITS = S.MEMBERSHIP_2995USD_CREDITS,
    T.MEMBERSHIP_3995USD_CREDITS = S.MEMBERSHIP_3995USD_CREDITS,
    T.MOST_RECENT_SUCCESSFUL_CREDIT_BILLING = S.MOST_RECENT_SUCCESSFUL_CREDIT_BILLING,
    T.REDEEMED_DOWNGRADED_PROMO = S.REDEEMED_DOWNGRADED_PROMO,
    T.SCRUBS_INTEREST_RESPONSE = S.SCRUBS_INTEREST_RESPONSE,
    T.SXF_LIFETIME_SUB_DEPARTMENT = S.SXF_LIFETIME_SUB_DEPARTMENT,
    T.SXF_ACTIVATING_SUB_DEPARTMENT = S.SXF_ACTIVATING_SUB_DEPARTMENT,
    T.SXF_LIFETIME_SUB_DEPARTMENT_WITHOUT_GENDER = S.SXF_LIFETIME_SUB_DEPARTMENT_WITHOUT_GENDER,
    T.SXF_ACTIVATING_SUB_DEPARTMENT_WITHOUT_GENDER = S.SXF_ACTIVATING_SUB_DEPARTMENT_WITHOUT_GENDER,
    T.SXF_CONSECUTIVE_FAILED_BILLING_MONTHS = S.SXF_CONSECUTIVE_FAILED_BILLING_MONTHS,
    T.SXF_IS_FAILED_BILLING = S.SXF_IS_FAILED_BILLING,
    T.SXF_ON_TRACK_PASSIVE_CANCEL_CRITERIA_MONTHS = S.SXF_ON_TRACK_PASSIVE_CANCEL_CRITERIA_MONTHS,
    T.SXF_TOTAL_FAILED_BILLING_MONTHS_INPASTYEAR = S.SXF_TOTAL_FAILED_BILLING_MONTHS_INPASTYEAR,
    T.SXF_IS_FIRST_TIME_FAILED_BILLING = S.SXF_IS_FIRST_TIME_FAILED_BILLING,
    T.SXF_VIP_GAMER = S.SXF_VIP_GAMER,
    T.CURRENT_STATUS = S.CURRENT_STATUS,
    T.is_new_TCs_opt_in = S.is_new_TCs_opt_in,
    T.IS_EMP_OPTIN = S.IS_EMP_OPTIN,
    T.CUSTOMER_BI_UPDATED_DATETIME = CURRENT_TIMESTAMP(),
    T.FAILED_BILLER_SEGMENT = S.FAILED_BILLER_SEGMENT,
    T.SKIPPER_SEGMENT = S.SKIPPER_SEGMENT,
    T.CANCELLED_VIP_SEGMENT = S.CANCELLED_VIP_SEGMENT,
    T.IS_BIRTH_MONTH = S.IS_BIRTH_MONTH,
    T.BIRTH_DAY = S.BIRTH_DAY,
    T.IS_ANNIVERSARY_TODAY = S.IS_ANNIVERSARY_TODAY,
    T.IS_ANNIVERSARY_MONTH = S.IS_ANNIVERSARY_MONTH,
    T.SXF_NMP_FIFTH_BILLING_PERK_FLAG = S.SXF_NMP_FIFTH_BILLING_PERK_FLAG,
    T.FIRST_MOST_RECENT_FAVORITED_ITEM_SITE_NAME = S.FIRST_MOST_RECENT_FAVORITED_ITEM_SITE_NAME,
    T.FIRST_MOST_RECENT_FAVORITED_ITEM_IMAGE_URL = S.FIRST_MOST_RECENT_FAVORITED_ITEM_IMAGE_URL,
    T.SECOND_MOST_RECENT_FAVORITED_ITEM_SITE_NAME = S.SECOND_MOST_RECENT_FAVORITED_ITEM_SITE_NAME,
    T.SECOND_MOST_RECENT_FAVORITED_ITEM_IMAGE_URL = S.SECOND_MOST_RECENT_FAVORITED_ITEM_IMAGE_URL,
    T.SXF_NMP_FIFTH_BILLING_PERK_COUNTER = S.SXF_NMP_FIFTH_BILLING_PERK_COUNTER
WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID,
    STORE,
    STORE_GROUP,
    PROMO_GROUP,
    LEAD_REGISTRATION_DATETIME,
    ACTIVATION_LOCAL_DATE,
    CANCEL_LOCAL_DATE,
    REGISTRATION_SOURCE_MEDIUM_CHANNEL,
    ACTIVATION_SOURCE_MEDIUM_CHANNEL,
    IS_ACTIVATED_ON_MOBILE_APP,
    TIMEZONE,
    VIP_LIFETIME_SAVING,
    AVG_DAYS_BETWEEN_PURCHASE,
    LAST_PLACED_DATE,
    IS_RETAIL_SHOPPER,
    IS_ONLINE_SHOPPER,
    IS_MOBILE_APP_SHOPPER,
    IS_FEMALE_PRODUCT_SHOPPER,
    IS_MALE_PRODUCT_SHOPPER,
    IS_UNISEX_PRODUCT_SHOPPER,
    IS_MALE_PRODUCT_ONLY_SHOPPER,
    LAST_RETAIL_PURCHASE_DATE,
    LAST_MOBILE_APP_PURCHASE_DATE,
    LAST_ONLINE_PURCHASE_DATE,
    LAST_PRODUCT_ORDER_PURCHASE_DATE,
    NEAREST_RETAIL_STORE,
    RETAIL_STORE_DISTANCE,
    RETAIL_STORE_DRIVE_TIME,
    NEAREST_RETAIL_STORE_ADDRESS1,
    NEAREST_RETAIL_STORE_ADDRESS2,
    NEAREST_RETAIL_STORE_CITY,
    NEAREST_RETAIL_STORE_STATE,
    NEAREST_RETAIL_STORE_ZIP,
    NEAREST_RETAIL_STORE_COUNTRY_CODE,
    NEAREST_RETAIL_STORE_GOOGLE_MAPS_LINK,
    NEAREST_RETAIL_STORE_LOCATION_IN_MALL,
    SKIP_REASON,
    IS_IN_GRACE_PERIOD,
    ACTIVATING_UPT,
    PURCHASE_MENS_ONLY_ACTIVATING_ORDER,
    PRE_PASSIVE_CANCEL_2M_CHECK,
    CURRENT_CASH_MARGIN_DECILE,
    CURRENT_PRODUCT_MARGIN_DECILE,
    LTV_ORDER_REVENUE,
    LTV_TOTAL_REVENUE,
    PRODUCT_ORDERS,
    CREDIT_BILLINGS,
    LTV_CASH_GROSS_MARGIN,
    LTV_GROSS_MARGIN_PCT,
    AVG_AOV,
    RECENT_CUSTOMER_ACTION,
    RECENT_BILL_ME_NOW_DATE,
    PREVIOUS_DECILE,
    MEMBERSHIP_AMOUNT_SAVED_TO_DATE,
    IS_REPEAT_FAILED_BILLING,
    CONSECUTIVE_FAILED_BILLING_MONTHS,
    IS_EMP_ENROLLED,
    IS_BRAND_APP_DOWNLOADED,
    IS_FITNESS_APP_DOWNLOADED,
    IS_HYDROW_PURCHASED,
    SX_UNIQUE_TOP_SIZE,
    SX_UNIQUE_BOTTOM_SIZE,
    HOW_DID_YOU_HEAR,
    MEMBERSHIP_2995USD_CREDITS,
    MEMBERSHIP_3995USD_CREDITS,
    MOST_RECENT_SUCCESSFUL_CREDIT_BILLING,
    REDEEMED_DOWNGRADED_PROMO,
    SCRUBS_INTEREST_RESPONSE,
    SXF_LIFETIME_SUB_DEPARTMENT,
    SXF_ACTIVATING_SUB_DEPARTMENT,
    SXF_LIFETIME_SUB_DEPARTMENT_WITHOUT_GENDER,
    SXF_ACTIVATING_SUB_DEPARTMENT_WITHOUT_GENDER,
    SXF_CONSECUTIVE_FAILED_BILLING_MONTHS,
    SXF_IS_FAILED_BILLING,
    SXF_ON_TRACK_PASSIVE_CANCEL_CRITERIA_MONTHS,
    SXF_TOTAL_FAILED_BILLING_MONTHS_INPASTYEAR,
    SXF_IS_FIRST_TIME_FAILED_BILLING,
    SXF_VIP_GAMER,
    CURRENT_STATUS,
    is_new_TCs_opt_in,
    IS_EMP_OPTIN,
    CUSTOMER_BI_UPDATED_DATETIME,
    FAILED_BILLER_SEGMENT,
    SKIPPER_SEGMENT,
    CANCELLED_VIP_SEGMENT,
    IS_BIRTH_MONTH,
    BIRTH_DAY,
    IS_ANNIVERSARY_TODAY,
    IS_ANNIVERSARY_MONTH,
    SXF_NMP_FIFTH_BILLING_PERK_FLAG,
    FIRST_MOST_RECENT_FAVORITED_ITEM_SITE_NAME,
    FIRST_MOST_RECENT_FAVORITED_ITEM_IMAGE_URL,
    SECOND_MOST_RECENT_FAVORITED_ITEM_SITE_NAME,
    SECOND_MOST_RECENT_FAVORITED_ITEM_IMAGE_URL,
    SXF_NMP_FIFTH_BILLING_PERK_COUNTER
    )
    VALUES(
    S.CUSTOMER_ID,
    S.STORE,
    S.STORE_GROUP,
    S.PROMO_GROUP,
    S.LEAD_REGISTRATION_DATETIME,
    S.ACTIVATION_LOCAL_DATE,
    S.CANCEL_LOCAL_DATE,
    S.REGISTRATION_SOURCE_MEDIUM_CHANNEL,
    S.ACTIVATION_SOURCE_MEDIUM_CHANNEL,
    S.IS_ACTIVATED_ON_MOBILE_APP,
    S.TIMEZONE,
    S.VIP_LIFETIME_SAVING,
    S.AVG_DAYS_BETWEEN_PURCHASE,
    S.LAST_PLACED_DATE,
    S.IS_RETAIL_SHOPPER,
    S.IS_ONLINE_SHOPPER,
    S.IS_MOBILE_APP_SHOPPER,
    S.IS_FEMALE_PRODUCT_SHOPPER,
    S.IS_MALE_PRODUCT_SHOPPER,
    S.IS_UNISEX_PRODUCT_SHOPPER,
    S.IS_MALE_PRODUCT_ONLY_SHOPPER,
    S.LAST_RETAIL_PURCHASE_DATE,
    S.LAST_MOBILE_APP_PURCHASE_DATE,
    S.LAST_ONLINE_PURCHASE_DATE,
    S.LAST_PRODUCT_ORDER_PURCHASE_DATE,
    S.NEAREST_RETAIL_STORE,
    S.RETAIL_STORE_DISTANCE,
    S.RETAIL_STORE_DRIVE_TIME,
    S.NEAREST_RETAIL_STORE_ADDRESS1,
    S.NEAREST_RETAIL_STORE_ADDRESS2,
    S.NEAREST_RETAIL_STORE_CITY,
    S.NEAREST_RETAIL_STORE_STATE,
    S.NEAREST_RETAIL_STORE_ZIP,
    S.NEAREST_RETAIL_STORE_COUNTRY_CODE,
    S.NEAREST_RETAIL_STORE_GOOGLE_MAPS_LINK,
    S.NEAREST_RETAIL_STORE_LOCATION_IN_MALL,
    S.SKIP_REASON,
    S.IS_IN_GRACE_PERIOD,
    S.ACTIVATING_UPT,
    S.PURCHASE_MENS_ONLY_ACTIVATING_ORDER,
    S.PRE_PASSIVE_CANCEL_2M_CHECK,
    S.CURRENT_CASH_MARGIN_DECILE,
    S.CURRENT_PRODUCT_MARGIN_DECILE,
    S.LTV_ORDER_REVENUE,
    S.LTV_TOTAL_REVENUE,
    S.PRODUCT_ORDERS,
    S.CREDIT_BILLINGS,
    S.LTV_CASH_GROSS_MARGIN,
    S.LTV_GROSS_MARGIN_PCT,
    S.AVG_AOV,
    S.RECENT_CUSTOMER_ACTION,
    S.RECENT_BILL_ME_NOW_DATE,
    S.PREVIOUS_DECILE,
    S.MEMBERSHIP_AMOUNT_SAVED_TO_DATE,
    S.IS_REPEAT_FAILED_BILLING,
    S.CONSECUTIVE_FAILED_BILLING_MONTHS,
    S.IS_EMP_ENROLLED,
    S.IS_BRAND_APP_DOWNLOADED,
    S.IS_FITNESS_APP_DOWNLOADED,
    S.IS_HYDROW_PURCHASED,
    S.SX_UNIQUE_TOP_SIZE,
    S.SX_UNIQUE_BOTTOM_SIZE,
    S.HOW_DID_YOU_HEAR,
    S.MEMBERSHIP_2995USD_CREDITS,
    S.MEMBERSHIP_3995USD_CREDITS,
    S.MOST_RECENT_SUCCESSFUL_CREDIT_BILLING,
    S.REDEEMED_DOWNGRADED_PROMO,
    S.SCRUBS_INTEREST_RESPONSE,
    S.SXF_LIFETIME_SUB_DEPARTMENT,
    S.SXF_ACTIVATING_SUB_DEPARTMENT,
    S.SXF_LIFETIME_SUB_DEPARTMENT_WITHOUT_GENDER,
    S.SXF_ACTIVATING_SUB_DEPARTMENT_WITHOUT_GENDER,
    S.SXF_CONSECUTIVE_FAILED_BILLING_MONTHS,
    S.SXF_IS_FAILED_BILLING,
    S.SXF_ON_TRACK_PASSIVE_CANCEL_CRITERIA_MONTHS,
    S.SXF_TOTAL_FAILED_BILLING_MONTHS_INPASTYEAR,
    S.SXF_IS_FIRST_TIME_FAILED_BILLING,
    S.SXF_VIP_GAMER,
    S.CURRENT_STATUS,
    S.is_new_TCs_opt_in,
    S.IS_EMP_OPTIN,
    CURRENT_TIMESTAMP(),
    S.FAILED_BILLER_SEGMENT,
    S.SKIPPER_SEGMENT,
    S.CANCELLED_VIP_SEGMENT,
    S.IS_BIRTH_MONTH,
    S.BIRTH_DAY,
    S.IS_ANNIVERSARY_TODAY,
    S.IS_ANNIVERSARY_MONTH,
    S.SXF_NMP_FIFTH_BILLING_PERK_FLAG,
    S.FIRST_MOST_RECENT_FAVORITED_ITEM_SITE_NAME,
    S.FIRST_MOST_RECENT_FAVORITED_ITEM_IMAGE_URL,
    S.SECOND_MOST_RECENT_FAVORITED_ITEM_SITE_NAME,
    S.SECOND_MOST_RECENT_FAVORITED_ITEM_IMAGE_URL,
    S.SXF_NMP_FIFTH_BILLING_PERK_COUNTER
);
