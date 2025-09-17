SET start_date = '2019-01-01';
SET end_date = DATE_TRUNC(MONTH, DATEADD(DAY, -1, CURRENT_DATE()));

CREATE OR REPLACE TEMPORARY TABLE _bop_vip AS
SELECT DISTINCT
     cap.month_date AS bop_month_date,
     dv.business_unit,
     dv.region,
     dv.customer_id,
     dv.first_activating_cohort,
     dv.recent_vip_cancellation_date,
     CASE
        WHEN datediff(month, dv.first_activating_cohort, cap.month_date) + 1 > 24 THEN 'M25+'
        WHEN datediff(month, dv.first_activating_cohort, cap.month_date) + 1 > 12 THEN 'M13 to M24'
        ELSE 'M' || cast(datediff(month, dv.first_activating_cohort, cap.month_date) + 1 AS VARCHAR(20))
     END as tenure,
     dv.membership_price,
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_cust cap
JOIN gfb.gfb_dim_vip dv
    ON dv.customer_id = cap.meta_original_customer_id
WHERE
    cap.month_date BETWEEN $start_date AND  $end_date
    AND DATEDIFF(MONTH, dv.first_activating_cohort, cap.month_date) + 1 > 1
    AND cap.is_bop_vip = 1;

CREATE OR REPLACE TEMP TABLE _gfb_membership_credit_activity AS
SELECT
    business_unit,
    region,
    customer_id,
    credit_id,
    issued_date,
    redeemed_order_date,
    cancelled_date,
    other_activity_date,
    credit_type,
    credit_reason,
    redemption_order_id
FROM gfb.gfb_membership_credit_activity mca
WHERE credit_type IN ('Fixed Credit','Token')
    AND credit_reason IN  ('Membership Credit','Token Billing','Converted Membership Credit');

CREATE OR REPLACE TEMPORARY TABLE _outstanding_credit AS
SELECT
    mca.business_unit,
    mca.region,
    month_date.month_date,
    mca.customer_id,
    COUNT(DISTINCT mca.credit_id) AS outstanding_credit_count
FROM
(
    SELECT DISTINCT
        dd.month_date
    FROM edw_prod.data_model_jfb.dim_date dd
    WHERE
        dd.month_date BETWEEN $start_date AND $end_date
) month_date
JOIN _gfb_membership_credit_activity mca
    ON  DATE_TRUNC(MONTH, mca.issued_date) <= month_date.month_date
    AND (DATE_TRUNC(MONTH, mca.redeemed_order_date) >= month_date.month_date OR mca.redeemed_order_date IS NULL)
    AND (DATE_TRUNC(MONTH, mca.cancelled_date) >= month_date.month_date OR mca.cancelled_date IS NULL)
    AND (DATE_TRUNC(MONTH, mca.other_activity_date) >= month_date.month_date OR mca.other_activity_date IS NULL)
GROUP BY
    mca.business_unit,
    mca.region,
    month_date.month_date,
    mca.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _vip_cancellation AS
SELECT
    customer_id,
    business_unit,
    region,
    first_activating_cohort,
    recent_vip_cancellation_date AS cancellation_date,
    DATE_TRUNC(MONTH, cancellation_date) AS cancellation_month,
    recent_cancellation_type
FROM gfb.gfb_dim_vip dv
WHERE cancellation_month BETWEEN $start_date and $end_date;

CREATE OR REPLACE TEMPORARY TABLE _gfb_order_line_data_set_place_date AS
SELECT
    olp.business_unit,
    olp.region,
    olp.order_date,
    olp.customer_id,
    DATE_TRUNC(MONTH, olp.order_date) AS order_month,
    olp.order_id,
    olp.order_type,
    olp.cash_collected_amount,
    olp.total_qty_sold,
    olp.total_product_revenue
FROM gfb.gfb_order_line_data_set_place_date olp
WHERE
    olp.order_classification = 'product order'
    AND olp.order_type IN( 'vip repeat',  'ecom')
    AND order_month BETWEEN $start_date AND $end_date;

CREATE OR REPLACE TEMPORARY TABLE _vip_repeat_purchase AS
SELECT
    business_unit,
    region,
    order_month,
    customer_id,
    COUNT(DISTINCT order_id) AS vip_repeat_order,
    SUM(cash_collected_amount) AS cash_collected_amount
FROM _gfb_order_line_data_set_place_date
WHERE
    order_type = 'vip repeat'
    AND order_month BETWEEN $start_date AND $end_date
GROUP BY
    business_unit,
    region,
    order_month,
    customer_id;

--question 1
CREATE OR REPLACE TRANSIENT TABLE gfb.gfb054_01_credit_billing_deep_dive AS
SELECT
    bv.business_unit,
    bv.region,
    bv.bop_month_date as month_date,
    bv.tenure,
    CASE
        WHEN oc.customer_id IS NULL THEN '0'
        WHEN oc.outstanding_credit_count >= 10 THEN '10+'
        ELSE CAST(oc.outstanding_credit_count AS VARCHAR(10))
    END AS outstanding_credit,
    COUNT(bv.customer_id) AS bop_vip,
    COUNT(vrp.customer_id) AS purchased_customer,
    COUNT(vc.customer_id) AS vip_cancellation,
    SUM(vrp.vip_repeat_order) AS vip_repeat_orders,
FROM _bop_vip bv
LEFT JOIN _outstanding_credit oc
    ON oc.customer_id = bv.customer_id
    AND oc.month_date = bv.bop_month_date
LEFT JOIN _vip_cancellation vc
    ON vc.customer_id = bv.customer_id
    AND vc.cancellation_month = bv.bop_month_date
LEFT JOIN _vip_repeat_purchase vrp
    ON vrp.customer_id = bv.customer_id
    AND vrp.order_month = bv.bop_month_date
GROUP BY
    bv.business_unit,
    bv.region,
    bv.bop_month_date,
    bv.tenure,
    outstanding_credit;

--question 2
--view 1
CREATE OR REPLACE TEMPORARY TABLE _redemption_before_cancel AS
SELECT DISTINCT
    a.business_unit,
    a.region,
    a.customer_id,
    a.first_activating_cohort,
    a.cancellation_date,
    a.redeemed_order_date,
    COALESCE(oc.outstanding_credit_count, 0) AS outstanding_credit_count,
FROM
(
    SELECT
        mca.business_unit,
        mca.region,
        mca.customer_id,
        mca.credit_id,
        mca.redeemed_order_date,
        vc.cancellation_date,
        vc.first_activating_cohort,
        RANK() OVER (PARTITION BY mca.customer_id ORDER BY mca.redeemed_order_date DESC) AS credit_redeem_rank
    FROM _gfb_membership_credit_activity mca
    join _vip_cancellation vc
        on vc.customer_id = mca.customer_id
    WHERE mca.credit_type = 'Fixed Credit'
        AND mca.credit_reason = 'Membership Credit'
        AND mca.redeemed_order_date <= vc.cancellation_date
        AND mca.redeemed_order_date >= $start_date
        AND mca.redeemed_order_date >= mca.issued_date -- there are some issues in FACT_CREDIT_EVENT table
) a
LEFT JOIN _outstanding_credit oc
    on oc.customer_id = a.customer_id
    and oc.month_date = DATE_TRUNC(MONTH, a.cancellation_date)
WHERE a.credit_redeem_rank = 1;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb054_02_v1_credit_billing_deep_dive AS
SELECT
    rbc.business_unit,
    rbc.region,
    CASE
        WHEN DATEDIFF(MONTH, rbc.first_activating_cohort, DATE_TRUNC(MONTH, rbc.cancellation_date)) + 1 > 24 then 'M25+'
        WHEN DATEDIFF(MONTH, rbc.first_activating_cohort, DATE_TRUNC(MONTH, rbc.cancellation_date)) + 1 > 12 then 'M13 to M24'
        ELSE 'M' || CAST(DATEDIFF(MONTH, rbc.first_activating_cohort, DATE_TRUNC(MONTH, rbc.cancellation_date)) + 1 AS VARCHAR(20))
    END AS tenure,
    DATE_TRUNC(MONTH, rbc.cancellation_date) AS cancellation_month,
    COUNT(rbc.customer_id) AS customer_count,
    AVG(rbc.outstanding_credit_count) AS avg_outstanding_credit_count,
    AVG(DATEDIFF(DAY, rbc.redeemed_order_date, rbc.cancellation_date) + 1) AS avg_days_between_last_redemption_and_cancellation
FROM _redemption_before_cancel rbc
WHERE DATEDIFF(MONTH, rbc.first_activating_cohort, DATE_TRUNC(MONTH, rbc.cancellation_date)) + 1 > 1
GROUP BY
    rbc.business_unit,
    rbc.region,
    tenure,
    cancellation_month;

--view 2
CREATE OR REPLACE TRANSIENT TABLE gfb.gfb054_02_v2_credit_billing_deep_dive AS
SELECT
    bv.business_unit,
    bv.region,
    bv.bop_month_date,
    CASE
        WHEN oc.customer_id IS NULL THEN '0'
        WHEN oc.outstanding_credit_count >= 10 THEN '10+'
        ELSE CAST(oc.outstanding_credit_count AS VARCHAR(10))
    end as outstanding_credit,
    COUNT(bv.customer_id) AS bop_vip,
    COUNT(vc.customer_id) AS vip_cancellation,
    vip_cancellation * 1.0/bop_vip AS vip_cancellation_rate
FROM _bop_vip bv
LEFT JOIN _vip_cancellation vc
    ON vc.customer_id = bv.customer_id
    AND vc.cancellation_month = bv.bop_month_date
LEFT JOIN _outstanding_credit oc
    ON oc.customer_id = bv.customer_id
    AND oc.month_date = bv.bop_month_date
GROUP BY
    bv.business_unit,
    bv.region,
    bv.bop_month_date,
    outstanding_credit;

--question 3
CREATE OR REPLACE TEMPORARY TABLE _order_after_cancel AS
SELECT DISTINCT
    olp.business_unit,
    olp.region,
    olp.order_date,
    olp.order_month,
    olp.customer_id,
    olp.order_id,
    olp.order_type,
    vc.cancellation_date
FROM _gfb_order_line_data_set_place_date olp
LEFT JOIN _vip_cancellation vc
    ON vc.customer_id = olp.customer_id
WHERE
    olp.order_type = 'ecom'
    AND olp.order_month BETWEEN $start_date AND $end_date
    AND olp.order_date > vc.cancellation_date;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb054_03_credit_billing_deep_dive AS
SELECT
    oac.business_unit,
    oac.region,
    oac.order_month,
    COUNT(DISTINCT oac.customer_id) AS purchased_cancelled_vip,
    COUNT(DISTINCT oac.order_id) AS orders_after_cancel_with_membership_credit,
FROM _order_after_cancel oac
JOIN _gfb_membership_credit_activity mca
    ON oac.order_id = mca.redemption_order_id
    AND mca.credit_type = 'Fixed Credit'
    AND mca.credit_reason = 'Membership Credit'
GROUP BY
    oac.business_unit,
    oac.region,
    oac.order_month;

--Q4
CREATE OR REPLACE TRANSIENT TABLE gfb.gfb054_04_credit_billing_deep_dive AS
SELECT
    vc.business_unit,
    vc.region,
    vc.cancellation_month,
    COUNT(vc.customer_id) AS passive_cancellation,
    SUM(oc.outstanding_credit_count) AS outstanding_credit_count,
    SUM(oc.outstanding_credit_count) * 1.0/COUNT(vc.customer_id) AS avg_oustanding_credit_per_passive_cancelled_vip
FROM _vip_cancellation vc
LEFT JOIN _outstanding_credit oc
    ON oc.customer_id = vc.customer_id
    AND oc.month_date = vc.cancellation_month
WHERE vc.recent_cancellation_type = 'Passive'
GROUP BY
    vc.business_unit,
    vc.region,
    vc.cancellation_month;

--Q5
CREATE OR REPLACE TEMPORARY TABLE _vip_repeat_purchase_with_credit_redemption AS
SELECT
    orders.business_unit,
    orders.region,
    orders.order_month,
    orders.customer_id,
    orders.first_activating_cohort,
    case
        WHEN DATEDIFF(MONTH, orders.first_activating_cohort, orders.order_month) + 1 > 24 THEN 'M25+'
        WHEN DATEDIFF(MONTH, orders.first_activating_cohort, orders.order_month) + 1 > 12 THEN 'M13 to M24'
        ELSE 'M' || CAST(DATEDIFF(MONTH, orders.first_activating_cohort, orders.order_month) + 1 AS VARCHAR(20))
    END AS tenure,
    COUNT(orders.order_id) AS vip_repeat_orders,
    SUM(orders.cash_collected_amount) AS cash_collected_amount,
    SUM(orders.total_qty_sold) AS total_qty_sold,
    SUM(orders.total_product_revenue) AS total_product_revenue,
    SUM(rc.redeemed_membership_credit_count) AS redeemed_membership_credit_count,
FROM
(
    SELECT
        olp.business_unit,
        olp.region,
        olp.order_month,
        olp.customer_id,
        olp.order_id,
        dv.first_activating_cohort,
        SUM(olp.cash_collected_amount) AS cash_collected_amount,
        SUM(olp.total_qty_sold) AS total_qty_sold,
        SUM(olp.total_product_revenue) AS total_product_revenue
    FROM _gfb_order_line_data_set_place_date olp
    JOIN gfb.gfb_dim_vip dv
        ON dv.customer_id = olp.customer_id
    WHERE
        olp.order_type = 'vip repeat'
    GROUP BY
        olp.business_unit,
        olp.region,
        olp.order_month,
        olp.customer_id,
        olp.order_id,
        dv.first_activating_cohort
) orders
LEFT JOIN
(
    SELECT
    mca.redemption_order_id,
    COUNT(mca.credit_id) AS redeemed_membership_credit_count
    FROM _gfb_membership_credit_activity mca
    WHERE
        (DATE_TRUNC(MONTH, mca.redeemed_order_date) BETWEEN $start_date AND $end_date)
        AND mca.credit_type = 'Fixed Credit'
        AND mca.credit_reason = 'Membership Credit'
    GROUP BY
        mca.redemption_order_id
) rc
ON rc.redemption_order_id = orders.order_id
WHERE
    DATEDIFF(MONTH, orders.first_activating_cohort, orders.order_month) + 1 > 0
GROUP BY
    orders.business_unit,
    orders.region,
    orders.order_month,
    orders.customer_id,
    orders.first_activating_cohort,
    tenure;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb054_05_credit_billing_deep_dive AS
SELECT
    rc.business_unit,
    rc.region,
    rc.order_month,
    rc.tenure,
    case
        when oc.customer_id is null then '0'
        when oc.outstanding_credit_count >= 10 then '10+'
        else cast(oc.outstanding_credit_count as varchar(10))
    end as outstanding_credit,
    SUM(rc.vip_repeat_orders) AS vip_repeat_orders,
    SUM(rc.cash_collected_amount) AS cash_collected_amount,
    SUM(rc.total_qty_sold) AS total_qty_sold,
    SUM(rc.total_product_revenue) AS total_product_revenue,
    SUM(rc.redeemed_membership_credit_count) AS redeemed_membership_credit_count,
    SUM(rc.total_qty_sold) * 1.0/SUM(rc.vip_repeat_orders) AS upt,
    SUM(rc.total_product_revenue) * 1.0/SUM(rc.vip_repeat_orders) AS  aov,
    SUM(rc.total_product_revenue) * 1.0/SUM(rc.total_qty_sold) AS aur,
FROM _vip_repeat_purchase_with_credit_redemption rc
LEFT JOIN _outstanding_credit oc
    ON oc.customer_id = rc.customer_id
    AND oc.month_date = rc.order_month
GROUP BY
    rc.business_unit,
    rc.region,
    rc.order_month,
    rc.tenure,
    outstanding_credit;
