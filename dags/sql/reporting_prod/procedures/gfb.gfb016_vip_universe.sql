CREATE OR REPLACE TEMPORARY TABLE _vip_dates AS
SELECT
    dv.customer_id,
    ds.store_brand AS store_brand_name,
    ds.store_region AS store_region_abbr,
    dv.country AS store_country_abbr,
    dv.first_activating_cohort AS vip_cohort,
    dv.birthday_month AS birthday_month,
    DATEDIFF('month', dv.first_activating_date, current_date()) + 1 AS tenure,
    DATEDIFF('year', birthday_month, current_date()) AS age,
    dv.customer_postal_code,
    dv.hdyh,
    dv.is_reactivated_vip,
    COALESCE(dv.registration_type, 'Unknown') AS registration_type,
    dv.is_fk_cross_brand,
    dv.is_jf_cross_brand,
    dv.is_sd_cross_brand,
    dv.is_sx_cross_brand,
    dv.is_fl_cross_brand,
    dv.membership_reward_tier,
    DATE_TRUNC(MONTH, dv.recent_vip_cancellation_date) AS recent_cancellation_month,
    dv.recent_cancellation_type,
    CASE
        WHEN dv.activating_payment_method = 'ppcc'
        THEN 'PPCC Activation'
        ELSE 'Non PPCC Activation'
    END AS ppcc_activation_flag,
    dv.gamer_activation_flag,
    dv.aged_lead_activation_flag,
    dv.membership_level,
    dv.membership_price,
    dv.activating_source,
    dv.activating_medium
FROM gfb.gfb_dim_vip dv
JOIN gfb.vw_store ds
    on ds.store_id = dv.store_id
WHERE dv.current_membership_status = 'VIP';

CREATE OR REPLACE TEMPORARY TABLE _clv AS
SELECT
    v.customer_id,
    MAX(v.vip_cohort) as recent_vip_cohort,
    SUM(clv.cash_gross_profit) as cgm,
    SUM(clv.product_gross_profit) as ggm
from _vip_dates v
LEFT JOIN edw_prod.analytics_base.customer_lifetime_value_ltd clv
    on clv.meta_original_customer_id = v.customer_id
GROUP BY v.customer_id;

CREATE OR REPLACE TEMP TABLE _gfb_order_data AS
SELECT
    customer_id,
    order_id,
    order_date,
    order_type,
    billing_country,
    billing_state,
    billing_city,
    SUM(total_product_revenue) as total_product_revenue
FROM gfb.gfb_order_line_data_set_place_date
WHERE
    order_type IN ('vip activating', 'vip repeat')
    AND order_classification = 'product order'
GROUP BY
    customer_id,
    order_id,
    order_date,
    order_type,
    billing_country,
    billing_state,
    billing_city;

CREATE OR REPLACE TEMP TABLE _gfb_order_data__first_activating AS
SELECT
    customer_id,
    order_id,
    order_date,
    order_type,
    billing_country,
    billing_state,
    billing_city
FROM _gfb_order_data
WHERE order_type = 'vip activating'
QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY order_id ASC) = 1;

CREATE OR REPLACE TEMP TABLE _gfb_order_data__repeat_vip AS
SELECT
    customer_id,
    order_id,
    order_date,
    order_type,
    billing_country,
    billing_state,
    billing_city,
    RANK() OVER (PARTITION BY customer_id ORDER BY order_id ASC) AS order_rnk
FROM _gfb_order_data
WHERE order_type = 'vip repeat';

CREATE OR REPLACE TEMPORARY TABLE _success_orders AS
SELECT
    customer_id,
    COUNT(order_id) as num_orders,
    SUM(total_product_revenue) as total_product_revenue
FROM _gfb_order_data
GROUP BY customer_id;

CREATE OR REPLACE TEMPORARY TABLE _activating_orders AS
SELECT DISTINCT
    customer_id,
    UPPER(billing_city) as city,
    CASE
        WHEN LOWER(billing_state) = 'cal' THEN 'CA'
        WHEN LOWER(billing_state) = 'ohi' THEN 'OH'
        WHEN LOWER(billing_state) = 'flo' THEN 'FL'
        WHEN LOWER(billing_state) = 'ill' THEN 'IL'
        WHEN LOWER(billing_state) = 'ari' THEN 'AZ'
    ELSE UPPER(billing_state) END AS state,
    UPPER(billing_country) as country
FROM _gfb_order_data__first_activating;

CREATE OR REPLACE TEMPORARY TABLE _sailthru_optin AS
SELECT DISTINCT
    customer_id,
    CASE
        WHEN opt_in = 1 THEN 'opt_in'
        WHEN opt_in = 0 THEN 'opt_out'
        ELSE 'unknown'
    END AS opt_out_status
FROM gfb.view_customer_opt_info;

CREATE OR REPLACE TEMPORARY TABLE _mobile_app_customers AS
SELECT DISTINCT customer_id
FROM (
        SELECT
            edw_prod.stg.udf_unconcat_brand(s.customer_id) AS customer_id
        FROM reporting_base_prod.shared.session s
        JOIN gfb.vw_store st
            ON st.store_id = s.store_id
            AND st.store_type = 'Mobile App'
        WHERE s.is_in_segment=TRUE
        UNION ALL
        SELECT
            customer_id
        FROM edw_prod.data_model_jfb.fact_order s
        JOIN gfb.vw_store st
            ON st.store_id = s.store_id
        WHERE
            st.store_type = 'Mobile App'
);

CREATE OR REPLACE TEMPORARY TABLE _outstanding_membership_credit AS
SELECT
    dc.customer_id,
    COUNT(dc.credit_id) as outstanding_membership_credit_count
FROM edw_prod.data_model_jfb.dim_credit dc
JOIN lake_jfb_view.ultra_merchant.store_credit sc
    on sc.store_credit_id = dc.credit_id
JOIN gfb.vw_store st
    ON st.store_id = dc.store_id
WHERE
    dc.credit_type = 'Fixed Credit'
    AND dc.credit_reason = 'Membership Credit'
    AND source_credit_id_type = 'store_credit_id'
    AND sc.statuscode = 3240 -- active
    AND st.store_region = 'EU'
GROUP BY dc.customer_id
UNION ALL
SELECT
    dc.customer_id,
    COUNT(dc.credit_id) outstanding_membership_credit_count
FROM edw_prod.data_model_jfb.dim_credit dc
    JOIN gfb.vw_store st
        ON st.store_id = dc.store_id
    JOIN lake_consolidated.ultra_merchant.membership_token AS mt
        ON mt.meta_original_membership_token_id = dc.credit_id
WHERE
    dc.credit_type = 'Token'
    AND dc.credit_reason IN ('Token Billing', 'Converted Membership Credit','Refund - Converted Credit')
    AND st.store_region <> 'EU'
    AND mt.statuscode = 3540 -- active
GROUP BY dc.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _days_between_activating_repeat as
SELECT
    a.customer_id,
    AVG(DATEDIFF(DAY, a.order_date, r.order_date)) AS days_between_activating_repeat
FROM _gfb_order_data__first_activating AS a
    JOIN _gfb_order_data__repeat_vip AS r
        ON a.customer_id = r.customer_id
        AND r.order_rnk = 1
GROUP by a.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _days_between_repeat AS
SELECT
    repeat_1.customer_id,
    AVG(DATEDIFF(DAY, repeat_1.order_date, repeat_2.order_date)) AS days_between_repeat
FROM _gfb_order_data__repeat_vip AS repeat_1
    JOIN _gfb_order_data__repeat_vip AS repeat_2
        ON repeat_2.customer_id = repeat_1.customer_id
        and repeat_2.order_rnk - 1 = repeat_1.order_rnk
GROUP BY repeat_1.customer_id;

CREATE OR REPLACE TEMP TABLE _ltv_cust AS
SELECT
    meta_original_customer_id AS customer_id,
    month_date,
    is_failed_billing,
    is_successful_billing,
    is_merch_purchaser,
    is_skip
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_cust
WHERE
    store_id IN (SELECT store_id FROM gfb.vw_store)
    AND month_date >= DATE_TRUNC(MONTH, DATEADD(YEAR, -2, current_date()));

CREATE OR REPLACE TEMPORARY TABLE _failed_billing AS
SELECT
    fb.customer_id,
    COUNT(DISTINCT fb.month_date) as failed_billing_count,
    COUNT(DISTINCT sb.customer_id) as after_failed_success_billing_customer_flag,
    COUNT(DISTINCT po.customer_id) as after_failed_purchased_customer_flag
FROM _ltv_cust fb
LEFT JOIN _ltv_cust sb
    ON sb.customer_id = fb.customer_id
    AND sb.is_successful_billing = 1
    AND sb.month_date > fb.month_date
LEFT JOIN _ltv_cust po
    ON po.customer_id = fb.customer_id
    AND po.is_merch_purchaser = 1
    AND po.month_date > fb.month_date
WHERE fb.is_failed_billing = 1
GROUP BY fb.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _snoozed_vip AS
SELECT DISTINCT
    vd.customer_id
FROM _vip_dates vd
JOIN lake_jfb_view.ultra_merchant.membership AS m
    ON m.customer_id = vd.customer_id
JOIN lake_jfb_view.ultra_merchant.membership_snooze AS ms
    ON ms.membership_id = m.membership_id
WHERE ms.date_end >= current_date();

CREATE OR REPLACE TEMPORARY TABLE _skip AS
SELECT
    customer_id,
     COUNT(DISTINCT month_date) AS skips
FROM _ltv_cust
WHERE is_skip = 1
GROUP BY customer_id;

CREATE OR REPLACE TEMPORARY TABLE _reactivated_vips AS
SELECT
    fa.customer_id,
    MIN(CAST(fa.activation_local_datetime AS DATE)) AS first_activating_date,
    MAX(CAST(fa.activation_local_datetime AS DATE)) AS recent_activating_date,
    COUNT(fa.customer_id) as activating_count,
    DATEDIFF(DAY, first_activating_date, recent_activating_date) + 1/activating_count AS avg_days_between_reactivating
FROM edw_prod.data_model_jfb.fact_activation fa
JOIN _vip_dates vd
    ON vd.customer_id = fa.customer_id
    AND vd.is_reactivated_vip = 1
GROUP BY fa.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _vip_universe__non_agg AS
SELECT
    v.customer_id,
    v.store_brand_name,
    v.store_region_abbr,
    v.store_country_abbr,
    v.vip_cohort,
    v.birthday_month,
    v.is_reactivated_vip,
    v.registration_type,
    v.is_fk_cross_brand,
    v.is_jf_cross_brand,
    v.is_sd_cross_brand,
    v.is_sx_cross_brand,
    v.is_fl_cross_brand,
    v.membership_reward_tier,
    v.recent_cancellation_month,
    v.recent_cancellation_type,
    v.ppcc_activation_flag,
    v.gamer_activation_flag,
    v.aged_lead_activation_flag,
    v.membership_level,
    v.membership_price,
    v.activating_source,
    v.activating_medium,
    v.customer_postal_code,
    ao.city,
    ao.country,
    so.opt_out_status,
    COALESCE(omc.outstanding_membership_credit_count, 0) AS outstanding_membership_credit_count,
    CASE
        WHEN v.Tenure = 1 THEN 'Under 1 month'
        WHEN v.Tenure > 1 AND v.Tenure <= 3 THEN '1 - 3'
        WHEN v.Tenure >= 4 AND v.Tenure <= 6 THEN '4 - 6'
        WHEN v.Tenure >= 7 AND v.Tenure <= 12 THEN '7 - 12'
        WHEN v.Tenure >= 13 AND v.Tenure <= 24 THEN '13 - 24'
        WHEN v.Tenure >= 25 AND v.Tenure <= 48 THEN '25 - 48'
        WHEN v.Tenure >= 49 AND v.Tenure <= 60 THEN '49 - 60'
        WHEN v.Tenure > 60 THEN 'Over 5 Years'
        ELSE 'Unknown Tenure'
    END AS Tenure,
    CASE
        WHEN v.Age >= 18 AND v.Age <= 20 THEN '18-20'
        WHEN v.Age >= 21 AND v.Age <= 25 THEN '21-25'
        WHEN v.Age >= 26 AND v.Age <= 30 THEN '26-30'
        WHEN v.Age >= 31 AND v.Age <= 35 THEN '31-35'
        WHEN v.Age >= 36 AND v.Age <= 45 THEN '36-45'
        WHEN v.Age > 45 AND v.Age <= 100 THEN 'Over 45'
        ELSE 'Unknown Age'
    END AS Age,
    CASE
        WHEN v.hdyh LIKE '%Facebook%' THEN 'Facebook'
        WHEN v.hdyh LIKE '%Friend%' THEN 'Friend'
        WHEN v.hdyh LIKE '%TV%' THEN 'TV'
        WHEN v.hdyh LIKE '%Instagram%' THEN 'Instagram'
        WHEN v.hdyh LIKE '%Pinterest%' THEN 'Pinterest'
        WHEN v.hdyh LIKE '%Twitter%' THEN 'Twitter'
        WHEN v.hdyh LIKE '%Banner Ad%' THEN 'Banner Ad'
        WHEN v.hdyh LIKE '%Online Ad%' THEN 'Online Ad'
        WHEN v.hdyh LIKE '%Search Engine%' THEN 'Search Engine'
        WHEN v.hdyh IS NOT NULL THEN v.hdyh
        ELSE 'Unsure HDYH'
    END AS hdyh,
    CASE
        WHEN c.cgm < 0 THEN 'Negative'
        WHEN c.cgm >= 0 AND c.cgm <= 25 THEN '0 - 25'
        WHEN c.cgm > 25 AND c.cgm <= 50 then '26 - 50'
        WHEN c.cgm > 50 AND c.cgm <= 100 then '51 - 100'
        WHEN c.cgm > 100 AND c.cgm <= 200 then '101 - 200'
        WHEN c.cgm > 200 then 'Over 200'
        ELSE 'Unknown CGM'
    END as cgm,
    CASE
        WHEN c.ggm < 0 THEN 'Negative'
        WHEN c.ggm >= 0 AND c.ggm <= 25 THEN '0 - 25'
        WHEN c.ggm > 25 AND c.ggm <= 50 then '26 - 50'
        WHEN c.ggm > 50 AND c.ggm <= 100 then '51 - 100'
        WHEN c.ggm > 100 AND c.ggm <= 200 then '101 - 200'
        WHEN c.ggm > 200 THEN 'Over 200'
        ELSE 'Unknown GGM'
    END AS ggm,
    CASE
        WHEN s.num_orders = 1 THEN 'Only 1 order'
        WHEN s.num_orders = 2 THEN '2 orders'
        WHEN s.num_orders = 3 THEN '3 orders'
        WHEN s.num_orders = 4 or s.Num_Orders = 5 THEN '4-5 orders'
        WHEN s.num_orders >= 6 THEN 'Over 6 orders'
        ELSE 'Unknown Order Count'
    END as num_orders,
    CASE
        WHEN v.store_region_abbr = 'EU' then 'EU'
        WHEN v.store_country_abbr = 'CA' then 'Canada'
        ELSE ao.State
     END AS state,
    CASE
        WHEN mac.customer_id IS NOT NULL
            THEN 'app_customer'
        ELSE 'not_app_customer'
    END AS app_customer_flag,
    CASE
        WHEN dbar.days_between_activating_repeat <= 7 THEN '1 Week'
        WHEN dbar.days_between_activating_repeat <= 30 THEN '1 Month'
        WHEN dbar.days_between_activating_repeat <= 60 THEN '2 Months'
        WHEN dbar.days_between_activating_repeat <= 90 THEN '3 Months'
        WHEN dbar.days_between_activating_repeat <= 120 THEN '4 Months'
        WHEN dbar.days_between_activating_repeat <= 150 THEN '5 Months'
        WHEN dbar.days_between_activating_repeat <= 180 THEN '6 Months'
        WHEN dbar.days_between_activating_repeat > 180 THEN '6+ Months'
    END AS days_between_activating_and_repeat,
    CASE
        WHEN dbr.days_between_repeat <= 7 THEN '1 Week'
        WHEN dbr.days_between_repeat <= 30 THEN '1 Month'
        WHEN dbr.days_between_repeat <= 60 THEN '2 Months'
        WHEN dbr.days_between_repeat <= 90 THEN '3 Months'
        WHEN dbr.days_between_repeat <= 120 THEN '4 Months'
        WHEN dbr.days_between_repeat <= 150 THEN '5 Months'
        WHEN dbr.days_between_repeat <= 180 THEN '6 Months'
        WHEN dbr.days_between_repeat > 180 THEN '6+ Months'
    END AS days_between_repeat,
    CASE
        WHEN fb.failed_billing_count > 12 THEN '13+'
        ELSE CAST(fb.failed_billing_count AS VARCHAR(20))
    END AS failed_billing_count,
    CASE
        WHEN fb.after_failed_success_billing_customer_flag = 1 THEN 'Successful Billing'
        WHEN fb.after_failed_success_billing_customer_flag = 0 THEN 'No Successful Billing'
    END AS after_failed_success_billing_customer_flag,
    CASE
        WHEN fb.after_failed_purchased_customer_flag = 1 THEN 'Product Purchase'
        WHEN fb.after_failed_purchased_customer_flag = 0 THEN 'No Product Purchase'
    END as after_failed_purchased_customer_flag,
    CASE
        WHEN s.total_product_revenue >= 0 AND s.total_product_revenue <= 25 THEN '0 - 25'
        WHEN s.total_product_revenue > 25 AND s.total_product_revenue <= 50 THEN '26 - 50'
        WHEN s.total_product_revenue > 50 AND s.total_product_revenue <= 100 THEN '51 - 100'
        WHEN s.total_product_revenue > 100 AND s.total_product_revenue <= 200 THEN '101 - 200'
        WHEN s.total_product_revenue > 200 AND s.total_product_revenue <= 300 THEN '200 - 300'
        WHEN s.total_product_revenue > 300 AND s.total_product_revenue <= 400 THEN '300 - 400'
        WHEN s.total_product_revenue > 400 AND s.total_product_revenue <= 500 THEN '400 - 500'
        WHEN s.total_product_revenue > 500 THEN '500+'
    END AS total_lifetime_product_revenue,
   CASE
        WHEN sk.skips <= 2 THEN '1 - 2'
        WHEN sk.skips <= 5 then '3 - 5'
        WHEN sk.skips <= 8 then '6 - 8'
        WHEN sk.skips <= 12 then '9 - 12'
        WHEN sk.skips <= 18 then '12 - 18'
        WHEN sk.skips <= 25 then '18 - 25'
        ELSE '0'
    END AS skips,
    CASE
        WHEN sv.customer_id IS NOT NULL THEN 1
        ELSE 0
    END AS snoozed_flag,
   CASE
        WHEN rv.avg_days_between_reactivating <= 7 THEN '1 Week'
        WHEN rv.avg_days_between_reactivating <= 30 THEN '1 Month'
        WHEN rv.avg_days_between_reactivating <= 60 THEN '2 Months'
        WHEN rv.avg_days_between_reactivating <= 90 THEN '3 Months'
        WHEN rv.avg_days_between_reactivating <= 120 THEN '4 Months'
        WHEN rv.avg_days_between_reactivating <= 150 THEN '5 Months'
        WHEN rv.avg_days_between_reactivating <= 180 THEN '6 Months'
        WHEN rv.avg_days_between_reactivating > 180 THEN '6+ Months'
   END AS days_between_reactivation
FROM _vip_dates v
    LEFT JOIN _clv c
        ON c.customer_id = v.customer_id
    LEFT JOIN _success_orders s
        on v.customer_id = s.customer_id
    LEFT JOIN _activating_orders ao
        ON ao.customer_id = v.customer_id
    LEFT JOIN _sailthru_optin so
        ON so.customer_id = v.customer_id
    LEFT JOIN _mobile_app_customers mac
        ON mac.customer_id = v.customer_id
    LEFT JOIN _outstanding_membership_credit omc
        ON omc.customer_id = v.customer_id
    LEFT JOIN _days_between_activating_repeat dbar
        ON dbar.customer_id = v.customer_id
    LEFT JOIN _days_between_repeat dbr
        ON dbr.customer_id = v.customer_id
    LEFT JOIN _failed_billing fb
        ON fb.customer_id = v.customer_id
    LEFT JOIN _skip sk
        ON sk.customer_id = v.customer_id
    LEFT JOIN _snoozed_vip sv
        ON sv.customer_id = v.customer_id
    LEFT JOIN _reactivated_vips rv
        ON rv.customer_id = v.customer_id;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb016_vip_universe as
SELECT
    store_brand_name,
    store_region_abbr,
    store_country_abbr,
    vip_cohort,
    age,
    tenure,
    state,
    hdyh,
    num_orders,
    cgm,
    ggm,
    opt_out_status,
    app_customer_flag,
    city,
    country,
    outstanding_membership_credit_count,
    days_between_activating_and_repeat,
    days_between_repeat,
    failed_billing_count,
    after_failed_success_billing_customer_flag,
    after_failed_purchased_customer_flag,
    total_lifetime_product_revenue,
    skips,
    snoozed_flag,
    is_reactivated_vip,
    days_between_reactivation,
    registration_type,
    is_fk_cross_brand,
    is_fl_cross_brand,
    is_jf_cross_brand,
    is_sd_cross_brand,
    is_sx_cross_brand,
    membership_reward_tier,
    recent_cancellation_month,
    recent_cancellation_type,
    gamer_activation_flag,
    ppcc_activation_flag,
    aged_lead_activation_flag,
    membership_level,
    membership_price,
    activating_source,
    activating_medium,
    COUNT(DISTINCT customer_id) num_customers
FROM _vip_universe__non_agg
GROUP BY
    store_brand_name,
    store_region_abbr,
    store_country_abbr,
    vip_cohort,
    age,
    tenure,
    state,
    hdyh,
    num_orders,
    cgm,
    ggm,
    opt_out_status,
    app_customer_flag,
    city,
    country,
    outstanding_membership_credit_count,
    days_between_activating_and_repeat,
    days_between_repeat,
    failed_billing_count,
    after_failed_success_billing_customer_flag,
    after_failed_purchased_customer_flag,
    total_lifetime_product_revenue,
    skips,
    snoozed_flag,
    is_reactivated_vip,
    days_between_reactivation,
    registration_type,
    is_fk_cross_brand,
    is_fl_cross_brand,
    is_jf_cross_brand,
    is_sd_cross_brand,
    is_sx_cross_brand,
    membership_reward_tier,
    recent_cancellation_month,
    recent_cancellation_type,
    gamer_activation_flag,
    ppcc_activation_flag,
    aged_lead_activation_flag,
    membership_level,
    membership_price,
    activating_source,
    activating_medium;
