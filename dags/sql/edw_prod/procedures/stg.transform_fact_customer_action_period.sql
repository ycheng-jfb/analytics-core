-- not used anymore. no change needed
SET target_table = 'stg.fact_customer_action_period';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

SET wm_edw_stg_fact_membership_event = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_membership_event'));
SET wm_edw_stg_fact_customer_action = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_customer_action'));


SET low_watermark_ltz = (select min(min_datetime)
                        from (select min(event_start_local_datetime) as min_datetime
                                from stg.fact_membership_event
                                where meta_update_datetime > $wm_edw_stg_fact_membership_event
                            UNION
                            select min(customer_action_local_datetime) as min_datetime
                                from stg.fact_customer_action
                                where meta_update_datetime > $wm_edw_stg_fact_customer_action
                            ) a);
SET low_watermark_ltz = IFF($low_watermark_ltz::DATE < '2010-01-01', '2010-01-01', $low_watermark_ltz);
SET beg_of_month_lw = (SELECT date_from_parts(year($low_watermark_ltz), month($low_watermark_ltz), 1));
SET beg_of_month = (select date_from_parts(year($execution_start_time), month($execution_start_time), 1));
SET months_to_load = (SELECT datediff(month, $beg_of_month_lw, $beg_of_month) + 1);

CREATE OR REPLACE  TEMP TABLE _period_month AS
SELECT
  dateadd(month, 1 * seq4(), $beg_of_month_lw) AS period_month_date
FROM table(generator(rowcount=>$months_to_load))
ORDER BY period_month_date;

CREATE OR REPLACE TEMP TABLE _customer_base AS
SELECT
    customer_id,
    store_id,
    recent_activation_date,
    period_month_date
FROM
(
    SELECT
        fme.customer_id,
        fme.store_id,
        fme.recent_activation_local_datetime::DATE AS recent_activation_date,
        pm.period_month_date,
        row_number() over(PARTITION BY customer_id, pm.period_month_date
                            ORDER BY fme.event_start_local_datetime DESC
                         ) AS rno
    FROM stg.fact_membership_event fme
    JOIN stg.dim_membership_event_type dms
        ON fme.membership_event_type_key = dms.membership_event_type_key
    JOIN _period_month pm
        ON fme.event_start_local_datetime::DATE < pm.period_month_date
        AND fme.event_end_local_datetime::DATE >= pm.period_month_date
    WHERE dms.membership_event_type = 'Activation'
        AND lower(fme.membership_type_detail) != 'classic'
        AND NOT NVL(fme.is_deleted, False)
) A
WHERE A.rno = 1;

CREATE OR REPLACE TEMP TABLE _customer_action_base AS
SELECT
    cb.period_month_date,
    cb.customer_id,
    ds.store_id,
    ds.store_full_name AS store_name,
    ds.store_brand AS store_brand,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    cb.recent_activation_date,
    fca.order_id,
    dcat.customer_action_type
FROM _customer_base cb
JOIN stg.fact_customer_action fca
    ON cb.customer_id = fca.customer_id
    AND fca.customer_action_period_local_date = cb.period_month_date
JOIN stg.fact_order fo
    ON fca.order_id = fo.order_id
JOIN stg.dim_order_sales_channel doc
    ON fo.order_sales_channel_key = doc.order_sales_channel_key
    AND doc.is_ps_order = FALSE
JOIN stg.dim_customer_action_type dcat
    ON fca.customer_action_type_key = dcat.customer_action_type_key
JOIN stg.dim_store ds
    ON ds.store_id = cb.store_id
WHERE fca.customer_id <> -1;

CREATE OR REPLACE TEMP TABLE _main_data_set AS
SELECT
    fca.period_month_date,
    fca.customer_id,
    fca.store_id,
    fca.store_name,
    fca.store_brand,
    fca.store_brand_abbr,
    fca.store_country,
    fca.store_region,
    fca.recent_activation_date,
    CASE fca.customer_action_type
        WHEN 'Placed Order - Cash' THEN 'Purchased Order - Cash'
        WHEN 'Placed Order - Non-Cash' THEN 'Purchased Order - Non Cash'
        WHEN 'Billed Credit' THEN
            CASE WHEN pay.order_payment_status_code IN (2600,2650,2651) THEN 'Billed Credit - Succeeded' --2600 Paid, 2650 Refunded, 2651 Refunded (Partial)
                    WHEN brsq.statuscode IN (4200,4201) THEN 'Billed Credit - Pending' --4200 Pending, 4201 Pending (In Progress)
                    WHEN dops.order_processing_status_code IN (2200) OR pay.order_payment_status_code IN (2510,2552) THEN 'Billed Credit - Failed' --2200 Cancelled, 2510 Authorization Failed, 2552 Authorization Expired
            END
        WHEN 'Billed Credit - Failed Processing' THEN 'Billed Credit - Failed'
        WHEN 'Billed Membership Fee' THEN
            CASE WHEN pay.order_payment_status_code IN (2600,2650,2651) THEN 'Billed Membership Fee - Succeeded' --2600 Paid, 2650 Refunded, 2651 Refunded (Partial)
                    WHEN brsq.statuscode IN (4200,4201) THEN 'Billed Membership Fee - Pending' --4200 Pending, 4201 Pending (In Progress)
                    WHEN dops.order_processing_status_code IN (2200) OR pay.order_payment_status_code IN (2510,2552) THEN 'Billed Membership Fee - Failed' --2200 Cancelled, 2510 Authorization Failed, 2552 Authorization Expired
            END
        WHEN 'Billed Membership Fee - Failed Processing' THEN 'Billed Membership Fee - Failed'
        WHEN 'Skipped Month' THEN 'Skipped Month'
        WHEN 'Cancelled Membership' THEN 'Cancelled Membership'
        WHEN 'Passive Cancelled Membership' THEN 'Cancelled Membership' -- To do for transform fact customer action
    END AS customer_action_type
FROM _customer_action_base fca
LEFT JOIN stg.fact_order fo
    ON fca.order_id = fo.order_id
LEFT JOIN stg.dim_order_processing_status dops
    ON fo.order_processing_status_key = dops.order_processing_status_key
LEFT JOIN stg.dim_order_payment_status pay
    ON fo.order_payment_status_key = pay.order_payment_status_key
LEFT JOIN stg.dim_order_status dos
    ON fo.order_status_key = dos.order_status_key
LEFT JOIN lake_consolidated.ultra_merchant.billing_retry_schedule_queue brsq
    ON fo.order_id = brsq.order_id
WHERE fca.customer_action_type IN ('Skipped Month','Cancelled Membership','Passive Cancelled Membership','Billed Credit - Failed Processing','Billed Membership Fee - Failed Processing') -- Bring in skipped, cancelled membership, and failed billing events
    OR (fca.customer_action_type IN ('Placed Order - Cash','Placed Order - Non-Cash') AND dos.order_status = 'Success') -- Successful orders
    OR (fca.customer_action_type = 'Billed Credit' AND pay.order_payment_status_code IN (2600,2650,2651)) -- Billed credit succeeded
    OR (fca.customer_action_type = 'Billed Credit' AND brsq.statuscode in (4200,4201)) -- Pending credit
    OR (fca.customer_action_type = 'Billed Credit' AND (dops.order_processing_status_code = 2200 or pay.order_payment_status_code IN (2510,2552))) -- Billed credit failed
    OR (fca.customer_action_type = 'Billed Membership Fee' AND pay.order_payment_status_code IN (2600,2650,2651)) -- Billed Membership Fee succeeded
    OR (fca.customer_action_type = 'Billed Membership Fee' AND brsq.statuscode in (4200,4201)) -- Pending Membership Fee
    OR (fca.customer_action_type = 'Billed Membership Fee' AND (dops.order_processing_status_code = 2200 or pay.order_payment_status_code IN (2510,2552))); -- Billed Membership Fee failed


-------------------------------------------------------------------------------------------------
	--Add in No Action for VIP customers
INSERT INTO _main_data_set
(
    period_month_date,
    customer_id,
    store_id,
    store_name,
    store_brand,
    store_brand_abbr,
    store_country,
    store_region,
    recent_activation_date,
    customer_action_type
)
SELECT
    cb.period_month_date,
    cb.customer_id,
    ds.store_id,
    ds.store_full_name AS store_name,
    ds.store_brand AS store_brand,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    cb.recent_activation_date,
    'No Action' AS customer_action_type
FROM _customer_base cb
LEFT JOIN stg.dim_store ds
    ON cb.store_id = ds.store_id
LEFT JOIN _main_data_set mds
    ON cb.customer_id = mds.customer_id
    AND cb.period_month_date = mds.period_month_date
WHERE mds.customer_id IS NULL;

--Pivot the data set by the Period and Customer
CREATE OR REPLACE TEMP TABLE  _pivot_data_set AS
SELECT
    period_month_date,
    customer_id,
    store_id,
    store_name,
    store_brand,
    store_brand_abbr,
    store_country,
    store_region,
    recent_activation_date,
    "'Purchased Order - Cash'" AS purchased_order_cash,
    "'Purchased Order - Non Cash'" AS purchased_order_non_cash,
    "'Billed Credit - Succeeded'" AS billed_credit_succeeded,
    "'Billed Credit - Pending'" AS billed_credit_pending,
    "'Billed Credit - Failed'" AS billed_credit_failed,
    "'Billed Membership Fee - Succeeded'" AS billed_membership_fee_succeeded,
    "'Billed Membership Fee - Pending'" AS billed_membership_fee_pending,
    "'Billed Membership Fee - Failed'" AS billed_membership_fee_failed,
    "'Skipped Month'" AS skipped_month,
    "'Cancelled Membership'" AS cancelled_membership,
    "'No Action'" AS no_action,
    "'Purchased Order - Cash'" + "'Purchased Order - Non Cash'" +
    "'Billed Credit - Succeeded'" + "'Billed Credit - Pending'" + "'Billed Credit - Failed'"
        + "'Billed Membership Fee - Succeeded'" + "'Billed Membership Fee - Pending'" + "'Billed Membership Fee - Failed'"
        + "'Skipped Month'" + "'Cancelled Membership'" + "'No Action'" AS total_count
FROM
(
    SELECT
        period_month_date,
        customer_id,
        store_id,
        store_name,
        store_brand,
        store_brand_abbr,
        store_country,
        store_region,
        recent_activation_date,
        customer_action_type
    FROM _main_data_set
) AS src_table
PIVOT
(
    COUNT(customer_action_type)
    FOR customer_action_type in (
                                    'Purchased Order - Cash',
                                    'Purchased Order - Non Cash',
                                    'Billed Credit - Succeeded',
                                    'Billed Credit - Pending',
                                    'Billed Credit - Failed',
                                    'Billed Membership Fee - Succeeded',
                                    'Billed Membership Fee - Pending',
                                    'Billed Membership Fee - Failed',
                                    'Skipped Month',
                                    'Cancelled Membership',
                                    'No Action'
                                )
) AS PivotTable;

INSERT INTO stg.fact_customer_action_period_stg
(
    period_name,
    period_month_date,
    customer_id,
    store_id,
    store_name,
    store_brand,
    store_brand_abbr,
    store_country,
    store_region,
    customer_action_category,
    vip_cohort_month_date,
    meta_create_datetime,
    meta_update_datetime
 )
SELECT
    p.label AS period_name,
    p.date_period_start::DATE AS period_month_date,
    customer_id,
    store_id,
    store_name,
    store_brand,
    store_brand_abbr,
    store_country,
    store_region,
    CASE
        WHEN no_action = 1 AND total_count = 1 THEN 'No Action'
        WHEN skipped_month >= 1	AND skipped_month + cancelled_membership = total_count THEN 'Skipped Month'
        WHEN purchased_order_cash >= 1 AND purchased_order_cash + cancelled_membership = total_count THEN 'Purchased Order - Cash'
        WHEN purchased_order_non_cash >= 1 AND purchased_order_non_cash + cancelled_membership = total_count THEN 'Purchased Order - Non Cash'
        WHEN billed_credit_succeeded >= 1 AND billed_credit_succeeded + cancelled_membership = total_count THEN 'Billed Credit - Succeeded'
        WHEN billed_credit_pending >= 1 AND billed_credit_pending + cancelled_membership = total_count THEN 'Billed Credit - Pending'
        WHEN billed_credit_failed >= 1 AND billed_credit_failed + cancelled_membership = total_count THEN 'Billed Credit - Failed'
        WHEN billed_membership_fee_succeeded >= 1 AND billed_membership_fee_succeeded + cancelled_membership = total_count THEN 'Billed Membership Fee - Succeeded'
        WHEN billed_membership_fee_pending >= 1 AND billed_membership_fee_pending + cancelled_membership = total_count THEN 'Billed Membership Fee - Pending'
        WHEN billed_membership_fee_failed >= 1 AND billed_membership_fee_failed + cancelled_membership = total_count THEN 'Billed Membership Fee - Failed'
        WHEN cancelled_membership >= 1 AND cancelled_membership = total_count THEN 'Cancelled Membership'
        WHEN skipped_month >= 1 AND purchased_order_cash >= 1 AND purchased_order_cash + skipped_month + cancelled_membership = total_count THEN 'Skipped Month / Purchased Order - Cash'
        WHEN skipped_month >= 1 AND purchased_order_non_cash >= 1 AND purchased_order_non_cash + skipped_month + cancelled_membership = total_count THEN 'Skipped Month / Purchased Order - Non Cash'
        WHEN billed_credit_succeeded >= 1 AND purchased_order_cash >= 1	AND purchased_order_cash + billed_credit_succeeded + cancelled_membership = total_count THEN 'Billed Credit - Succeeded / Purchased Order - Cash'
        WHEN billed_credit_succeeded >= 1 AND purchased_order_non_cash >= 1	AND purchased_order_non_cash + billed_credit_succeeded + cancelled_membership = total_count	THEN 'Billed Credit - Succeeded / Purchased Order - Non Cash'
        WHEN billed_membership_fee_succeeded >= 1 AND purchased_order_cash >= 1	AND purchased_order_cash + billed_membership_fee_succeeded + cancelled_membership = total_count THEN 'Billed Membership Fee - Succeeded / Purchased Order - Cash'
        WHEN billed_membership_fee_succeeded >= 1 AND purchased_order_non_cash >= 1	AND purchased_order_non_cash + billed_membership_fee_succeeded + cancelled_membership = total_count	THEN 'Billed Membership Fee - Succeeded / Purchased Order - Non Cash'
        WHEN purchased_order_cash >= 1 AND purchased_order_non_cash >= 1 AND purchased_order_cash + purchased_order_non_cash + cancelled_membership = total_count THEN 'Purchased Order - Cash / Purchased Order - Non Cash'
        WHEN billed_credit_failed >= 1 AND purchased_order_cash >= 1 AND billed_credit_failed + purchased_order_cash + cancelled_membership = total_count THEN 'Billed Credit - Failed / Purchased Order - Cash'
        WHEN billed_credit_failed >= 1 AND purchased_order_non_cash >= 1 AND billed_credit_failed + purchased_order_non_cash + cancelled_membership = total_count THEN 'Billed Credit - Failed / Purchased Order - Non Cash'
        WHEN billed_membership_fee_failed >= 1 AND purchased_order_cash >= 1 AND billed_membership_fee_failed + purchased_order_cash + cancelled_membership = total_count THEN 'Billed Membership Fee - Failed / Purchased Order - Cash'
        WHEN billed_membership_fee_failed >= 1 AND purchased_order_non_cash >= 1 AND billed_membership_fee_failed + purchased_order_non_cash + cancelled_membership = total_count THEN 'Billed Membership Fee - Failed / Purchased Order - Non Cash'
        WHEN skipped_month >= 1 AND purchased_order_cash >= 1 AND purchased_order_non_cash >= 1	AND skipped_month + purchased_order_cash + purchased_order_non_cash + cancelled_membership = total_count THEN 'Skipped Month / Purchased Order - Cash / Purchased Order - Non Cash'
        WHEN billed_credit_succeeded >= 1 AND purchased_order_cash >= 1 AND purchased_order_non_cash >= 1 AND billed_credit_succeeded + purchased_order_cash + purchased_order_non_cash + cancelled_membership = total_count THEN 'Billed Credit - Succeeded / Purchased Order - Cash / Purchased Order - Non Cash'
        WHEN billed_credit_failed >= 1 AND purchased_order_cash >= 1 AND purchased_order_non_cash >= 1 AND billed_credit_failed + purchased_order_cash + purchased_order_non_cash + cancelled_membership = total_count THEN 'Billed Credit - Failed / Purchased Order - Cash / Purchased Order - Non Cash'
        WHEN billed_membership_fee_succeeded >= 1 AND purchased_order_cash >= 1 AND purchased_order_non_cash >= 1 AND billed_membership_fee_succeeded + purchased_order_cash + purchased_order_non_cash + cancelled_membership = total_count THEN 'Billed Membership Fee - Succeeded / Purchased Order - Cash / Purchased Order - Non Cash'
        WHEN billed_membership_fee_failed >= 1 AND purchased_order_cash >= 1 AND purchased_order_non_cash >= 1 AND billed_membership_fee_failed + purchased_order_cash + purchased_order_non_cash + cancelled_membership = total_count THEN 'Billed Membership Fee - Failed / Purchased Order - Cash / Purchased Order - Non Cash'
        WHEN purchased_order_cash >= 1 THEN 'Purchase Order - Cash'
        WHEN billed_credit_succeeded >= 1 THEN 'Billed Credit - Succeeded'
        WHEN billed_membership_fee_succeeded >= 1 THEN 'Billed Membership Fee - Succeeded'
        ELSE 'Other'
    END AS customer_action_category,
    date_from_parts(year(recent_activation_date), month(recent_activation_date), 1) AS vip_cohort_month_date,
    $execution_start_time,
    $execution_start_time
FROM _pivot_data_set rds
JOIN  lake_consolidated.ultra_merchant.period p
ON rds.period_month_date = p.date_period_start::DATE
AND p.type = 'monthly';
