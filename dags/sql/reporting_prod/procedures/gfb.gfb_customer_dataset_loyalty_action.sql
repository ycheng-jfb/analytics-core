set low_watermark_ltz = (
    select
        max(a.MONTH_DATE)
    from REPORTING_PROD.GFB.gfb_customer_dataset_base a
    );


CREATE OR REPLACE TEMP TABLE _loyalty_points_base AS
SELECT
    cd.customer_id
    ,cd.month_date
    ,s.time_zone
FROM REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_BASE cd
JOIN EDW_PROD.reference.store_timezone s
    ON s.store_id = cd.store_id
where
    cd.MONTH_DATE >= $low_watermark_ltz
    and cd.MONTH_DATE < date_trunc('MONTH', current_date());


CREATE OR REPLACE TEMPORARY TABLE _loyalty_points_pre_stg AS
SELECT
   base.customer_id,
   base.month_date,
   SUM(CASE WHEN points > 0 THEN points ELSE 0 END) AS points_earned,
   ABS(SUM(CASE WHEN mrt.points < 0 AND mrtt.label NOT ILIKE '%Expiration' AND mrtt.label NOT ILIKE 'Membership Cancellation' THEN mrt.points  ELSE 0 END))  AS points_redeemed,
   ABS(SUM(CASE WHEN mrtt.label ILIKE '%Expiration' THEN points ELSE 0 END)) AS points_expired,
   SUM(mrt.points) AS points_available,
   max(CASE WHEN mrtt.type ILIKE 'debit' AND mrtt.label NOT ILIKE '%Expiration' AND mrtt.label NOT ILIKE 'Membership Cancellation' THEN mrt.datetime_added ELSE NULL END) AS last_redeemed_points_date
FROM _loyalty_points_base base
JOIN  lake_jfb_view.ultra_merchant.membership m
    ON m.customer_id = base.customer_id
LEFT JOIN lake_jfb_view.ultra_merchant.membership_reward_transaction mrt
    ON mrt.membership_id = m.membership_id
    AND DATE_TRUNC('MONTH', CONVERT_TIMEZONE(base.time_zone, mrt.datetime_added)::TIMESTAMP_NTZ::date) = base.month_date
LEFT JOIN lake_jfb_view.ultra_merchant.membership_reward_transaction_type mrtt
    ON mrtt.membership_reward_transaction_type_id = mrt.membership_reward_transaction_type_id
GROUP BY
    base.customer_id,
    base.month_date;


delete from REPORTING_PROD.GFB.gfb_customer_dataset_loyalty_action a
where
    a.MONTH_DATE >= $low_watermark_ltz;


insert into REPORTING_PROD.GFB.gfb_customer_dataset_loyalty_action
SELECT
    customer_id,
    month_date,
    points_earned,
    points_redeemed,
    points_expired,
    points_available,
    last_redeemed_points_date
FROM _loyalty_points_pre_stg;
