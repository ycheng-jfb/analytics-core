SET target_table = 'reporting_prod.shared.daily_platform_sessions_orders';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(public.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE,
                                  FALSE));

MERGE INTO public.meta_table_dependency_watermark AS t
    USING (
        SELECT
            $target_table AS table_name,
            NULLIF(dependent_table_name,$target_table) AS dependent_table_name,
            new_high_watermark_datetime AS new_high_watermark_datetime
            FROM (SELECT -- For self table
                      NULL AS dependent_table_name,
                      MAX(meta_update_datetime) AS new_high_watermark_datetime
               FROM shared.daily_platform_sessions_orders
               UNION ALL
               SELECT 'reporting_prod.shared.sessions_by_platform' AS dependent_table_name,
                      MAX(meta_update_datetime)                    AS new_high_watermark_datetime
               FROM shared.sessions_by_platform
               UNION ALL
               SELECT 'reporting_prod.shared.sessions_order_stg' AS dependent_table_name,
                      MAX(meta_update_datetime)        AS new_high_watermark_datetime
               FROM shared.sessions_order_stg
            ) AS h
        ) AS s
    ON t.table_name = s.table_name
        AND EQUAL_NULL(t.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED
        AND NOT EQUAL_NULL(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
        THEN UPDATE SET
            t.new_high_watermark_datetime = s.new_high_watermark_datetime,
            t.meta_update_datetime = CURRENT_TIMESTAMP::timestamp_ltz(3)
    WHEN NOT MATCHED
    THEN INSERT (
        table_name,
        dependent_table_name,
        high_watermark_datetime,
        new_high_watermark_datetime
        )
    VALUES (
        s.table_name,
        s.dependent_table_name,
        '1900-01-01'::timestamp_ltz,
        s.new_high_watermark_datetime
    );

SET wm_reporting_prod_shared_sessions_by_platform = public.udf_get_watermark($target_table,
                                                                             'reporting_prod.shared.sessions_by_platform');
SET wm_reporting_prod_shared_sessions_order_stg = public.udf_get_watermark($target_table, 'reporting_prod.shared.sessions_order_stg');

CREATE OR REPLACE TEMP TABLE _daily_platform_sessions_orders__base
(
    session_date DATE
);

-- full refresh
INSERT INTO _daily_platform_sessions_orders__base
SELECT DISTINCT session_date
FROM shared.sessions_by_platform
WHERE $is_full_refresh = TRUE;

-- incremental refresh
INSERT INTO _daily_platform_sessions_orders__base
SELECT DISTINCT session_date
FROM (SELECT session_date
      FROM shared.sessions_by_platform
      WHERE meta_update_datetime > $wm_reporting_prod_shared_sessions_by_platform
      UNION ALL
      SELECT date
      FROM shared.sessions_order_stg
      WHERE meta_update_datetime > $wm_reporting_prod_shared_sessions_order_stg) AS incr
WHERE $is_full_refresh = FALSE;

CREATE OR REPLACE TEMPORARY TABLE _daily_platform_sessions_orders__agg_orders AS
SELECT s.session_id,
       s.order_platform,
       s.operating_system,
       s.order_status,
       s.is_activating,
       s.membership_state,
       IFF(s.order_platform = 'Mobile App', 1, 0) AS is_mobile_app_order,
       COUNT(DISTINCT COALESCE(s.customer_id, s.visitor_id))              AS unique_purchasers,
       SUM(s.orders)                              AS orders,
       SUM(s.vip_activations)                     AS vip_activations,
       SUM(s.units)                               AS units,
       SUM(s.subtotal)                            AS subtotal,
       SUM(s.discount)                            AS discount,
       SUM(s.shipping_revenue)                    AS shipping_revenue,
       SUM(s.payment_transaction)                 AS payment_transaction,
       SUM(s.tax)                                 AS tax,
       SUM(s.product_gross_revenue)               AS product_gross_revenue,
       SUM(s.cash_collected)                      AS cash_collected,
       SUM(s.cash_credit)                         AS cash_credit,
       SUM(s.non_cash_credit)                     AS non_cash_credit,
       SUM(s.total_cost_product)                  AS total_cost_product,
       SUM(s.estimated_shipping_cost)             AS estimated_shipping_cost,
       SUM(s.product_margin_pre_return)           AS product_margin_pre_return,
       SUM(s.vip_activations_1hr)                 AS vip_activations_1hr,
       SUM(s.vip_activations_24hr)                AS vip_activations_24hr,
       SUM(s.vip_activations_7_day)               AS vip_activations_7_day,
       SUM(s.vip_activations_1_month)             AS vip_activations_1_month,
       SUM(s.membership_credits_redeemed_count)   AS membership_credits_redeemed_count,
       SUM(s.membership_credits_redeemed_amount)  AS membership_credits_redeemed_amount,
       SUM(s.membership_token_redeemed_count)     AS membership_token_redeemed_count,
       SUM(s.membership_token_redeemed_amount)    AS membership_token_redeemed_amount
FROM shared.sessions_order_stg s
WHERE s.date IN (SELECT session_date FROM _daily_platform_sessions_orders__base)
GROUP BY s.session_id,
         s.order_platform,
         s.operating_system,
         s.order_status,
         s.is_activating,
         s.membership_state,
         IFF(s.order_platform = 'Mobile App', 1, 0);

CREATE OR REPLACE TEMP TABLE _daily_platform_sessions_orders__agg_orders_stg AS
SELECT DISTINCT CAST(s.session_local_datetime AS DATE)                                          AS date,
                s.customer_id,
                s.visitor_id,
                s.session_id,
                COALESCE(s.user_segment, 'Unknown')                                             AS user_segment,
                EXTRACT(DAY FROM s.session_local_datetime)                                      AS day,
                s.store_brand_name                                                              AS brand,
                s.store_region_abbr                                                             AS region,
                s.store_country_abbr                                                            AS country,
                s.store_group                                                                   AS store,
                CONCAT(s.store_brand_abbr, s.store_country_abbr)                                AS storeabbr,
                s.is_mobile_app_store                                                           AS ismobileappstore,
                COALESCE(o.order_platform, s.session_platform)                                  AS platform,
                IFF(o.order_platform = 'Mobile App' OR s.session_platform = 'Mobile App', 'Mobile App',
                    s.raw_device)                                                                  deviceraw,
                IFF(s.operating_system ILIKE 'Mac%%' OR s.operating_system = 'iOS', 'Apple',
                    'Other')                                                                    AS operatingsystemgrouped,
                s.operating_system                                                              AS operatingsystemraw,
                IFF(s.store_brand_name = 'Fabletics', s.customer_gender, NULL)                  AS customergender,
                CASE WHEN s.membership_state = 'VIP' THEN CONCAT('M', s.monthly_vip_tenure) END AS viptenure,
                CASE
                    WHEN s.membership_state = 'VIP' AND s.monthly_vip_tenure BETWEEN 1 AND 12
                        THEN CONCAT('M', s.monthly_vip_tenure)
                    WHEN s.membership_state = 'VIP' AND s.monthly_vip_tenure BETWEEN 13 AND 24 THEN 'M13-24'
                    WHEN s.membership_state = 'VIP' AND s.monthly_vip_tenure >= 25 THEN 'M25+'
                    ELSE 'Non-VIP' END                                                          AS viptenuregroup,
                s.daily_lead_tenure                                                             AS leadtenure,
                CASE
                    WHEN s.membership_state = 'Lead' AND (daily_lead_tenure + 1) = 1 THEN 'D1'
                    WHEN s.membership_state = 'Lead' AND (daily_lead_tenure + 1) BETWEEN 2 AND 7 THEN 'D2-7'
                    WHEN s.membership_state = 'Lead' AND (daily_lead_tenure + 1) BETWEEN 8 AND 30 THEN 'D8-30'
                    WHEN s.membership_state = 'Lead' AND (daily_lead_tenure + 1) BETWEEN 31 AND 60 THEN 'D31-60'
                    WHEN s.membership_state = 'Lead' AND (daily_lead_tenure + 1) BETWEEN 61 AND 90 THEN 'D61-90'
                    WHEN s.membership_state = 'Lead' AND (daily_lead_tenure + 1) BETWEEN 91 AND 180 THEN 'D91-180'
                    WHEN s.membership_state = 'Lead' AND (daily_lead_tenure + 1) BETWEEN 181 AND 365 THEN 'D181-365'
                    WHEN s.membership_state = 'Lead' AND (daily_lead_tenure + 1) BETWEEN 366 AND 730 THEN 'D366-730'
                    WHEN s.membership_state = 'Lead' AND (daily_lead_tenure + 1) >= 731 THEN 'D731+'
                    ELSE 'Non-Lead' END                                                         AS leadtenuregroup,
                s.membership_state                                                              AS membershipstate,
                IFF(s.is_lead_registration_action = TRUE, 1, 0)                                 AS isleadregistration,
                o.is_activating                                                                 AS isactivating,
                IFF(is_atb_action = TRUE, 1, 0)                                                 AS isatbsession,
                s.channel,
                s.subchannel,
                s.channel_adj,
                s.subchannel_adj,
                COALESCE(o.unique_purchasers, 0)                                                AS uniquepurchasers,
                IFF(s.membership_state = 'Prospect'
                        AND s.is_lead_registration_action = TRUE, 1, 0)                         AS leadregistrations,
                COALESCE(o.orders, 0)                                                           AS orders,
                COALESCE(o.vip_activations, 0)                                                  AS vipactivations,
                COALESCE(o.vip_activations_1hr, 0)                                              AS vipactivations1hr,
                COALESCE(o.vip_activations_24hr, 0)                                             AS vipactivations24hr,
                COALESCE(o.vip_activations_7_day, 0)                                            AS vipactivations7day,
                COALESCE(o.vip_activations_1_month, 0)                                          AS vipactivations1month,
                COALESCE(o.units, 0)                                                            AS units,
                COALESCE(o.subtotal, 0)                                                         AS subtotal,
                COALESCE(o.discount, 0)                                                         AS discount,
                COALESCE(o.shipping_revenue, 0)                                                 AS shippingrevenue,
                COALESCE(o.product_gross_revenue, 0)                                            AS productgrossrevenue,
                COALESCE(o.cash_collected, 0)                                                   AS cashcollected,
                COALESCE(o.cash_credit, 0)                                                      AS cashcredit,
                COALESCE(o.non_cash_credit, 0)                                                  AS noncashcredit,
                COALESCE(o.total_cost_product, 0)                                               AS totalcostproduct,
                COALESCE(o.estimated_shipping_cost, 0)                                          AS estimatedshippingcost,
                COALESCE(o.product_margin_pre_return, 0)                                        AS productgrossmargin,
                COALESCE(o.membership_credits_redeemed_count + membership_token_redeemed_count,
                         0)                                                                     AS membershipcreditredemptions,
                COALESCE(o.membership_credits_redeemed_amount + membership_token_redeemed_amount,
                         0)                                                                     AS membershipcreditamount,
                IFF(s.is_skip_month_action = TRUE, 1, 0)                                        AS skips,
                s.is_scrubs_gateway,
                s.is_scrubs_customer,
                s.is_scrubs_action,
                s.is_scrubs_session,
                s.is_paid_media
FROM _daily_platform_sessions_orders__base b
    LEFT JOIN shared.sessions_by_platform AS s
        ON b.session_date = CAST(s.session_local_datetime AS DATE)
    LEFT JOIN _daily_platform_sessions_orders__agg_orders AS o
         ON s.session_id = o.session_id;

ALTER TABLE _daily_platform_sessions_orders__base CLUSTER BY (session_date);

DELETE
FROM shared.daily_platform_sessions_orders t
WHERE t.date IN (SELECT session_date FROM _daily_platform_sessions_orders__base);

INSERT INTO shared.daily_platform_sessions_orders (
    date,
    day,
    brand,
    region,
    country,
    store,
    storeabbr,
    ismobileappstore,
    platform,
    deviceraw,
    operatingsystemgrouped,
    operatingsystemraw,
    customergender,
    viptenure,
    viptenuregroup,
    leadtenure,
    leadtenuregroup,
    membershipstate,
    isleadregistration,
    isactivating,
    isatbsession,
    channel,
    subchannel,
    channel_adj,
    subchannel_adj,
    sessions,
    uniquecustomers,
    uniquepurchasers,
    leadregistrations,
    orders,
    vipactivations,
    vipactivations1hr,
    vipactivations24hr,
    vipactivations7day,
    vipactivations1month,
    units,
    subtotal,
    discount,
    shippingrevenue,
    productgrossrevenue,
    cashcollected,
    cashcredit,
    noncashcredit,
    totalcostproduct,
    estimatedshippingcost,
    productgrossmargin,
    membershipcreditredemptions,
    membershipcreditamount,
    skips,
    is_scrubs_gateway,
    is_scrubs_customer,
    is_scrubs_action,
    is_scrubs_session,
    is_paid_media,
    user_segment,
    meta_create_datetime,
    meta_update_datetime
)
SELECT date,
       day,
       brand,
       region,
       country,
       store,
       storeabbr,
       ismobileappstore,
       platform,
       deviceraw,
       operatingsystemgrouped,
       operatingsystemraw,
       customergender,
       viptenure,
       viptenuregroup,
       leadtenure,
       leadtenuregroup,
       membershipstate,
       isleadregistration,
       isactivating,
       isatbsession,
       channel,
       subchannel,
       channel_adj,
       subchannel_adj,
       COUNT(s.session_id)              AS sessions,
       COUNT(DISTINCT COALESCE(s.customer_id, s.visitor_id))    AS uniquecustomers,
       SUM(uniquepurchasers)            AS uniquepurchasers,
       SUM(leadregistrations)           AS leadregistrations,
       SUM(orders)                      AS orders,
       SUM(vipactivations)              AS vipactivations,
       SUM(vipactivations1hr)           AS vipactivations1hr,
       SUM(vipactivations24hr)          AS vipactivations24hr,
       SUM(vipactivations7day)          AS vipactivations7day,
       SUM(vipactivations1month)        AS vipactivations1month,
       SUM(units)                       AS units,
       SUM(subtotal)                    AS subtotal,
       SUM(discount)                    AS discount,
       SUM(shippingrevenue)             AS shippingrevenue,
       SUM(productgrossrevenue)         AS productgrossrevenue,
       SUM(cashcollected)               AS cashcollected,
       SUM(cashcredit)                  AS cashcredit,
       SUM(noncashcredit)               AS noncashcredit,
       SUM(totalcostproduct)            AS totalcostproduct,
       SUM(estimatedshippingcost)       AS estimatedshippingcost,
       SUM(productgrossmargin)          AS productgrossmargin,
       SUM(membershipcreditredemptions) AS membershipcreditredemptions,
       SUM(membershipcreditamount)      AS membershipcreditamount,
       SUM(skips)                       AS skips,
       is_scrubs_gateway,
       is_scrubs_customer,
       is_scrubs_action,
       is_scrubs_session,
       is_paid_media,
       user_segment,
       $execution_start_time            AS meta_create_datetime,
       $execution_start_time            AS meta_update_datetime
FROM _daily_platform_sessions_orders__agg_orders_stg s
GROUP BY date,
         day,
         brand,
         region,
         country,
         store,
         storeabbr,
         ismobileappstore,
         platform,
         deviceraw,
         operatingsystemgrouped,
         operatingsystemraw,
         customergender,
         viptenure,
         viptenuregroup,
         leadtenure,
         leadtenuregroup,
         membershipstate,
         isleadregistration,
         isactivating,
         isatbsession,
         channel,
         subchannel,
         channel_adj,
         subchannel_adj,
         is_scrubs_gateway,
         is_scrubs_customer,
         is_scrubs_action,
         is_scrubs_session,
         is_paid_media,
         user_segment;

UPDATE public.meta_table_dependency_watermark
SET high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime    = CURRENT_TIMESTAMP::timestamp_ltz(3)
WHERE table_name = $target_table;
