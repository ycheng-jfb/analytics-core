/*
need to get this data AS far back AS possible to get original lead registration type
AND map a prospect vip activation to original lead reg
below will have unique customer ID counts, not session ID
*/
CREATE OR REPLACE TEMPORARY TABLE _lead_registration as
SELECT
    customer_id,
    session_id,
    is_reactivated_lead,
    registration_local_datetime,
    leadregistrationtype
FROM (
    SELECT DISTINCT
        c.customer_id,
        c.session_id,
        c.is_reactivated_lead,
        c.registration_local_datetime,
        s.leadregistrationtype
    FROM edw_prod.data_model.fact_registration AS c
    JOIN (
        SELECT
            s.session_id,
            CASE
                WHEN s.is_quiz_registration_action = TRUE
                    THEN 'Quiz'
                WHEN s.is_speedy_registration_action = TRUE
                    THEN 'Speedy Sign-Up'
                WHEN s.is_skip_quiz_registration_action = TRUE
                    THEN 'Skip Quiz'
                ELSE NULL
            END AS leadregistrationtype
        FROM staging.site_visit_metrics AS s
        WHERE s.is_lead_registration_action = TRUE
    ) AS s
    ON c.session_id = s.session_id
    ) AS A
ORDER BY customer_id ASC;

CREATE OR REPLACE TEMPORARY TABLE _order_base as
SELECT
    fo.session_id,
    fo.customer_id,
    fo.order_id,
    order_local_datetime,
    os.order_status,
    ops.order_processing_status_code,
    ops.order_processing_status,
    order_sales_channel_l1,
    order_sales_channel_l2,
    IFF(shipping_revenue_local_amount = 0, TRUE, FALSE) AS is_free_shipping,
    is_border_free_order,
    is_ps_order,
    is_retail_ship_only_order,
    is_preorder,
    is_product_seeding_order,
    order_classification_l1,
    order_classification_l2,
    membership_order_type_l1,
    membership_order_type_l2,
    membership_order_type_l3,
    is_vip,
    --,IS_VIP_ACTIVATION_FROM_REACTIVATED_LEAD
    1 AS orders,
    fo.unit_count,
    fo.product_subtotal_local_amount * fo.order_date_usd_conversion_rate AS product_subtotal_incl_tariff, --includes tariff
    fo.subtotal_excl_tariff_local_amount * fo.order_date_usd_conversion_rate AS product_subtotal_excl_tariff, --excludes tariff
    fo.tariff_revenue_local_amount * fo.order_date_usd_conversion_rate AS tariff,
    fo.shipping_revenue_local_amount * fo.order_date_usd_conversion_rate AS shipping_revenue, --includes shipping discount
    fo.shipping_revenue_before_discount_local_amount * fo.order_date_usd_conversion_rate AS shipping_revenue_before_discount,
    fo.product_discount_local_amount * fo.order_date_usd_conversion_rate AS product_discount,
    fo.shipping_discount_local_amount * fo.order_date_usd_conversion_rate AS shipping_discount,
    fo.product_gross_revenue_local_amount * fo.order_date_usd_conversion_rate AS product_gross_revenue, --gaap gross revenue
    fo.product_gross_revenue_excl_shipping_local_amount * fo.order_date_usd_conversion_rate AS product_gross_revenue_excl_shipping,
    fo.amount_to_pay * fo.order_date_usd_conversion_rate AS amount_to_pay,
    fo.payment_transaction_local_amount * fo.order_date_usd_conversion_rate AS payment_transaction,
    fo.tax_local_amount * fo.order_date_usd_conversion_rate AS tax,
    fo.cash_gross_revenue_local_amount * fo.order_date_usd_conversion_rate AS cash_gross_revenue, --cash collected
    fo.product_margin_pre_return_local_amount * fo.order_date_usd_conversion_rate AS product_margin_pre_return, --acq gaap gross margin
    fo.estimated_shipping_supplies_cost_local_amount * fo.order_date_usd_conversion_rate AS estimated_shipping_supplies_cost,
    fo.estimated_landed_cost_local_amount * fo.order_date_usd_conversion_rate AS estimated_landed_cost,
    fo.shipping_cost_local_amount * fo.order_date_usd_conversion_rate AS shipping_cost,
    fo.non_cash_credit_local_amount * fo.order_date_usd_conversion_rate AS non_cash_credit_amount,
    fo.non_cash_credit_count AS non_cash_credit_count,
    fo.cash_credit_local_amount * fo.order_date_usd_conversion_rate AS cash_credit_amount,
    fo.cash_membership_credit_count AS cash_membership_credit_count,
    fo.cash_refund_credit_local_amount * fo.order_date_usd_conversion_rate AS cash_refund_credit_amount,
    fo.cash_refund_credit_count AS cash_refund_credit_count,
    fo.cash_giftcard_credit_local_amount * fo.order_date_usd_conversion_rate AS cash_giftcard_credit_amount,
    fo.cash_giftcard_credit_count AS cash_giftcard_credit_count,
    fo.cash_giftco_credit_local_amount * fo.order_date_usd_conversion_rate AS cash_giftco_credit_amount,
    fo.cash_giftco_credit_count AS cash_giftco_credit_count,
    fo.token_local_amount * fo.order_date_usd_conversion_rate AS token_amount,
    fo.token_count AS token_count
FROM shared.session_refresh_base AS s
        -- JOIN _session_ids si ON si.session_id = s.session_id
    JOIN edw_prod.data_model.fact_order fo
        ON fo.session_id = s.session_id
    JOIN edw_prod.data_model.dim_order_status AS os
        ON os.order_status_key = fo.order_status_key
    JOIN edw_prod.data_model.dim_order_sales_channel AS osc
        ON osc.order_sales_channel_key = fo.order_sales_channel_key
    JOIN edw_prod.data_model.dim_order_membership_classification AS mem
        ON mem.order_membership_classification_key = fo.order_membership_classification_key
    JOIN edw_prod.data_model.dim_order_processing_status AS ops
        ON ops.order_processing_status_key = fo.order_processing_status_key
WHERE os.order_status IN ('Success', 'Pending')
    AND order_sales_channel_l1 = 'Online Order'
    AND order_classification_l1 = 'Product Order'
ORDER BY fo.session_id ASC;

-------------

CREATE OR REPLACE TEMPORARY TABLE _orders AS
SELECT
    o.session_id,
    COUNT(IFF(o.shipping_discount = 0,o.ORDER_ID,NULL)) AS orders_w_free_shipping,
    SUM(IFF(o.membership_order_type_L1 = 'Activating VIP', o.orders,0)) AS activating_product_order,
    SUM(IFF(o.membership_order_type_L1 = 'NonActivating', o.orders,0)) AS nonactivating_product_order,
    SUM(IFF(o.membership_order_type_L1 = 'Activating VIP', o.unit_count,0)) AS activating_product_units,
    SUM(IFF(o.membership_order_type_L1 = 'NonActivating', o.unit_count,0)) AS nonactivating_product_units,
    SUM(IFF(o.membership_order_type_L1 = 'Activating VIP', o.product_subtotal_incl_tariff,0)) AS activating_product_subtotal,
    SUM(IFF(o.membership_order_type_L1 = 'NonActivating', o.product_subtotal_incl_tariff,0)) AS nonactivating_product_subtotal,
    SUM(IFF(o.membership_order_type_L1 = 'Activating VIP', o.product_discount + shipping_discount,0)) AS activating_product_discount,
    SUM(IFF(o.membership_order_type_L1 = 'NonActivating', o.product_discount + shipping_discount,0)) AS nonactivating_product_discount,
    SUM(IFF(o.membership_order_type_L1 = 'Activating VIP', o.product_gross_revenue,0)) AS activating_product_gross_revenue_usd,
    SUM(IFF(o.membership_order_type_L1 = 'NonActivating', o.product_gross_revenue,0)) AS nonactivating_product_gross_revenue_usd,
    SUM(IFF(o.membership_order_type_L1 = 'Activating VIP', o.product_margin_pre_return,0)) AS activating_product_margin_pre_return_usd,
    SUM(IFF(o.membership_order_type_L1 = 'NonActivating', o.product_margin_pre_return,0)) AS nonactivating_product_margin_pre_return_usd,
    SUM(IFF(o.membership_order_type_L1 = 'Activating VIP', o.cash_gross_revenue,0)) AS activating_cash_gross_revenue_usd,
    SUM(IFF(o.membership_order_type_L1 = 'NonActivating', o.cash_gross_revenue,0)) AS nonactivating_cash_gross_revenue_usd
FROM _order_base AS o
GROUP BY o.session_id
ORDER BY o.session_id ASC;


-------------------------------------------------------
--getting order data FROM lead registrations to map back to original lead registration session ID
CREATE OR REPLACE TEMPORARY TABLE _orders_by_lead_registration AS
SELECT *
FROM (
    SELECT
        l.customer_id,
        l.session_id AS session_id_reg,
        fa.session_id AS session_id_act,
        fa.order_id,
        registration_local_datetime,
        fa.activation_local_datetime,
        leadregistrationtype,
        is_reactivated_lead,
        membership_type AS activating_membership_type,
        rank() over (partition by l.customer_id order by fa.order_id ASC) AS rnk_order,
        SUM(CASE WHEN datediff(hour,registration_local_datetime, fa.activation_local_datetime) <= 24
            THEN activating_product_order
            ELSE 0 END)
        AS activating_product_orders_24hr_FROM_leads,
        SUM(CASE WHEN datediff(day,registration_local_datetime, fa.activation_local_datetime) <= 7
                THEN activating_product_order
                ELSE 0 END)
        AS activating_product_orders_d7_FROM_leads,
        SUM(CASE
            WHEN datediff(month,registration_local_datetime, fa.activation_local_datetime) <= 1
            THEN activating_product_order
            ELSE 0 END)
        AS activating_product_orders_m1_FROM_leads,
        SUM(activating_product_order) AS activating_product_order_FROM_leads,
        SUM(activating_product_units) AS activating_product_units_FROM_leads,
        SUM(activating_product_subtotal) AS activating_product_subtotal_FROM_leads,
        SUM(activating_product_discount) AS activating_product_discount_FROM_leads,
        SUM(activating_product_gross_revenue_usd) AS activating_product_gross_revenue_usd_FROM_leads,
        SUM(activating_product_margin_pre_return_usd) AS activating_product_margin_pre_return_usd_FROM_leads
    FROM _lead_registration AS l
    JOIN edw_prod.data_model.fact_activation AS fa
        ON l.customer_id = fa.customer_id
    JOIN _orders AS o
        ON fa.session_id = o.session_id
    WHERE fa.SOURCE_ACTIVATION_LOCAL_DATETIME >= registration_local_datetime --AND l.session_id = 8001273689
    GROUP BY l.customer_id,
        l.session_id,
        fa.session_id,
        fa.order_id,
        leadregistrationtype,
        is_reactivated_lead,
        membership_type,
        registration_local_datetime,
        fa.activation_local_datetime
     ) AS A
WHERE rnk_order = 1
ORDER BY session_id_reg ASC
;

CREATE OR REPLACE TEMPORARY TABLE _stg_session_single_view_media
AS
  SELECT
    b.session_id,
    visitor_id,
    b.customer_id,
    b.store_id,
    store_brand AS storebrand,
    store_region AS storeregion,
    store_country AS storecountry,
    b.session_local_datetime AS sessionlocaldatetime,
    b.membership_state AS membershipstate,
    b.user_segment,
    IFF(membership_state = 'Lead', 'D' || daily_lead_tenure, 'Non Lead') AS leadtenuredaily,
    CASE
          WHEN membership_state = 'Lead' AND daily_lead_tenure = 1 THEN 'D1'
          WHEN membership_state = 'Lead' AND daily_lead_tenure between 2 AND 7 THEN 'D2-7'
          WHEN membership_state = 'Lead' AND daily_lead_tenure between 8 AND 30 THEN 'D8-30'
          WHEN membership_state = 'Lead' AND daily_lead_tenure between 31 AND 60 THEN 'D31-60'
          WHEN membership_state = 'Lead' AND daily_lead_tenure between 61 AND 90 THEN 'D61-90'
          WHEN membership_state = 'Lead' AND daily_lead_tenure between 91 AND 180 THEN 'D91-180'
          WHEN membership_state = 'Lead' AND daily_lead_tenure between 181 AND 365 THEN 'D181-365'
          WHEN membership_state = 'Lead' AND daily_lead_tenure between 366 AND 730 THEN 'D366-730'
          WHEN membership_state = 'Lead' AND daily_lead_tenure >= 731 THEN 'D731+'
          ELSE 'Non-Lead' end
      AS leadtenuredailygroup,
      CASE
          WHEN membership_state = 'Lead' AND monthly_vip_tenure <= 12 THEN 'M' || monthly_vip_tenure
          WHEN membership_state = 'VIP' AND monthly_vip_tenure between 13 AND 24 THEN 'M13-24'
          WHEN membership_state = 'VIP' AND monthly_vip_tenure >= 25 THEN 'M25+'
          ELSE 'Non-VIP' end
      AS viptenuremonthlygroup,
      IFF(membership_state = 'VIP', 'M' || monthly_vip_tenure, 'Non VIP') AS viptenuremonthly,
      Platform,
      CASE
          WHEN platform in ('Desktop', 'Tablet') THEN 'Desktop'
          WHEN platform in ('Unknown', 'Mobile') THEN 'Mobile'
          ELSE platform end
      AS platformraw,
    b.browser,
    os AS operatingsystem,
    hdyh,
    b.channel_type,
    b.channel,
    b.subchannel,
    b.last_non_direct_media_channel,
    b.last_non_direct_media_subchannel,
    b.utm_medium AS utmmedium,
    b.utm_source AS utmsource,
    b.utm_campaign AS utmcampaign,
    b.utm_content AS utmcontent,
    COALESCE(b.utm_term, b.ccode) AS utmterm,
    b.dm_gateway_id AS gatewayid,
    b.gateway_code AS dmgcode,
    b.gateway_type AS gatewaytype,
    b.gateway_sub_type AS gatewaysubtype,
    b.gateway_name AS gatewayname,
    b.offer AS gatewayoffer,
    b.gender_featured AS gatewaygender,
    b.ftv_or_rtv AS gatewayftvorrtv,
    b.influencer_name AS gatewayinfluencername,
    b.experience_description AS gatewayexperience,
    gateway_test_name AS gatewaytestname, --testsite
    b.gateway_test_id AS gatewaytestid,
    gateway_test_start_local_datetime AS gatewayteststartlocaldatetime,
    gateway_test_end_local_datetime AS gatewaytestendlocaldatetime,
    gateway_test_description AS gatewaytestdescription,
    gateway_test_hypothesis AS gatewaytesthypothesis,
    gateway_test_results AS gatewaytestresults,
    b.dm_gateway_test_site_id AS lptestcellid,
    b.gateway_test_lp_traffic_split AS gatewaytestlptrafficsplit,
    gateway_test_lp_type AS gatewaytestlptype, --testsitetype
    gateway_test_lp_class AS gatewaytestlpclass,
    gateway_test_lp_location AS gatewaytestlplocation,
    b.label AS lpname,
    b.dm_site_id AS lpid,
    b.gateway_test_lp_type AS lptype,
    is_yitty_gateway AS isyittygateway,
    is_scrubs_gateway AS isscrubsgateway,
    is_scrubs_customer AS isscrubscustomer,
    is_scrubs_action AS isscrubsaction,
    is_scrubs_session AS isscrubssession,
    is_male_gateway AS ismalegateway,
    is_male_session_action AS ismalesessionaction,
    is_male_session AS ismalesession,
    iff(is_male_customer = TRUE, TRUE, FALSE) AS ismalecustomer,
    iff(is_male_customer = TRUE OR is_male_session = TRUE, TRUE, FALSE) AS ismalecustomersessions,
    COALESCE(lead.leadregistrationtype, 'Unknown') AS leadregistrationtype,
    COALESCE(activating_membership_type, 'Non-Activating Order') AS activatingordermembershiptype,
    1                                                  AS sessions,
    IFF(b.customer_id IS NOT NULL, 1, 0)               AS customers,
    IFF(b.visitor_id IS NOT NULL, 1, 0)                AS visitors,
    IFF(is_quiz_start_action = TRUE, 1, 0)             AS quizstarts,
    IFF(is_quiz_complete_action = TRUE, 1, 0)          AS quizcompletes,
    IFF(is_skip_quiz_action = TRUE, 1, 0)              AS quizskips,
    IFF(is_quiz_registration_action = TRUE, 1, 0)      AS quizleads,
    IFF(is_skip_quiz_registration_action = TRUE, 1, 0) AS quizskipleads,
    IFF(is_speedy_registration_action = TRUE, 1, 0)    AS speedyleads,
    IFF(b.is_lead_registration_action = 1, 1, 0)       AS leads, --lead registrations
    IFF(lead2.is_reactivated_lead = TRUE, 1, 0)        AS leadsreactivated,
      --FROM leads
    SUM(IFNULL(lead2.activating_product_orders_24hr_FROM_leads, 0)) AS activatingorders24hrfromleads,
    SUM(IFNULL(lead2.activating_product_orders_d7_FROM_leads, 0)) AS activatingordersd7fromleads,
    SUM(IFNULL(lead2.activating_product_orders_m1_FROM_leads, 0)) AS activatingordersm1fromleads,
    SUM(IFNULL(lead2.activating_product_order_FROM_leads, 0)) AS activatingordersfromleads,
    SUM(IFNULL(lead2.activating_product_units_FROM_leads, 0)) AS activatingunitsfromleads,
    SUM(IFNULL(lead2.activating_product_subtotal_FROM_leads, 0)) AS activatingsubtotalfromleads,
    SUM(IFNULL(lead2.activating_product_discount_FROM_leads, 0)) AS activatingdiscountfromleads,
    SUM(IFNULL(lead2.activating_product_gross_revenue_usd_FROM_leads, 0)) AS activatingproductgrossrevenuefromleads,
    SUM(IFNULL(lead2.activating_product_margin_pre_return_usd_FROM_leads, 0)) AS activatingproductmarginprereturnfromleads,
      -- ON date
    SUM(IFNULL(o.activating_product_order, 0)) AS activatingorders,
    SUM(IFNULL(o.nonactivating_product_order, 0)) AS nonactivatingorders,
    SUM(IFNULL(o.activating_product_units, 0)) AS activatingunits,
    SUM(IFNULL(o.nonactivating_product_units, 0)) AS nonactivatingunits,
    SUM(IFNULL(o.activating_product_subtotal, 0)) AS activatingsubtotal,
    SUM(IFNULL(o.nonactivating_product_subtotal, 0)) AS nonactivatingsubtotal,
    SUM(IFNULL(o.activating_product_discount, 0)) AS activatingdiscount,
    SUM(IFNULL(o.nonactivating_product_discount, 0)) AS nonactivatingdiscount,
    SUM(IFNULL(o.activating_product_gross_revenue_usd, 0)) AS activatingproductgrossrevenue,
    SUM(IFNULL(o.nonactivating_product_gross_revenue_usd, 0)) AS nonactivatingproductgrossrevenue,
    SUM(IFNULL(o.activating_product_margin_pre_return_usd, 0)) AS activatingproductmarginprereturn,
    SUM(IFNULL(o.nonactivating_product_margin_pre_return_usd, 0)) AS nonactivatingproductmarginprereturn,
    SUM(IFNULL(o.activating_cash_gross_revenue_usd, 0)) AS activatingcashgrossrevenue,
    SUM(IFNULL(o.nonactivating_cash_gross_revenue_usd, 0)) AS nonactivatingcashgrossrevenue,
    si.refresh_time AS refreshtime
  FROM shared.session_refresh_base AS b
  JOIN shared.svm_session_ids_stg si
      ON si.session_id = b.session_id
  LEFT JOIN _orders o
      ON b.session_id = o.session_id
  LEFT JOIN _lead_registration AS lead
      ON lead.customer_id = b.customer_id
  LEFT JOIN _orders_by_lead_registration AS lead2
      ON lead2.session_id_reg = b.session_id
      AND lead2.customer_id = b.customer_id
  GROUP BY b.session_id,
        visitor_id,
        b.customer_id,
        b.store_id,
        store_brand,
        store_region,
        store_country,
        b.session_local_datetime,
        b.membership_state,
        b.user_segment,
        IFF(membership_state = 'Lead', 'D' || daily_lead_tenure, 'Non Lead'),
        CASE
           WHEN membership_state = 'Lead' AND daily_lead_tenure = 1 THEN 'D1'
           WHEN membership_state = 'Lead' AND daily_lead_tenure BETWEEN 2 AND 7 THEN 'D2-7'
           WHEN membership_state = 'Lead' AND daily_lead_tenure BETWEEN 8 AND 30 THEN 'D8-30'
           WHEN membership_state = 'Lead' AND daily_lead_tenure BETWEEN 31 AND 60 THEN 'D31-60'
           WHEN membership_state = 'Lead' AND daily_lead_tenure BETWEEN 61 AND 90 THEN 'D61-90'
           WHEN membership_state = 'Lead' AND daily_lead_tenure BETWEEN 91 AND 180 THEN 'D91-180'
           WHEN membership_state = 'Lead' AND daily_lead_tenure BETWEEN 181 AND 365 THEN 'D181-365'
           WHEN membership_state = 'Lead' AND daily_lead_tenure BETWEEN 366 AND 730 THEN 'D366-730'
           WHEN membership_state = 'Lead' AND daily_lead_tenure >= 731 THEN 'D731+'
           ELSE 'Non-Lead' END,
        CASE
           WHEN membership_state = 'Lead' AND monthly_vip_tenure <= 12 THEN 'M' || monthly_vip_tenure
           WHEN membership_state = 'VIP' AND monthly_vip_tenure BETWEEN 13 AND 24 THEN 'M13-24'
           WHEN membership_state = 'VIP' AND monthly_vip_tenure >= 25 THEN 'M25+'
           ELSE 'Non-VIP' END,
        IFF(membership_state = 'VIP', 'M' || MONTHLY_VIP_TENURE, 'Non VIP'),
        Platform,
        CASE
           WHEN platform IN ('Desktop', 'Tablet') THEN 'Desktop'
           WHEN platform IN ('Unknown', 'Mobile') THEN 'Mobile'
           ELSE platform END,
        b.browser,
        os,
        hdyh,
        b.channel_type,
        b.channel,
        b.subchannel,
        b.last_non_direct_media_channel,
        b.last_non_direct_media_subchannel,
        b.utm_medium,
        b.utm_source,
        b.utm_campaign,
        b.utm_content,
        COALESCE(b.utm_term, b.ccode),
        b.dm_gateway_id,
        b.gateway_code,
        b.gateway_type,
        b.gateway_sub_type,
        b.gateway_name,
        b.offer,
        b.gender_featured,
        b.ftv_or_rtv,
        b.influencer_name,
        b.experience_description,
        gateway_test_name,
        b.gateway_test_id,
        gateway_test_start_local_datetime,
        gateway_test_end_local_datetime,
        gateway_test_description,
        gateway_test_hypothesis,
        gateway_test_results,
        b.dm_gateway_test_site_id,
        b.gateway_test_lp_traffic_split,
        gateway_test_lp_type,
        gateway_test_lp_class,
        gateway_test_lp_location,
        b.label,
        b.dm_site_id,
        b.gateway_test_lp_type,
        is_yitty_gateway,
        is_scrubs_gateway,
        is_scrubs_customer,
        is_scrubs_action,
        is_scrubs_session,
        is_male_gateway,
        is_male_session_action,
        is_male_session,
        IFF(is_male_customer = TRUE, TRUE, FALSE),
        IFF(is_male_customer = TRUE OR IS_MALE_SESSION = TRUE,TRUE,FALSE),
        COALESCE(LEAD.leadregistrationtype, 'Unknown'),
        COALESCE(activating_membership_type, 'Non-Activating Order'),
        IFF(b.customer_id IS NOT NULL, 1, 0),
        IFF(b.visitor_id IS NOT NULL, 1, 0),
        IFF(is_quiz_start_action = TRUE, 1, 0),
        IFF(is_quiz_complete_action = TRUE, 1, 0),
        IFF(is_skip_quiz_action = TRUE, 1, 0),
        IFF(is_quiz_registration_action = TRUE, 1, 0),
        IFF(is_skip_quiz_registration_action = TRUE, 1, 0),
        IFF(is_speedy_registration_action = TRUE, 1, 0),
        IFF(b.is_lead_registration_action = 1, 1, 0),
        IFF(lead2.is_reactivated_lead = TRUE, 1, 0),
        si.refresh_time
ORDER BY b.session_id ASC;

MERGE INTO shared.session_single_view_media AS t
USING _stg_session_single_view_media AS SRC
    ON T.session_id = SRC.session_id
    WHEN NOT MATCHED THEN
        INSERT (
            session_id,
            visitor_id,
            customer_id,
            store_id,
            storebrand,
            storeregion,
            storecountry,
            sessionlocaldate,
            sessionlocaldatetime,
            membershipstate,
            user_segment,
            leadtenuredaily,
            leadtenuredailygroup,
            viptenuremonthlygroup,
            viptenuremonthly,
            platform,
            platformraw,
            browser,
            operatingsystem,
            hdyh,
            channel_type,
            channel,
            subchannel,
            last_non_direct_media_channel,
            last_non_direct_media_subchannel,
            utmmedium,
            utmsource,
            utmcampaign,
            utmcontent,
            utmterm,
            gatewayid,
            dmgcode,
            gatewaytype,
            gatewaysubtype,
            gatewayname,
            gatewayoffer,
            gatewaygender,
            gatewayftvorrtv,
            gatewayinfluencername,
            gatewayexperience,
            gatewaytestname,
            gatewaytestid,
            gatewayteststartlocaldatetime,
            gatewaytestendlocaldatetime,
            gatewaytestdescription,
            gatewaytesthypothesis,
            gatewaytestresults,
            lptestcellid,
            gatewaytestlptrafficsplit,
            gatewaytestlptype,
            gatewaytestlpclass,
            gatewaytestlplocation,
            lpname,
            lpid,
            lptype,
            isyittygateway,
            isscrubsgateway,
            ismalegateway,
            ismalesessionaction,
            ismalesession,
            ismalecustomer,
            ismalecustomersessions,
            leadregistrationtype,
            activatingordermembershiptype,
            sessions,
            customers,
            visitors,
            quizstarts,
            quizcompletes,
            quizskips,
            quizleads,
            quizskipleads,
            speedyleads,
            leads,
            leadsreactivated,
            activatingorders24hrfromleads,
            activatingordersd7fromleads,
            activatingordersm1fromleads,
            activatingordersfromleads,
            activatingunitsfromleads,
            activatingsubtotalfromleads,
            activatingdiscountfromleads,
            activatingproductgrossrevenuefromleads,
            activatingproductmarginprereturnfromleads,
            activatingorders,
            nonactivatingorders,
            activatingunits,
            nonactivatingunits,
            activatingsubtotal,
            nonactivatingsubtotal,
            activatingdiscount,
            nonactivatingdiscount,
            activatingproductgrossrevenue,
            nonactivatingproductgrossrevenue,
            activatingproductmarginprereturn,
            nonactivatingproductmarginprereturn,
            activatingcashgrossrevenue,
            nonactivatingcashgrossrevenue,
            refreshtime,
            isscrubscustomer,
            isscrubsaction,
            isscrubssession
            )
            VALUES(
                SRC.session_id,
                SRC.visitor_id,
                SRC.customer_id,
                SRC.store_id,
                SRC.storebrand,
                SRC.storeregion,
                SRC.storecountry,
                SRC.sessionlocaldatetime::DATE,
                SRC.sessionlocaldatetime,
                SRC.membershipstate,
                SRC.user_segment,
                SRC.leadtenuredaily,
                SRC.leadtenuredailygroup,
                SRC.viptenuremonthlygroup,
                SRC.viptenuremonthly,
                SRC.platform,
                SRC.platformraw,
                SRC.browser,
                SRC.operatingsystem,
                SRC.hdyh,
                SRC.channel_type,
                SRC.channel,
                SRC.subchannel,
                SRC.last_non_direct_media_channel,
                SRC.last_non_direct_media_subchannel,
                SRC.utmmedium,
                SRC.utmsource,
                SRC.utmcampaign,
                SRC.utmcontent,
                SRC.utmterm,
                SRC.gatewayid,
                SRC.dmgcode,
                SRC.gatewaytype,
                SRC.gatewaysubtype,
                SRC.gatewayname,
                SRC.gatewayoffer,
                SRC.gatewaygender,
                SRC.gatewayftvorrtv,
                SRC.gatewayinfluencername,
                SRC.gatewayexperience,
                SRC.gatewaytestname,
                SRC.gatewaytestid,
                SRC.gatewayteststartlocaldatetime,
                SRC.gatewaytestendlocaldatetime,
                SRC.gatewaytestdescription,
                SRC.gatewaytesthypothesis,
                SRC.gatewaytestresults,
                SRC.lptestcellid,
                SRC.gatewaytestlptrafficsplit,
                SRC.gatewaytestlptype,
                SRC.gatewaytestlpclass,
                SRC.gatewaytestlplocation,
                SRC.lpname,
                SRC.lpid,
                SRC.lptype,
                SRC.isyittygateway,
                SRC.isscrubsgateway,
                SRC.ismalegateway,
                SRC.ismalesessionaction,
                SRC.ismalesession,
                SRC.ismalecustomer,
                SRC.ismalecustomersessions,
                SRC.leadregistrationtype,
                SRC.activatingordermembershiptype,
                SRC.sessions,
                SRC.customers,
                SRC.visitors,
                SRC.quizstarts,
                SRC.quizcompletes,
                SRC.quizskips,
                SRC.quizleads,
                SRC.quizskipleads,
                SRC.speedyleads,
                SRC.leads,
                SRC.leadsreactivated,
                SRC.activatingorders24hrfromleads,
                SRC.activatingordersd7fromleads,
                SRC.activatingordersm1fromleads,
                SRC.activatingordersfromleads,
                SRC.activatingunitsfromleads,
                SRC.activatingsubtotalfromleads,
                SRC.activatingdiscountfromleads,
                SRC.activatingproductgrossrevenuefromleads,
                SRC.activatingproductmarginprereturnfromleads,
                SRC.activatingorders,
                SRC.nonactivatingorders,
                SRC.activatingunits,
                SRC.nonactivatingunits,
                SRC.activatingsubtotal,
                SRC.nonactivatingsubtotal,
                SRC.activatingdiscount,
                SRC.nonactivatingdiscount,
                SRC.activatingproductgrossrevenue,
                SRC.nonactivatingproductgrossrevenue,
                SRC.activatingproductmarginprereturn,
                SRC.nonactivatingproductmarginprereturn,
                SRC.activatingcashgrossrevenue,
                SRC.nonactivatingcashgrossrevenue,
                SRC.refreshtime,
                SRC.isscrubscustomer,
                SRC.isscrubsaction,
                SRC.isscrubssession
                )
            WHEN MATCHED AND (
                /* Ignore all columns from reporting_base.shared.session_refresh_base
                & reporting_base.shared.svm_session_ids_stg except refreshtime column
                as it is the most sensitive. Removes redundant & unnecessary comparisons below. */
                T.refreshtime <> SRC.refreshtime
                OR T.leadregistrationtype <> SRC.leadregistrationtype
                OR T.activatingordermembershiptype <> SRC.activatingordermembershiptype
                OR T.sessions <> SRC.sessions
                OR T.customers <> SRC.customers
                OR T.visitors <> SRC.visitors
                OR T.quizstarts <> SRC.quizstarts
                OR T.quizcompletes <> SRC.quizcompletes
                OR T.quizskips <> SRC.quizskips
                OR T.quizleads <> SRC.quizleads
                OR T.quizskipleads <> SRC.quizskipleads
                OR T.speedyleads <> SRC.speedyleads
                OR T.leads <> SRC.leads
                OR T.leadsreactivated <> SRC.leadsreactivated
                OR T.activatingorders24hrfromleads <> SRC.activatingorders24hrfromleads
                OR T.activatingordersd7fromleads <> SRC.activatingordersd7fromleads
                OR T.activatingordersm1fromleads <> SRC.activatingordersm1fromleads
                OR T.activatingordersfromleads <> SRC.activatingordersfromleads
                OR T.activatingunitsfromleads <> SRC.activatingunitsfromleads
                OR T.activatingsubtotalfromleads <> SRC.activatingsubtotalfromleads
                OR T.activatingdiscountfromleads <> SRC.activatingdiscountfromleads
                OR T.activatingproductgrossrevenuefromleads <> SRC.activatingproductgrossrevenuefromleads
                OR T.activatingproductmarginprereturnfromleads <> SRC.activatingproductmarginprereturnfromleads
                OR T.activatingorders <> SRC.activatingorders
                OR T.nonactivatingorders <> SRC.nonactivatingorders
                OR T.activatingunits <> SRC.activatingunits
                OR T.nonactivatingunits <> SRC.nonactivatingunits
                OR T.activatingsubtotal <> SRC.activatingsubtotal
                OR T.nonactivatingsubtotal <> SRC.nonactivatingsubtotal
                OR T.activatingdiscount <> SRC.activatingdiscount
                OR T.nonactivatingdiscount <> SRC.nonactivatingdiscount
                OR T.activatingproductgrossrevenue <> SRC.activatingproductgrossrevenue
                OR T.nonactivatingproductgrossrevenue <> SRC.nonactivatingproductgrossrevenue
                OR T.activatingproductmarginprereturn <> SRC.activatingproductmarginprereturn
                OR T.nonactivatingproductmarginprereturn <> SRC.nonactivatingproductmarginprereturn
                OR T.activatingcashgrossrevenue <> SRC.activatingcashgrossrevenue
                OR T.nonactivatingcashgrossrevenue <> SRC.nonactivatingcashgrossrevenue
              ) THEN
            UPDATE SET
                T.visitor_id = SRC.visitor_id,
                T.customer_id = SRC.customer_id,
                T.store_id = SRC.store_id,
                T.storebrand = SRC.storebrand,
                T.storeregion = SRC.storeregion,
                T.storecountry = SRC.storecountry,
                T.sessionlocaldate = SRC.sessionlocaldatetime::DATE,
                T.sessionlocaldatetime = SRC.sessionlocaldatetime,
                T.membershipstate = SRC.membershipstate,
                T.user_segment = SRC.user_segment,
                T.leadtenuredaily = SRC.leadtenuredaily,
                T.leadtenuredailygroup = SRC.leadtenuredailygroup,
                T.viptenuremonthlygroup = SRC.viptenuremonthlygroup,
                T.viptenuremonthly = SRC.viptenuremonthly,
                T.platform = SRC.platform,
                T.platformraw = SRC.platformraw,
                T.browser = SRC.browser,
                T.operatingsystem = SRC.operatingsystem,
                T.hdyh = SRC.hdyh,
                T.channel_type = SRC.channel_type,
                T.channel = SRC.channel,
                T.subchannel = SRC.subchannel,
                T.last_non_direct_media_channel = SRC.last_non_direct_media_channel,
                T.last_non_direct_media_subchannel = SRC.last_non_direct_media_subchannel,
                T.utmmedium = SRC.utmmedium,
                T.utmsource = SRC.utmsource,
                T.utmcampaign = SRC.utmcampaign,
                T.utmcontent = SRC.utmcontent,
                T.utmterm = SRC.utmterm,
                T.gatewayid = SRC.gatewayid,
                T.dmgcode = SRC.dmgcode,
                T.gatewaytype = SRC.gatewaytype,
                T.gatewaysubtype = SRC.gatewaysubtype,
                T.gatewayname = SRC.gatewayname,
                T.gatewayoffer = SRC.gatewayoffer,
                T.gatewaygender = SRC.gatewaygender,
                T.gatewayftvorrtv = SRC.gatewayftvorrtv,
                T.gatewayinfluencername = SRC.gatewayinfluencername,
                T.gatewayexperience = SRC.gatewayexperience,
                T.gatewaytestname = SRC.gatewaytestname,
                T.gatewaytestid = SRC.gatewaytestid,
                T.gatewayteststartlocaldatetime = SRC.gatewayteststartlocaldatetime,
                T.gatewaytestendlocaldatetime = SRC.gatewaytestendlocaldatetime,
                T.gatewaytestdescription = SRC.gatewaytestdescription,
                T.gatewaytesthypothesis = SRC.gatewaytesthypothesis,
                T.gatewaytestresults = SRC.gatewaytestresults,
                T.lptestcellid = SRC.lptestcellid,
                T.gatewaytestlptrafficsplit = SRC.gatewaytestlptrafficsplit,
                T.gatewaytestlptype = SRC.gatewaytestlptype,
                T.gatewaytestlpclass = SRC.gatewaytestlpclass,
                T.gatewaytestlplocation = SRC.gatewaytestlplocation,
                T.lpname = SRC.lpname,
                T.lpid = SRC.lpid,
                T.lptype = SRC.lptype,
                T.isyittygateway = SRC.isyittygateway,
                T.isscrubsgateway = SRC.isscrubsgateway,
                T.ismalegateway = SRC.ismalegateway,
                T.ismalesessionaction = SRC.ismalesessionaction,
                T.ismalesession = SRC.ismalesession,
                T.ismalecustomer = SRC.ismalecustomer,
                T.ismalecustomersessions = SRC.ismalecustomersessions,
                T.leadregistrationtype = SRC.leadregistrationtype,
                T.activatingordermembershiptype = SRC.activatingordermembershiptype,
                T.sessions = SRC.sessions,
                T.customers = SRC.customers,
                T.visitors = SRC.visitors,
                T.quizstarts = SRC.quizstarts,
                T.quizcompletes = SRC.quizcompletes,
                T.quizskips = SRC.quizskips,
                T.quizleads = SRC.quizleads,
                T.quizskipleads = SRC.quizskipleads,
                T.speedyleads = SRC.speedyleads,
                T.leads = SRC.leads,
                T.leadsreactivated = SRC.leadsreactivated,
                T.activatingorders24hrfromleads = SRC.activatingorders24hrfromleads,
                T.activatingordersd7fromleads = SRC.activatingordersd7fromleads,
                T.activatingordersm1fromleads = SRC.activatingordersm1fromleads,
                T.activatingordersfromleads = SRC.activatingordersfromleads,
                T.activatingunitsfromleads = SRC.activatingunitsfromleads,
                T.activatingsubtotalfromleads = SRC.activatingsubtotalfromleads,
                T.activatingdiscountfromleads = SRC.activatingdiscountfromleads,
                T.activatingproductgrossrevenuefromleads = SRC.activatingproductgrossrevenuefromleads,
                T.activatingproductmarginprereturnfromleads = SRC.activatingproductmarginprereturnfromleads,
                T.activatingorders = SRC.activatingorders,
                T.nonactivatingorders = SRC.nonactivatingorders,
                T.activatingunits = SRC.activatingunits,
                T.nonactivatingunits = SRC.nonactivatingunits,
                T.activatingsubtotal = SRC.activatingsubtotal,
                T.nonactivatingsubtotal = SRC.nonactivatingsubtotal,
                T.activatingdiscount = SRC.activatingdiscount,
                T.nonactivatingdiscount = SRC.nonactivatingdiscount,
                T.activatingproductgrossrevenue = SRC.activatingproductgrossrevenue,
                T.nonactivatingproductgrossrevenue = SRC.nonactivatingproductgrossrevenue,
                T.activatingproductmarginprereturn = SRC.activatingproductmarginprereturn,
                T.nonactivatingproductmarginprereturn = SRC.nonactivatingproductmarginprereturn,
                T.activatingcashgrossrevenue = SRC.activatingcashgrossrevenue,
                T.nonactivatingcashgrossrevenue = SRC.nonactivatingcashgrossrevenue,
                T.refreshtime = SRC.refreshtime,
                T.isscrubscustomer = SRC.isscrubscustomer,
                T.isscrubsaction = SRC.isscrubsaction,
                T.isscrubssession = SRC.isscrubssession
            ;

/* Bot Cleanup */
DELETE FROM shared.session_single_view_media t
WHERE
    session_id NOT IN (SELECT session_id FROM reporting_base_prod.shared.session_refresh_base);


