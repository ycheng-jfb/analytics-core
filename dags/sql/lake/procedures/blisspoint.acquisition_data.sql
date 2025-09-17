SET from_date_ltz = %(from_date)s::TIMESTAMP_LTZ;
SET from_date_ntz_utc = convert_timezone('UTC', $from_date_ltz)::TIMESTAMP_NTZ;

CREATE OR REPLACE TEMPORARY TABLE _customer_base AS (
    SELECT
        customer_id
    FROM lake_consolidated_view.ultra_merchant.membership m
    WHERE m.datetime_added >= $from_date_ntz_utc
);


CREATE OR REPLACE TEMPORARY TABLE _reg_sessions AS (
    SELECT
        c.customer_id,
        s.session_local_datetime::TIMESTAMP_LTZ AS registration_session_hq_datetime,
        s.ip,
        m.datetime_activated,
        s.channel AS channel,
        s.session_id AS session_id,
        m.store_id
    FROM _customer_base c
    LEFT JOIN lake_consolidated_view.ultra_merchant.membership m ON m.customer_id = c.customer_id
    LEFT JOIN lake_consolidated_view.ultra_merchant.membership_signup ms ON ms.membership_signup_id = m.membership_signup_id
    LEFT JOIN reporting_base_prod.staging.session_media_channel s ON s.session_id = ms.session_id
);



CREATE OR REPLACE TEMPORARY TABLE _savage_session AS
SELECT
    session_id,
    medium,
    source,
    COALESCE(cm.channel, cm2.channel) AS sx_channel,
    row_number()
            OVER (PARTITION BY ga.session_id ORDER BY nvl2(sx_channel, 0, 1) ASC, iff(medium = '(none)', 1, 0) ASC) AS rn
FROM lake.media.bigquery_ga_sessions_utm_source_medium_legacy ga
LEFT JOIN LAKE_VIEW.SHAREPOINT.MED_SAVAGEX_CHANNEL_MAPPING cm ON lower(cm.utm_medium) = lower(ga.medium)
    AND lower(cm.utm_source) = lower(ga.source)
LEFT JOIN LAKE_VIEW.SHAREPOINT.MED_SAVAGEX_CHANNEL_MAPPING cm2 ON lower(cm2.utm_medium) = lower(ga.medium)
    AND lower(cm2.utm_source) = lower(CHAR(37))
WHERE view_id = 167641144
  AND try_cast(ga.session_id AS INT) > 0
  AND visit_start_time >= ADD_MONTHS($from_date_ltz, -1);


UPDATE _reg_sessions r
SET channel = COALESCE(ss.sx_channel, 'No GA Session')
FROM _savage_session ss
JOIN edw_prod.data_model.dim_store st
WHERE ss.session_id = edw_prod.stg.udf_unconcat_brand(r.session_id)
  AND ss.rn = 1
  AND st.store_id = r.store_id
  AND st.store_brand = 'Savage X';


CREATE OR REPLACE TEMPORARY TABLE _activating_orders AS (
    SELECT
        a.customer_id,
        a.order_id,
        a.subtotal,
        a.discount,
        a.datetime_added
    FROM (
        SELECT
            o.customer_id,
            o.order_id,
            o.subtotal,
            o.discount,
            o.datetime_added,
            ROW_NUMBER() OVER (PARTITION BY cb.customer_id ORDER BY o.order_id) AS rn
        FROM _customer_base cb
        JOIN lake_consolidated_view.ultra_merchant."ORDER"  o ON o.customer_id = cb.customer_id
        JOIN lake_consolidated_view.ultra_merchant.order_classification oc ON oc.order_id = o.order_id
            AND oc.order_type_id = 23
    ) a
    WHERE a.rn = 1
);

CREATE OR REPLACE TEMPORARY TABLE _first_activaton_order_id AS (
select CUSTOMER_ID,order_id from(
select CUSTOMER_ID,order_id,row_number() over(partition by CUSTOMER_ID order by ACTIVATION_LOCAL_DATETIME) as rno
from edw_prod.data_model.fact_activation ) first_activaton
where rno=1
);

INSERT INTO _activating_orders
(
    customer_id,
    order_id,
    subtotal,
    discount,
    datetime_added)
SELECT
    cb.customer_id,
    o.order_id,
    o.subtotal,
    o.discount,
    o.datetime_added
FROM _customer_base cb
JOIN _first_activaton_order_id c ON c.customer_id = cb.customer_id
JOIN lake_consolidated_view.ultra_merchant."ORDER"  o ON o.order_id = c.order_id
WHERE NOT EXISTS(SELECT 1 FROM _activating_orders ao WHERE ao.customer_id = cb.customer_id)
  AND $from_date_ntz_utc < '2015-01-01';



CREATE OR REPLACE TEMPORARY TABLE _customer_detail AS (
    SELECT
        t.customer_id,
        cd.name,
        cd.value,
        IFF(cd.name = 'customer_referrer_id' AND cd.value != '', cd.value, NULL) AS customer_referrer_id
    FROM _customer_base t
    JOIN lake_consolidated_view.ultra_merchant.customer_detail cd ON cd.customer_id = t.customer_id
        AND cd.name IN ('customer_referrer_id', 'heard-us')
);

delete from _customer_detail WHERE try_to_number(customer_referrer_id) is null and customer_referrer_id is not null;

CREATE OR REPLACE TEMPORARY TABLE _hdyh AS (
    SELECT
        a.customer_id,
        a.howdidyouhear
    FROM (
        SELECT
            cd.customer_id,
            COALESCE(cr.label, cd.value) AS howdidyouhear,
            ROW_NUMBER() OVER (PARTITION BY cd.customer_id ORDER BY (SELECT NULL)) AS rn
        FROM _customer_detail cd
        LEFT JOIN lake_consolidated_view.ultra_merchant.customer_referrer cr ON cr.meta_original_customer_referrer_id = cd.customer_referrer_id
    ) a
    WHERE rn = 1
);


CREATE OR REPLACE TEMPORARY TABLE _free_trial AS (
    SELECT DISTINCT
        o.customer_id
    FROM _activating_orders o
    JOIN lake_consolidated_view.ultra_merchant.order_discount od ON o.order_id = od.order_id
    JOIN lake_consolidated_view.ultra_merchant.promo_promo_classfication ppc ON od.promo_id = ppc.promo_id
    JOIN lake_consolidated_view.ultra_merchant.promo_classification pc ON ppc.promo_classification_id = pc.promo_classification_id
    WHERE pc.label = 'Free Trial'
);


CREATE OR REPLACE TEMPORARY TABLE _fk_free_trail AS (
    SELECT
        c.customer_id
    FROM EDW_PROD.DATA_MODEL.DIM_CUSTOMER AS c
    WHERE c.is_free_trial = 'TRUE'
);


CREATE OR REPLACE TEMPORARY TABLE _meta_nav AS (
    SELECT
        c.customer_id
    FROM lake_consolidated_view.ultra_merchant.customer_detail cd
    JOIN EDW_PROD.DATA_MODEL.DIM_CUSTOMER c ON c.customer_id = cd.customer_id
    JOIN edw_prod.data_model.dim_store s ON s.store_id = c.store_id
        AND contains(s.store_group, 'FabKids')
    WHERE name = 'Initial Medium'
      AND CONTAINS(value, 'meta')
);


SELECT IFF(lower(store_brand) = 'fabletics' and dc.gender = 'M',
           'FL MEN' || ' ' || ds.store_country,
           ds.store_brand_abbr|| ' ' || ds.store_country) AS customer_segment,
    edw_prod.stg.udf_unconcat_brand(c.customer_id) AS customer_id,
    umc.store_id AS store_id,
    IFNULL(h.howdidyouhear, 'N/A') AS how_did_you_hear,
    CASE
        WHEN rs.channel IN ('Brand Site', 'N/A') THEN 'Direct Traffic'
        WHEN rs.channel = 'Organic Search' THEN 'SEO'
        ELSE IFNULL(rs.channel, 'N/A')
    END AS channel,
    umc.datetime_added AS registration_datetime_pt,
    rs.registration_session_hq_datetime AS registration_session_datetime_pt,
    rs.datetime_activated AS activation_datetime_pt,
    IFF(DATEDIFF(SECOND, rs.registration_session_hq_datetime, rs.datetime_activated) <= 86400, 1,
        0) AS is_24_hr_activation,
    ao.subtotal AS activating_order_subtotal,
    ao.discount AS activating_order_discount,
    CASE
        WHEN ft.customer_id IS NOT NULL THEN 'Free Trial'
        WHEN rs.datetime_activated IS NOT NULL THEN 'Not Free Trial'
        ELSE 'N/A'
    END AS free_trial_flag,
    a.zip AS zip_code,
    rs.ip,
    IFF(fk.customer_id IS NULL, 0, 1) AS is_fk_free_trail,
    IFF(mn.customer_id IS NULL, 0, 1) AS is_meta_nav
FROM _customer_base c
JOIN lake_consolidated_view.ultra_merchant.customer umc ON umc.customer_id = c.customer_id
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = umc.store_id
    AND NOT CONTAINS(ds.store_name, 'sample')
    AND NOT CONTAINS(ds.store_name, 'g-swag')
    AND ds.store_name NOT IN (
                              'JustFab - Heels.com Drop Ship',
                              'JustFab - Wholesale',
                              'JustFab - Retail Replen'
        )
LEFT JOIN EDW_PROD.DATA_MODEL.DIM_CUSTOMER dc ON c.customer_id = dc.customer_id
LEFT JOIN _reg_sessions rs ON rs.customer_id = c.customer_id
LEFT JOIN _activating_orders ao ON ao.customer_id = c.customer_id
LEFT JOIN lake_consolidated_view.ultra_merchant.address a ON a.address_id = umc.default_address_id
LEFT JOIN _free_trial ft ON ft.customer_id = c.customer_id
LEFT JOIN _fk_free_trail fk ON c.customer_id = fk.customer_id
LEFT JOIN _meta_nav mn ON fk.customer_id = mn.customer_id
LEFT JOIN _hdyh h ON h.customer_id = c.customer_id;
