set low_watermark_ltz = (
    select
        max(a.MONTH_DATE)
    from REPORTING_PROD.GFB.gfb_customer_dataset_base a
    );


CREATE OR REPLACE TEMP TABLE _gms_contacts_base AS
SELECT
    cd.customer_id
    ,cd.month_date
    ,st.time_zone
FROM REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_BASE cd
JOIN edw_prod.reference.store_timezone st
    ON st.store_id = cd.store_id
where
    cd.MONTH_DATE >= $low_watermark_ltz
    and cd.MONTH_DATE < date_trunc('MONTH', current_date());


CREATE OR REPLACE TEMPORARY TABLE _gms_contacts_details AS
SELECT
    x.date_added,
    x.bu,
    x.call_type,
    x.disposition,
    x.customer_id,
    x.month_date,
    COUNT(x.case_id) AS total
FROM
(
    SELECT DISTINCT
        c.case_id,
        CONVERT_TIMEZONE(gb.time_zone, c.date_added::TIMESTAMP_LTZ)::DATE AS date_added,
        cust.customer_id,
        gb.month_date,
        ds.STORE_BRAND AS bu,
        CASE
            WHEN cfd.label LIKE '%Retention%' THEN 'Retention'
            WHEN cfd.label LIKE '%Order%' THEN 'Order and Shipping'
            WHEN cfd.label LIKE '%General%' THEN 'General Questions'
            WHEN cfd.label LIKE '%Billing%' THEN 'Billing'
            WHEN cfd.label LIKE '%account%' THEN 'account Maintenance'
            WHEN cfd.label LIKE '%Other%' THEN 'Other'
            END  AS call_type,
        cf.label AS disposition
    FROM _gms_contacts_base gb
    JOIN lake_jfb_view.ultra_merchant.case_customer cust
        ON gb.customer_id = cust.customer_id
    JOIN lake_jfb_view.ultra_merchant.case c
        ON cust.case_id = c.case_id
    JOIN lake_jfb_view.ultra_merchant.store s
        ON s.store_group_id = c.store_group_id
    JOIN reporting_prod.gfb.vw_store ds
        ON ds.store_id = s.store_id
    LEFT JOIN lake_jfb_view.ultra_merchant.case_classification cc
        ON cc.case_id = c.case_id
    LEFT JOIN lake_jfb_view.ultra_merchant.case_flag_disposition_case_flag_type cfdc
        ON cc.case_flag_type_id = cfdc.case_flag_type_id
    LEFT JOIN lake_jfb_view.ultra_merchant.case_flag_disposition cfd
        ON cfdc.case_flag_disposition_id = cfd.case_flag_disposition_id
    LEFT JOIN lake_jfb_view.ultra_merchant.case_flag cf
        ON cf.case_flag_id = cc.case_flag_id
    WHERE
        date_added >= '2018-01-01'
        AND store_type = 'Online'
        AND is_core_store = TRUE
        AND cfd.case_disposition_type_id IN (9, 15, 16, 17, 18, 19)
        AND cf.case_flag_id IN(373, 372, 371, 370, 278, 281, 253, 310, 240, 234, 282, 55, 287, 293, 235, 284, 237, 290, 307, 367, 305,315, 352,346, 288, 243, 280, 242, 348, 355, 276, 350, 309, 73, 311, 312, 289, 241, 246, 314, 347, 283, 86, 354,295, 244, 279,356, 245, 306, 351, 316, 308, 286, 296, 251, 313, 368, 233, 362, 294, 285, 238, 252, 277)
        AND c.date_added = gb.month_date
        AND gb.month_date = DATE_TRUNC('MONTH', CONVERT_TIMEZONE(gb.time_zone, c.date_added::timestamp_ltz)::date)
) x
GROUP BY
    date_added
    ,bu
    ,call_type
    ,disposition
    ,customer_id
    ,month_date;


CREATE OR REPLACE TEMPORARY TABLE _gms_contacts_pre_stg AS
SELECT DISTINCT
    vip.customer_id,
    vip.month_date,
    COUNT(DISTINCT(CASE WHEN gc.customer_id IS NOT NULL THEN gc.customer_id ELSE NULL END))    AS gms_contact_flag,
    COUNT(DISTINCT(CASE WHEN gc.call_type = 'Retention' THEN gc.customer_id ELSE NULL END)) AS gms_contact_flag_retention,
    COUNT(DISTINCT(CASE WHEN gc.call_type = 'Order and Shipping' THEN gc.customer_id ELSE NULL END))  AS gms_contact_flag_order_and_shipping,
    COUNT(DISTINCT(CASE WHEN gc.call_type = 'General Questions' THEN gc.customer_id ELSE NULL END))   AS gms_contact_flag_general_questions,
    COUNT(DISTINCT(CASE WHEN gc.call_type = 'Other' THEN gc.customer_id ELSE NULL END)) AS gms_contact_flag_other,
    COUNT(DISTINCT(CASE WHEN gc.call_type = 'Billing' THEN gc.customer_id ELSE NULL END)) AS gms_contact_flag_billing,
    COUNT(DISTINCT(CASE WHEN gc.call_type = 'account Maintenance' THEN gc.customer_id ELSE NULL END)) AS gms_contact_flag_acCOUNT_maintenance,
    COUNT(DISTINCT(CASE WHEN gc.disposition = 'Skip Month' THEN gc.customer_id ELSE NULL END)) AS gms_contact_flag_skip_month,
    COUNT(DISTINCT(CASE WHEN gc.disposition = 'How VIP Membership Works' THEN gc.customer_id ELSE NULL END))  AS gms_contact_flag_how_membership_works,
    COUNT(DISTINCT(CASE WHEN gc.disposition IN('Exchange or Return','Return or Exchange Policy','Exchange for Other Item','Returns - Exchange') THEN gc.customer_id ELSE NULL END))  AS gms_contact_flag_returns,
    COUNT(DISTINCT(CASE WHEN gc.disposition IN('Website Issues - JF Main','Website or App Issues','Website Issues - JF Mobile','Website/App Navigation') THEN gc.customer_id ELSE NULL END)) AS gms_contact_flag_website_issues,
    SUM(ifnull(gc.total, 0))AS gms_contact_count
FROM REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_BASE vip
JOIN _gms_contacts_details  gc
    ON vip.customer_id = gc.customer_id
    AND gc.month_date = vip.month_date
GROUP BY
    vip.customer_id,
    vip.month_date;


delete from REPORTING_PROD.GFB.gfb_customer_dataset_gms_actions a
where
    a.MONTH_DATE >= $low_watermark_ltz;


insert into REPORTING_PROD.GFB.gfb_customer_dataset_gms_actions
SELECT
    customer_id,
    month_date,
    gms_contact_flag,
    gms_contact_flag_retention,
    gms_contact_flag_order_and_shipping,
    gms_contact_flag_general_questions,
    gms_contact_flag_other,
    gms_contact_flag_billing,
    gms_contact_flag_acCOUNT_maintenance,
    gms_contact_flag_skip_month,
    gms_contact_flag_how_membership_works,
    gms_contact_flag_returns,
    gms_contact_flag_website_issues,
    gms_contact_count
FROM _gms_contacts_pre_stg;
