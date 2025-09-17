-- TODO commented line 205 since salesfloor customer have event_end_local_datetime as '2022-12-14 07:49:47.022 -0800'

--NOTE - ISSUE WITH CUSTOMER FEED - WE are not differentiating vip/lead/online/retail purchaser!!!

ALTER SESSION SET TIMEZONE='America/Los_Angeles';

SET target_table = 'reporting_prod.salesfloor.customer_feed';
SET current_datetime = current_timestamp();


MERGE INTO reporting_prod.public.meta_table_dependency_watermark AS t
    USING
        (
            SELECT
    'reporting_prod.salesfloor.customer_feed' AS table_name,
    NULLIF(dependent_table_name,'reporting_prod.salesfloor.customer_feed') AS dependent_table_name,
    high_watermark_datetime AS new_high_watermark_datetime
FROM(
                                                    SELECT
            'lake_consolidated.ULTRA_MERCHANT."ORDER"' AS dependent_table_name,
            max(meta_update_datetime) AS high_watermark_datetime
        FROM lake_consolidated.ULTRA_MERCHANT."ORDER"
    UNION
        SELECT
            'edw_prod.data_model.dim_customer' AS dependent_table_name,
            max(meta_update_datetime) AS high_watermark_datetime
        FROM edw_prod.data_model.dim_customer
    union
        SELECT
            'edw_prod.data_model.fact_order' AS dependent_table_name,
            max(meta_update_datetime) AS high_watermark_datetime
        FROM edw_prod.data_model.fact_order
    union
        SELECT
            'lake_consolidated.ultra_merchant.customer_detail' AS dependent_table_name,
            max(meta_update_datetime) AS high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.customer_detail
    union
        SELECT
            'reporting_prod.shared.vw_attentive_sms_fl' AS dependent_table_name,
           max(meta_update_datetime) AS high_watermark_datetime
        FROM reporting_prod.shared.VW_ATTENTIVE_SMS_FL
    union
        SELECT
            'edw_prod.data_model.fact_membership_event' AS dependent_table_name,
            max(meta_update_datetime) AS high_watermark_datetime
        FROM edw_prod.data_model.fact_membership_event
    union
        SELECT
            'med_db_staging.attentive.vw_attentive_sms' AS dependent_table_name,
            max(meta_update_datetime) AS high_watermark_datetime
        FROM lake.media.vw_attentive_attentive_sms_legacy
                    ) h

    ) AS s ON t.table_name = s.table_name
    AND equal_null(t.dependent_table_name, s.dependent_table_name)
WHEN MATCHED
    AND not equal_null(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
    THEN UPDATE
        SET t.new_high_watermark_datetime = s.new_high_watermark_datetime,
            t.meta_update_datetime = $current_datetime::timestamp_ltz(3)
WHEN NOT MATCHED
    THEN
INSERT
    (
    table_name,
    dependent_table_name,
    high_watermark_datetime,
    new_high_watermark_datetime,
    meta_create_datetime
    )
VALUES
    (
        s.table_name,
        s.dependent_table_name,
        '1900-01-01'::timestamp_ltz,
        s.new_high_watermark_datetime,
        $current_datetime::timestamp_ltz
(3)
        );

set watermark_date = (Select dateadd('DAY', -2, watermark_datetimee ) as high_watermark_date from
(
select min(NVL(high_watermark_datetime, '1900-01-01'::TIMESTAMP_LTZ)) AS watermark_datetimee  from reporting_prod.public.meta_table_dependency_watermark
where table_name = $target_table
));

CREATE OR REPLACE TEMP TABLE _workday_employee_ids AS
SELECT
    associate_id,
    try_to_number(workday_id) as workday_id,
    start_date as start_datetime,
    COALESCE(lead(start_date) over (partition by associate_id order by start_date),current_timestamp) as end_datetime
FROM (SELECT distinct
        a.administrator_id as associate_id,
        COALESCE(e.employee_id, e2.employee_id ,t2.workday_id) as workday_id,
        COALESCE(a.DATE_ADDED,e.START_DATE,t2.start_datetime) as START_DATE
    FROM lake.ultra_identity.administrator a
    LEFT JOIN lake.WORKDAY.EMPLOYEES e
        on a.login = e.employee_id
        AND e.company IN ('170 JF Retail Services','190 Savage X Fenty','192 Savage X Retail')
    LEFT JOIN lake.WORKDAY.EMPLOYEES e2
        on a.login != e2.employee_id
        and lower(trim(a.firstname))=lower(trim(e2.FIRST_NAME))
        AND lower(trim(a.lastname))=lower(trim(e2.last_NAME))
        AND e2.company IN ('170 JF Retail Services','190 Savage X Fenty','192 Savage X Retail')
        and ((a.login!='NRamos') or (a.login='NRamos' and year(e2.start_date) = '2021'))
    left JOIN (
        SELECT distinct
            COALESCE(try_to_number(trim(up_for_euid.value,'"')),-2) AS associate_id,
            COALESCE(trim(up_for_wdid.value,'"'),emp.employee_id) as workday_id,
            up_for_wdid.datetime_added AS start_datetime
        FROM lake_view.ultra_identity.user_property AS up_for_euid
        LEFT JOIN lake_view.ultra_identity.user_property AS up_for_wdid
            ON up_for_euid.user_id = up_for_wdid.user_id
            AND up_for_wdid.key = 'workdayEmployeeId'
        JOIN lake_view.ultra_identity.user AS u
            ON u.user_id = up_for_euid.user_id
        LEFT JOIN lake.workday.employees emp
            ON lower(trim(emp.first_name))=lower(trim(u.firstname))
            AND lower(trim(emp.last_name))=lower(trim(u.lastname))
            AND emp.company in ('170 JF Retail Services','190 Savage X Fenty','192 Savage X Retail')
        WHERE up_for_euid.key = 'ecomUserId'
      ) t2 on t2.associate_id = a.administrator_id
) abc
;

CREATE OR REPLACE TEMP TABLE _order_tracking_id_null as
SELECT order_id
FROM lake_consolidated.ULTRA_MERCHANT."ORDER"
WHERE ORDER_TRACKING_ID is null
    AND datetime_modified::date >= dateadd('month',-12,current_date)
    and meta_update_datetime >= $watermark_date
UNION
SELECT order_id
FROM edw_prod.data_model.fact_order
WHERE administrator_id =-1
    AND order_local_datetime::date >= dateadd('month',-12,current_date)
    and meta_update_datetime >= $watermark_date
    ;

CREATE OR REPLACE TEMP TABLE _missing_administrator_ids as
SELECT cw.administrator_id,o.order_id
FROM lake_consolidated.ULTRA_MERCHANT."ORDER" o
JOIN lake_consolidated.ULTRA_MERCHANT.SESSION s  on s.SESSION_ID=o.session_id
JOIN lake_consolidated.ULTRA_MERCHANT.CART c on c.SESSION_ID=s.SESSION_ID
JOIN lake_consolidated.ULTRA_MERCHANT.CART_WAREHOUSE cw on c.CART_ID=cw.cart_id
JOIN _order_tracking_id_null otn on o.ORDER_ID=otn.order_id
where o.meta_update_datetime >= $watermark_date
;

CREATE OR REPLACE TEMP TABLE _preferred_store_id as
SELECT
    c.customer_id,
    case when c.name = 'retail_preferred_store_id' then c.value end as retail_preferred_store_id,
    case when c.name = 'preferred_retail_store_id' then c.value end as preferred_retail_store_id
FROM lake_consolidated.ultra_merchant.customer_detail c
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = c.customer_id
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = dc.store_id
WHERE name IN ('retail_preferred_store_id','preferred_retail_store_id')
and store_brand_abbr in ('FL','YTY')
and c.meta_update_datetime >= $watermark_date
;

CREATE OR REPLACE TEMP TABLE _preferred_store_id_final as
SELECT
    DISTINCT customer_id,
    iff(ds.store_retail_location_code='N/A', null, ds.store_retail_location_code)  as retail_preferred_store_id,
    ds1.store_retail_location_code as preferred_retail_store_id
FROM _preferred_store_id p
left JOIN edw_prod.data_model.dim_store ds on ds.store_id = retail_preferred_store_id
left JOIN edw_prod.data_model.dim_store ds1 on ds1.store_id = preferred_retail_store_id; --OQ EDIT - Added left join, this table was empty before


CREATE OR REPLACE TEMP TABLE _nearest_store as
SELECT distinct
    dc.customer_id,
    rdd.duration,
    st.store_retail_location_code AS retail_store_id,
    row_number() over (partition by dc.customer_id order by rdd.duration asc) AS distance_rank
FROM edw_prod.data_model.dim_customer dc --edw_prod.data_model.fact_registration fr
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = dc.store_id
    --JOIN  dc ON fr.customer_id = dc.customer_id
JOIN reporting_base_prod.data_science.fl_retail_driving_distance rdd ON TO_CHAR(rdd.vip_zip) = TO_CHAR
                                     ((CASE WHEN dc.DEFAULT_POSTAL_CODE<>'Unknown' AND LENGTH(dc.DEFAULT_POSTAL_CODE)>=5 THEN LEFT(dc.DEFAULT_POSTAL_CODE,5)
                                         WHEN dc.QUIZ_ZIP IS NOT NULL AND dc.QUIZ_ZIP<>'Unknown' AND length(dc.QUIZ_ZIP)>=5 THEN LEFT(dc.QUIZ_ZIP,5) END))
JOIN edw_prod.data_model.dim_store  AS  st ON st.store_retail_zip_code = rdd.store_zip
    AND st.store_brand = 'Fabletics'
    and st.STORE_SUB_TYPE = 'Store'
    and st.STORE_RETAIL_STATUS ilike any ('open - no issues', 'Lease executed', '%salesfloor ready%')
    and st.STORE_RETAIL_LOCATION_CODE != '0001'
LEFT JOIN lake_consolidated_view.ultra_merchant.store AS  v ON v.store_id = st.store_id AND v.code LIKE 'flretailvarsity%'
WHERE ds.store_brand IN ('Fabletics','Yitty')
AND v.store_id IS NULL
and rdd.duration <= 30
and dc.meta_update_datetime >= $watermark_date
;

--remove employees & fake/test accounts:
CREATE OR REPLACE TEMPORARY TABLE _emps_or_fake_customers AS
select distinct dc.customer_id
from edw_prod.data_model.fact_order fo
         join edw_prod.data_model.fact_order_line fol on fol.order_id = fo.order_id
         join edw_prod.data_model.dim_store st on st.store_id = fo.store_id
         join edw_prod.data_model.fact_order_discount fod on fod.ORDER_ID = fo.order_id
         join edw_prod.data_model.dim_promo_history dph on dph.promo_history_key = fod.promo_history_key --correct join!!
         join edw_prod.data_model.dim_customer dc on dc.customer_id = fo.CUSTOMER_ID
         join edw_prod.data_model.dim_order_sales_channel doc on fo.order_sales_channel_key = doc.order_sales_channel_key
         join edw_prod.data_model.dim_order_status dos on fo.order_status_key = dos.order_status_key
         join edw_prod.data_model.dim_order_processing_status ops
              on fo.order_processing_status_key = ops.order_processing_status_key
         join edw_prod.data_model.dim_order_line_status ols on ols.order_line_status_key = fol.order_line_status_key
where doc.order_classification_l1 in ('Product Order')
  and dos.order_status in ('Success', 'Pending')
  and ols.order_line_status <> 'Cancelled'
  and doc.is_product_seeding_order = 0
and lower(promo_code) in ('team50','team40') --old promo code
UNION --union all include dupes
SELECT DISTINCT customer_id
FROM edw_prod.data_model.dim_customer dc
WHERE
        (dc.email   ILIKE '%justfab.com'
       OR dc.email ILIKE '%shoedazzle.com'
       OR dc.email ILIKE '%fabletics.com'
       OR dc.email ILIKE '%fabkids.com'
       OR dc.email ILIKE '%ibinc.com'
       OR dc.email ILIKE '%techstyle.com'
       OR dc.email ILIKE '%savagex.com'
       OR dc.email ILIKE '%test.com'  --allow these accounts for sample feed
       OR dc.email ILIKE '%example%'
       OR dc.email ILIKE '%acquired.com'
       OR dc.email ILIKE '%reacquired.com'
       OR dc.email ILIKE '%qq.com' --scrub the list for any fake emails (ex: @test, @acquired, @reacquired, @qq), any emails that don't end in .com, .edu, .org, or .net (like .con instead of .com), or any .au or .nz email addresses)
       OR dc.email ILIKE '%.au'
       OR dc.email ILIKE '%.nz'
       OR dc.email ILIKE '%.local'
       OR dc.email ILIKE '%.con');

--only interested in opted out customers
create or replace temp table _email_opt_out as
select distinct customer_id
from lake_view.emarsys.email_subscribes
where brand = 'fabletics_us'
  and opt_in = 0; --only grab opt_outs

create or replace temp table _sms_opt_out as
select distinct att.CUSTOMER_ID,
                rank() over (partition by att.customer_id order by cast(att.timestamp as timestamp) desc) as rn
FROM lake.media.vw_attentive_attentive_sms_legacy att
         JOIN lake_view.sharepoint.med_account_mapping_media am
              on am.source_id = att.company_id
                  and am.source ilike 'attentive'
         JOIN edw_prod.data_model.dim_store st
              on st.store_id = am.store_id
                  and st.store_brand = 'Fabletics'
where att.timestamp != '0'
  and att.type in ('OPT_OUT') --only grab opt_outs
  and att.meta_update_datetime >=  dateadd('DAY', -2, $watermark_date )
qualify rn = 1;

create or replace temp table _db_phone as
select c.customer_id,
       regexp_replace(phone, '\\(|\\)|-|\\+| |\\/', '') as phone_cleaned,
       case
           when length(phone_cleaned) = 10 then concat('+1', phone_cleaned) --assumed to be US number, adding country code
           when length(phone_cleaned) = 11 then concat('+', phone_cleaned) --assume correct contry code added, add+
           else null end                       as phone_formatted   --ignore other numbers, will throw error for salesfloor
from edw_prod.data_model.dim_Customer c
         join lake_consolidated_view.ultra_merchant.address a on a.address_id = c.default_address_id
where phone is not null
and c.meta_update_datetime >=  $watermark_date
;

create or replace temp table _att_phone as
    select distinct CUSTOMER_ID, phone, max(TIMESTAMP) as max_time,
                    rank() over (partition by CUSTOMER_ID order by max_time, phone desc) rn
from reporting_prod.shared.VW_ATTENTIVE_SMS_FL
where CUSTOMER_ID is not null
and phone is not null
and meta_update_datetime >= dateadd('DAY', -2, $watermark_date )
group by 1, 2
qualify rn = 1;

--customers who placed a retail order in last 12 months
CREATE OR REPLACE TEMP TABLE _retail_customers as
SELECT distinct
    fo.customer_id,
    dc.email,
    dc.first_name,
    dc.last_name,
    '{"id":'||default_address_id||',"address_line_1":"'||default_address1||'","address_line_2":"'||default_address1
        ||'","postal_code":'||default_postal_code||',"state":"'||default_state_province||'","city":"'||default_city
        ||'","country":"'||country_code||'","label":"Home","is_default":1}' AS addresses,
    '{"year": '||iff(birth_year=-1,1900, BIRTH_YEAR)||',"month":'||iff(birth_month=-1,1, birth_month)||',"date":'||'1'||'}' as birthday,
    '{"year":'|| date_part(year,activation_local_datetime) || ',"month":' || date_part(month,activation_local_datetime)
        || ',"date":' || date_part(day,activation_local_datetime) || '}' as anniversary,
    workday_id as employee_id,
    row_number() over (partition by fo.customer_id order by order_local_datetime desc) AS associate_rank,
    retail_store_id AS store_id,
    iff(email.customer_id is not null, true, false) as email_opted_out,
    iff(sms.customer_id is not null, true, false) as sms_opted_out,
    coalesce(attp.phone, dbp.phone_formatted) as phone_number
FROM edw_prod.data_model.fact_order fo
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = fo.customer_id
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = fo.store_id
JOIN edw_prod.data_model.dim_order_status dos ON dos.order_status_key = fo.order_status_key
JOIN edw_prod.data_model.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
LEFT JOIN _missing_administrator_ids m ON fo.order_id = m.order_id
LEFT JOIN _workday_employee_ids w ON w.associate_id= iff(fo.administrator_id = -1,m.administrator_id ,fo.administrator_id)  AND fo.order_local_datetime >= w.start_datetime AND fo.order_local_datetime < w.end_datetime
JOIN _nearest_store ns on ns.customer_id = fo.customer_id AND distance_rank = 1
LEFT JOIN edw_prod.data_model.fact_activation fa on fa.customer_id = fo.customer_id AND FA.is_current = 'TRUE'
LEFT JOIN _emps_or_fake_customers e on e.customer_id = fo.customer_id
left join _email_opt_out email on edw_prod.stg.udf_unconcat_brand(dc.customer_id) = email.customer_id
left join _sms_opt_out sms on edw_prod.stg.udf_unconcat_brand(dc.customer_id) = sms.customer_id
left join _db_phone dbp on dbp.customer_id = dc.CUSTOMER_ID
left join _att_phone attp on attp.customer_id = edw_prod.stg.udf_unconcat_brand(dc.customer_id)
WHERE store_brand in ('Fabletics','Yitty')
AND ds.store_type = 'Retail'
AND store_region = 'NA'
AND order_local_datetime::date >= dateadd('month',-12,current_date)
AND TO_DATE(fo.META_UPDATE_DATETIME) >= $watermark_date
AND order_status = 'Success'
AND order_classification_l1 = 'Product Order'
AND dosc.is_product_seeding_order = 'FALSE'
AND e.customer_id IS NULL;

--online vips who live within 30 minutes of store
CREATE OR REPLACE TEMP TABLE _online_vips as
SELECT distinct
    fo.customer_id,
    dc.email,
    dc.first_name,
    dc.last_name,
    '{"id":'||default_address_id||',"address_line_1":"'||default_address1||'","address_line_2":"'||default_address1
        ||'","postal_code":'||default_postal_code||',"state":"'||default_state_province||'","city":"'||default_city
        ||'","country":"'||country_code||'","label":"Home","is_default":1}' AS addresses,
    '{"year": '||iff(birth_year=-1,1900, BIRTH_YEAR)||',"month":'||iff(birth_month=-1,1, birth_month)||',"date":'||'1'||'}' as birthday,
    '{"year":'|| date_part(year,activation_local_datetime) || ',"month":' || date_part(month,activation_local_datetime)
        || ',"date":' || date_part(day,activation_local_datetime) || '}' as anniversary,
    workday_id as employee_id,
    row_number() over (partition by fo.customer_id order by order_local_datetime desc) AS associate_rank,
    retail_store_id AS store_id,
    iff(email.customer_id is not null, true, false) as email_opted_out,
    iff(sms.customer_id is not null, true, false) as sms_opted_out,
    coalesce(attp.phone, dbp.phone_formatted) as phone_number
FROM edw_prod.data_model.fact_order fo
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = fo.customer_id
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = fo.store_id
JOIN edw_prod.data_model.dim_order_status dos ON dos.order_status_key = fo.order_status_key
JOIN edw_prod.data_model.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
LEFT JOIN _missing_administrator_ids m ON fo.order_id = m.order_id
LEFT JOIN _workday_employee_ids w ON w.associate_id= iff(fo.administrator_id = -1,m.administrator_id ,fo.administrator_id)
                                         AND fo.order_local_datetime >= w.start_datetime AND fo.order_local_datetime < w.end_datetime
JOIN _nearest_store ns on ns.customer_id = fo.customer_id AND distance_rank = 1 --wat?
LEFT JOIN edw_prod.data_model.fact_activation fa on fa.customer_id = fo.customer_id AND FA.is_current = 'TRUE'
    and fa.CANCELLATION_LOCAL_DATETIME > current_timestamp() --VIPs
LEFT JOIN _emps_or_fake_customers e on e.customer_id = fo.customer_id
left join _email_opt_out email on edw_prod.stg.udf_unconcat_brand(dc.customer_id) = email.customer_id
left join _sms_opt_out sms on edw_prod.stg.udf_unconcat_brand(dc.customer_id) = sms.customer_id
left join _db_phone dbp on dbp.customer_id = dc.CUSTOMER_ID
left join _att_phone attp on attp.customer_id = edw_prod.stg.udf_unconcat_brand(dc.customer_id)
WHERE store_brand in ('Fabletics','Yitty')
AND ds.store_type != 'Retail'
AND store_region = 'NA'
AND order_local_datetime::date >= dateadd('month',-12,current_date)
AND TO_DATE(fo.META_UPDATE_DATETIME) >= $watermark_date
AND order_status = 'Success'
AND order_classification_l1 = 'Product Order'
AND dosc.is_product_seeding_order = 'FALSE'
AND e.customer_id IS NULL

and fo.customer_id not in (select distinct customer_id from _retail_customers);

-- online leads within 30 minutes retail drive time
CREATE OR REPLACE TEMP TABLE _online_leads as
SELECT distinct
    fme.customer_id,
    dc.email,
    dc.first_name,
    dc.last_name,
    '{"id":'||default_address_id||',"address_line_1":"'||default_address1||'","address_line_2":"'||default_address1
        ||'","postal_code":'||default_postal_code||',"state":"'||default_state_province||'","city":"'||default_city
        ||'","country":"'||country_code||'","label":"Home","is_default":1}'AS addresses,
    '{"year": '||iff(birth_year=-1,1900, BIRTH_YEAR)||',"month":'||iff(birth_month=-1,1, birth_month)||',"date":'||'1'||'}' as birthday,
    '{"year":'|| null || ',"month":' || null || ',"date":' || null || '}' as anniversary,
    retail_store_id AS store_id,
    iff(email.customer_id is not null, true, false) as email_opted_out,
    iff(sms.customer_id is not null, true, false) as sms_opted_out,
    coalesce(attp.phone, dbp.phone_formatted) as phone_number
FROM edw_prod.data_model.fact_membership_event fme
JOIN edw_prod.data_model.dim_store ds on ds.store_id = fme.store_id
JOIN edw_prod.data_model.dim_customer dc ON fme.customer_id = dc.customer_id
JOIN _nearest_store ns on ns.customer_id = dc.customer_id AND distance_rank = 1
LEFT JOIN _emps_or_fake_customers e on e.customer_id = fme.customer_id
left join _email_opt_out email on edw_prod.stg.udf_unconcat_brand(dc.customer_id) = email.customer_id
left join _sms_opt_out sms on edw_prod.stg.udf_unconcat_brand(dc.customer_id) = sms.customer_id
left join _db_phone dbp on dbp.customer_id = dc.CUSTOMER_ID
left join _att_phone attp on attp.customer_id = edw_prod.stg.udf_unconcat_brand(dc.customer_id)
WHERE ds.store_brand IN ('Fabletics','Yitty')
AND ds.store_type = 'Online'
AND ds.store_region = 'NA'
AND fme.membership_state = 'Lead' --ADD FILTER FOR REGISTERED WITHIN X NUMBER OF MONTHS/DAYS
  and IS_CURRENT = 1
  and EVENT_START_LOCAL_DATETIME between dateadd(month, -4, current_date) and dateadd(month, -1, current_date) --Give ECOM/CRM a chance to convert
  and TO_DATE(fme.META_UPDATE_DATETIME) >= $watermark_date
AND e.customer_id IS NULL;

CREATE OR REPLACE TEMP TABLE _customer_feed_no_tag as
SELECT
    customer_id,
    first_name AS first_name,
    last_name AS last_name,
    email AS email_default,
    NULL AS email_alt,
    phone_number AS phone_def,
    NULL AS phone_alt,
    NULL AS status,
    store_id,
    /*ifnull(employee_id, '')*/ '' as employee_id,
    NULL AS secondary_emp_id,
    NULL AS secondary_store_id,
    NULL AS sf_contact_record_id,
    addresses AS addresses,
    NULL AS facebook,
    NULL AS twitter,
    NULL AS instagram,
    NULL AS website,
    birthday AS birthday,
    anniversary AS anniversary,
    NULL AS misc_date,
    NULL AS crm_last_processed,
    '' AS sf_email_subscription, --if opted out of either, then opted out of both, otherwise 1
    NULL AS preferred_language,
    NULL AS extended_attributes,
    NULL AS limited_visibility,
    '' AS sf_sms_subscription --if opted out of either, then opted out of both, otherwise 1
FROM _retail_customers
WHERE associate_rank = 1
and iff(email_opted_out or sms_opted_out, 0, 1) = 1   --filter out unsubbed
and store_id is not null
--and not addresses ilike '%-1%'
UNION
SELECT
    customer_id,
    first_name AS first_name,
    last_name AS last_name,
    email AS email_default,
    NULL AS email_alt,
    phone_number AS phone_def,
    NULL AS phone_alt,
    NULL AS status,
    store_id,
    /*ifnull(employee_id, '')*/ '' as employee_id,
    NULL AS secondary_emp_id,
    NULL AS secondary_store_id,
    NULL AS sf_contact_record_id,
    addresses AS addresses,
    NULL AS facebook,
    NULL AS twitter,
    NULL AS instagram,
    NULL AS website,
    birthday AS birthday,
    anniversary AS anniversary,
    NULL AS misc_date,
    NULL AS crm_last_processed,
    '' AS sf_email_subscription,
    NULL AS preferred_language,
    NULL AS extended_attributes,
    NULL AS limited_visibility,
    '' AS sf_sms_subscription
FROM _online_vips
WHERE iff(email_opted_out or sms_opted_out, 0, 1) = 1
and store_id is not null
--and not addresses ilike '%-1%'
UNION
SELECT
    customer_id,
    first_name AS first_name,
    last_name AS last_name,
    email AS email_default,
    NULL AS email_alt,
    phone_number AS phone_def,
    NULL AS phone_alt,
    NULL AS status,
    store_id AS store_id,
    /*ifnull(employee_id, '')*/ '' as employee_id,
    NULL AS secondary_emp_id,
    NULL AS secondary_store_id,
    NULL AS sf_contact_record_id,
    addresses AS addresses,
    NULL AS facebook,
    NULL AS twitter,
    NULL AS instagram,
    NULL AS website,
    birthday AS birthday,
    anniversary AS anniversary,
    NULL AS misc_date,
    NULL AS crm_last_processed,
    '' AS sf_email_subscription,
    NULL AS preferred_language,
    NULL AS extended_attributes,
    NULL AS limited_visibility,
    '' AS sf_sms_subscription
FROM _online_leads
WHERE iff(email_opted_out or sms_opted_out, 0, 1) = 1
and store_id is not null;

CREATE OR REPLACE TEMP TABLE _customer_feed as
SELECT
    cfnt.*,
    case
        when falt.MEMBERSHIP_REWARD_TIER = 'Gold' then 'tagGOLD'
        when falt.MEMBERSHIP_REWARD_TIER = 'Platinum' then 'tagPLATINUM'
        when falt.MEMBERSHIP_REWARD_TIER = 'Black' then 'tagBLACK'
        when falt.MEMBERSHIP_REWARD_TIER IS NULL then 'tagLEAD'
    end as tag_id
FROM _customer_feed_no_tag cfnt
LEFT JOIN (
    select
        customer_id,
        MEMBERSHIP_REWARD_TIER,
        row_number() over (partition by customer_id order by EVENT_START_LOCAL_DATETIME desc) as ranking
    from edw_prod.DATA_MODEL_FL.FACT_ACTIVATION_LOYALTY_TIER
) falt
ON edw_prod.stg.udf_unconcat_brand(cfnt.customer_id) = falt.customer_id and falt.ranking = 1;

ALTER TABLE
    _customer_feed
ADD
    COLUMN meta_row_hash INT;

UPDATE
    _customer_feed
SET
    meta_row_hash = HASH(
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL_DEFAULT,
        EMAIL_ALT,
        PHONE_DEF,
        PHONE_ALT,
        STATUS,
        STORE_ID,
        EMPLOYEE_ID,
        TAG_ID,
        SECONDARY_EMP_ID,
        SECONDARY_STORE_ID,
        SF_CONTACT_RECORD_ID,
        ADDRESSES,
        FACEBOOK,
        TWITTER,
        INSTAGRAM,
        WEBSITE,
        BIRTHDAY,
        ANNIVERSARY,
        MISC_DATE,
        CRM_LAST_PROCESSED,
        SF_EMAIL_SUBSCRIPTION,
        PREFERRED_LANGUAGE,
        EXTENDED_ATTRIBUTES,
        LIMITED_VISIBILITY,
        SF_SMS_SUBSCRIPTION
    );

MERGE INTO reporting_prod.salesfloor.customer_feed as c using (
    SELECT
        *
    FROM
        _customer_feed
) as cf on c.CUSTOMER_ID = cf.CUSTOMER_ID
and c.STORE_ID = cf.STORE_ID
and c.EMPLOYEE_ID = cf.EMPLOYEE_ID
WHEN MATCHED
AND c.meta_row_hash != cf.meta_row_hash then
UPDATE
SET
    c.CUSTOMER_ID = cf.CUSTOMER_ID,
    c.FIRST_NAME = cf.FIRST_NAME,
    c.LAST_NAME = cf.LAST_NAME,
    c.EMAIL_DEFAULT = cf.EMAIL_DEFAULT,
    c.EMAIL_ALT = cf.EMAIL_ALT,
    c.PHONE_DEF = cf.PHONE_DEF,
    c.PHONE_ALT = cf.PHONE_ALT,
    c.STATUS = cf.STATUS,
    c.STORE_ID = cf.STORE_ID,
    c.EMPLOYEE_ID = cf.EMPLOYEE_ID,
    c.TAG_ID = cf.TAG_ID,
    c.SECONDARY_EMP_ID = cf.SECONDARY_EMP_ID,
    c.SECONDARY_STORE_ID = cf.SECONDARY_STORE_ID,
    c.SF_CONTACT_RECORD_ID = cf.SF_CONTACT_RECORD_ID,
    c.ADDRESSES = cf.ADDRESSES,
    c.FACEBOOK = cf.FACEBOOK,
    c.TWITTER = cf.TWITTER,
    c.INSTAGRAM = cf.INSTAGRAM,
    c.WEBSITE = cf.WEBSITE,
    c.BIRTHDAY = cf.BIRTHDAY,
    c.ANNIVERSARY = cf.ANNIVERSARY,
    c.MISC_DATE = cf.MISC_DATE,
    c.CRM_LAST_PROCESSED = cf.CRM_LAST_PROCESSED,
    c.SF_EMAIL_SUBSCRIPTION = cf.SF_EMAIL_SUBSCRIPTION,
    c.PREFERRED_LANGUAGE = cf.PREFERRED_LANGUAGE,
    c.EXTENDED_ATTRIBUTES = cf.EXTENDED_ATTRIBUTES,
    c.LIMITED_VISIBILITY = cf.LIMITED_VISIBILITY,
    c.SF_SMS_SUBSCRIPTION = cf.SF_SMS_SUBSCRIPTION,
    c.meta_row_hash = cf.meta_row_hash,
    c.meta_update_datetime = $current_datetime
    WHEN NOT MATCHED THEN
INSERT
    (
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL_DEFAULT,
        EMAIL_ALT,
        PHONE_DEF,
        PHONE_ALT,
        STATUS,
        STORE_ID,
        EMPLOYEE_ID,
        TAG_ID,
        SECONDARY_EMP_ID,
        SECONDARY_STORE_ID,
        SF_CONTACT_RECORD_ID,
        ADDRESSES,
        FACEBOOK,
        TWITTER,
        INSTAGRAM,
        WEBSITE,
        BIRTHDAY,
        ANNIVERSARY,
        MISC_DATE,
        CRM_LAST_PROCESSED,
        SF_EMAIL_SUBSCRIPTION,
        PREFERRED_LANGUAGE,
        EXTENDED_ATTRIBUTES,
        LIMITED_VISIBILITY,
        SF_SMS_SUBSCRIPTION,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
    )
VALUES
    (
        cf.CUSTOMER_ID,
        cf.FIRST_NAME,
        cf.LAST_NAME,
        cf.EMAIL_DEFAULT,
        cf.EMAIL_ALT,
        cf.PHONE_DEF,
        cf.PHONE_ALT,
        cf.STATUS,
        cf.STORE_ID,
        cf.EMPLOYEE_ID,
        cf.TAG_ID,
        cf.SECONDARY_EMP_ID,
        cf.SECONDARY_STORE_ID,
        cf.SF_CONTACT_RECORD_ID,
        cf.ADDRESSES,
        cf.FACEBOOK,
        cf.TWITTER,
        cf.INSTAGRAM,
        cf.WEBSITE,
        cf.BIRTHDAY,
        cf.ANNIVERSARY,
        cf.MISC_DATE,
        cf.CRM_LAST_PROCESSED,
        cf.SF_EMAIL_SUBSCRIPTION,
        cf.PREFERRED_LANGUAGE,
        cf.EXTENDED_ATTRIBUTES,
        cf.LIMITED_VISIBILITY,
        cf.SF_SMS_SUBSCRIPTION,
        cf.meta_row_hash,
        $current_datetime,
        $current_datetime
    );

UPDATE reporting_prod.public.meta_table_dependency_watermark
SET
    high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime = $current_datetime::timestamp_ltz(3)
WHERE table_name = $target_table;


--Update tagTOP50 tag_id value
create temp table _top50 as (
    select
        store_id,
        customer_id,
        product_gross_revenue,
        rank() over (partition by store_id order by product_gross_revenue desc) as ranking
    from
    (
        select
            cf.customer_id,
            cf.store_id,
            sum(fo.PRODUCT_GROSS_REVENUE_LOCAL_AMOUNT) as product_gross_revenue
        from reporting_prod.salesfloor.customer_feed cf
        left join EDW_PROD.DATA_MODEL_FL.fact_order fo
        on edw_prod.stg.udf_unconcat_brand(cf.customer_id) = fo.customer_id
        where tag_id = 'tagBLACK'
        group by cf.customer_id, cf.store_id
    )
);

update reporting_prod.salesfloor.customer_feed cf
Set
    tag_id = concat(tag_id, ';tagTOP50'),
    meta_update_datetime = $current_datetime
from _top50 t
where cf.customer_id = t.customer_id
and ranking < 51
and tag_id not ilike '%tagTOP50%';

update reporting_prod.salesfloor.customer_feed cf
Set
    tag_id = replace(tag_id, ';tagTOP50', ''),
    meta_update_datetime = $current_datetime
from (
    select
        cf.customer_id as cf_customer_id,
        t50.customer_id as t50_customer_id
    from reporting_prod.salesfloor.customer_feed cf
    left join _top50 t50
    on cf.customer_id = t50.customer_id
    and t50.ranking < 51
) joined
where cf.customer_id = joined.cf_customer_id
and t50_customer_id is null
and tag_id ilike '%tagTOP50%';


--Added to allow flagging of records to send in next export by
-- setting meta_update_datetime to NULL
UPDATE reporting_prod.salesfloor.customer_feed
SET meta_update_datetime = $current_datetime
WHERE meta_update_datetime IS NULL;
