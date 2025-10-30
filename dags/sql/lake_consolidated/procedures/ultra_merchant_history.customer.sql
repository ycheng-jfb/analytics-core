--Full refresh
create or replace TRANSIENT TABLE lake_consolidated.ultra_merchant_history.CUSTOMER (
    data_source_id INT,
    meta_company_id INT,
    customer_id INT,
    store_group_id INT,
    store_id INT,
    customer_type_id INT,
    customer_key VARCHAR(50),
    username VARCHAR(75),
    password VARCHAR(150) WITH TAG (UTIL.PUBLIC.PII_INFO_TAG='PII'),
    email VARCHAR(75) WITH TAG (UTIL.PUBLIC.PII_INFO_TAG='PII'),
    firstname VARCHAR(25) WITH TAG (UTIL.PUBLIC.PII_INFO_TAG='PII'),
    lastname VARCHAR(25) WITH TAG (UTIL.PUBLIC.PII_INFO_TAG='PII'),
    name VARCHAR(50) WITH TAG (UTIL.PUBLIC.PII_INFO_TAG='PII'),
    company VARCHAR(100),
    default_address_id INT,
    default_discount_id INT,
    datetime_added TIMESTAMP_NTZ(3),
    datetime_modified TIMESTAMP_NTZ(3),
    statuscode INT,
    password_salt VARCHAR(25) WITH TAG (UTIL.PUBLIC.PII_INFO_TAG='PII'),
    ph_method VARCHAR(25),
    customer_testing_group INT,
    meta_original_customer_id INT,
    meta_row_source VARCHAR(40),
    hvr_change_op INT,
    effective_start_datetime TIMESTAMP_LTZ(3),
    effective_end_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3)
);


SET lake_jfb_watermark = (
    select min(last_update)
    from (
        select cast(min(META_SOURCE_CHANGE_DATETIME) as TIMESTAMP_LTZ(3)) AS last_update
        FROM lake_jfb.ultra_merchant_history.CUSTOMER
    ) AS A
);


create or replace TEMP TABLE _lake_jfb_customer_history AS
select distinct
    customer_id,
    store_group_id,
    store_id,
    customer_type_id,
    customer_key,
    username,
    password,
    email,
    firstname,
    lastname,
    name,
    company,
    default_address_id,
    default_discount_id,
    datetime_added,
    datetime_modified,
    statuscode,
    password_salt,
    ph_method,
    customer_testing_group,
    meta_row_source,
    hvr_change_op,
    cast(META_SOURCE_CHANGE_DATETIME as TIMESTAMP_LTZ(3))  as effective_start_datetime,
    10 as data_source_id
FROM lake_jfb.ultra_merchant_history.CUSTOMER
WHERE META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
QUALIFY ROW_NUMBER() OVER(PARTITION BY customer_id, META_SOURCE_CHANGE_DATETIME
    ORDER BY HVR_CHANGE_SEQUENCE DESC) = 1;


CREATE OR REPLACE TEMP TABLE _lake_jfb_company_customer AS
SELECT customer_id, company_id as meta_company_id
FROM (
     SELECT DISTINCT
         L.CUSTOMER_ID,
         DS.COMPANY_ID
     FROM lake_jfb.REFERENCE.DIM_STORE AS DS
     INNER JOIN lake_jfb.ultra_merchant_history.customer AS L
     ON DS.STORE_ID = L.STORE_ID
    WHERE l.META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
    ) AS l
QUALIFY ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY company_id ASC) = 1;


INSERT INTO lake_consolidated.ultra_merchant_history.CUSTOMER (
    data_source_id,
    meta_company_id,
    customer_id,
    store_group_id,
    store_id,
    customer_type_id,
    customer_key,
    username,
    password,
    email,
    firstname,
    lastname,
    name,
    company,
    default_address_id,
    default_discount_id,
    datetime_added,
    datetime_modified,
    statuscode,
    password_salt,
    ph_method,
    customer_testing_group,
    meta_original_customer_id,
    meta_row_source,
    hvr_change_op,
    effective_start_datetime,
    effective_end_datetime,
    meta_update_datetime
)
SELECT DISTINCT
    s.data_source_id,
    s.meta_company_id,

    CASE WHEN NULLIF(s.customer_id, '0') IS NOT NULL THEN CONCAT(s.customer_id, s.meta_company_id) ELSE NULL END as customer_id,
    s.store_group_id,
    s.store_id,
    s.customer_type_id,
    s.customer_key,
    s.username,
    s.password,
    s.email,
    s.firstname,
    s.lastname,
    s.name,
    s.company,
    CASE WHEN NULLIF(s.default_address_id, '0') IS NOT NULL THEN CONCAT(s.default_address_id, s.meta_company_id) ELSE NULL END as default_address_id,
    CASE WHEN NULLIF(s.default_discount_id, '0') IS NOT NULL THEN CONCAT(s.default_discount_id, s.meta_company_id) ELSE NULL END as default_discount_id,
    s.datetime_added,
    s.datetime_modified,
    s.statuscode,
    s.password_salt,
    s.ph_method,
    s.customer_testing_group,
    s.customer_id as meta_original_customer_id,
    s.meta_row_source,
    s.hvr_change_op,
    s.effective_start_datetime,
    NULL AS effective_end_datetime,
    CURRENT_TIMESTAMP() AS meta_update_datetime
FROM (
    SELECT
        A.*,
        COALESCE(CJ.meta_company_id, 40) AS meta_company_id
    FROM _lake_jfb_customer_history AS A
    LEFT JOIN _lake_jfb_company_customer AS CJ
        ON cj.customer_id = a.customer_id
    WHERE COALESCE(CJ.meta_company_id, 40) IN (10)
    ) AS s
WHERE NOT EXISTS (
    SELECT
        1
    FROM lake_consolidated.ultra_merchant_history.CUSTOMER AS t
    WHERE
        s.data_source_id = t.data_source_id
        AND          s.customer_id = t.meta_original_customer_id
        AND s.effective_start_datetime = t.effective_start_datetime
    )
ORDER BY s.effective_start_datetime ASC;


CREATE OR REPLACE temp table _customer_updates as
SELECT s.*,
    row_number() over(partition by s.data_source_id,

        s.meta_original_customer_id
ORDER BY s.effective_start_datetime ASC) AS rnk
FROM (
SELECT DISTINCT
    s.data_source_id,

        s.meta_original_customer_id,
    s.effective_start_datetime,
    s.effective_end_datetime
FROM lake_consolidated.ultra_merchant_history.CUSTOMER as s
INNER JOIN (
    SELECT
        data_source_id,

        meta_original_customer_id,
        effective_start_datetime
    FROM lake_consolidated.ultra_merchant_history.CUSTOMER
    WHERE effective_end_datetime is NULL
    ) AS t
    ON s.data_source_id = t.data_source_id
    AND     s.meta_original_customer_id = t.meta_original_customer_id
WHERE (
    s.effective_start_datetime >= t.effective_start_datetime
    OR s.effective_end_datetime = '9999-12-31 00:00:00.000 -0800'
)) s;


CREATE OR REPLACE TEMP TABLE _customer_delta AS
SELECT
    s.data_source_id,

        s.meta_original_customer_id,
    s.effective_start_datetime,
    COALESCE(dateadd(MILLISECOND, -1, t.effective_start_datetime), '9999-12-31 00:00:00.000 -0800') AS new_effective_end_datetime
FROM _customer_updates AS s
    LEFT JOIN _customer_updates AS t
    ON s.data_source_id = t.data_source_id
    AND     s.meta_original_customer_id = t.meta_original_customer_id
    AND S.rnk = T.rnk - 1
WHERE (S.effective_end_datetime <> dateadd(MILLISECOND, -1, T.effective_start_datetime)
    OR S.effective_end_datetime IS NULL) ;


UPDATE lake_consolidated.ultra_merchant_history.CUSTOMER AS t
SET t.effective_end_datetime = s.new_effective_end_datetime,
    META_UPDATE_DATETIME = current_timestamp()
FROM _customer_delta AS s
WHERE s.data_source_id = t.data_source_id
    AND     s.meta_original_customer_id = t.meta_original_customer_id
    AND s.effective_start_datetime = t.effective_start_datetime
    AND COALESCE(t.effective_end_datetime, '1900-01-01 00:00:00 -0800') <> s.new_effective_end_datetime
