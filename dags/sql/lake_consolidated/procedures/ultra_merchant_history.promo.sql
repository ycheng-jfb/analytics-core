CREATE or replace TRANSIENT TABLE lake_consolidated.ultra_merchant_history.PROMO (
    data_source_id INT,
    meta_company_id INT,
    promo_id INT,
    store_group_id INT,
    promo_type_id INT,
    subtotal_discount_id INT,
    shipping_discount_id INT,
    product_id INT,
    product_category_id INT,
    membership_plan_id INT,
    required_product_id INT,
    required_product_category_id INT,
    required_membership_plan_id INT,
    shipping_option_id INT,
    required_promo_id INT,
    code VARCHAR(50),
    label VARCHAR(50),
    description VARCHAR(255),
    min_subtotal NUMBER(19, 4),
    min_purchase_unit_price NUMBER(19, 4),
    min_required_purchase_unit_price NUMBER(19, 4),
    max_product_quantity INT,
    min_product_quantity INT,
    required_product_quantity INT,
    max_uses_per_customer INT,
    max_discount_sets_per_use INT,
    rush_shipping_discount_allowed INT,
    apply_automatically INT,
    add_product_automatically INT,
    remove_product_automatically INT,
    membership_promo_expiration_days INT,
    membership_promo_current_period_only INT,
    allow_membership_credits INT,
    allow_like_promos INT,
    date_start TIMESTAMP_NTZ(0),
    date_end TIMESTAMP_NTZ(0),
    datetime_added TIMESTAMP_NTZ(3),
    statuscode INT,
    max_purchase_unit_price NUMBER(19, 4),
    max_required_purchase_unit_price NUMBER(19, 4),
    allow_prepaid_creditcard INT,
    datetime_modified TIMESTAMP_NTZ(3),
    featured_product_location_id INT,
    required_featured_product_location_id INT,
    restrict_code_access_to_specific_memberships INT,
    terms VARCHAR,
    time_start TIME,
    time_end TIME,
    long_label VARCHAR(2550),
    refunds_allowed INT,
    exchanges_allowed INT,
    membership_reward_multiplier NUMBER(18, 1),
    group_code VARCHAR(50),
    filtered_featured_product_location_id INT,
    filtered_product_category_id INT,
    filtered_benefit_featured_product_location_id INT,
    filtered_benefit_product_category_id INT,
    display_on_pdp INT,
    pdp_label VARCHAR(255),
    max_uses_per_day INT,
    store_domain_type_id_list VARCHAR(50),
    membership_brand_id_list VARCHAR(50),
    qualification_product_include_membership_brand_id_list VARCHAR(50),
    qualification_product_exclude_membership_brand_id_list VARCHAR(50),
    benefit_product_include_membership_brand_id_list VARCHAR(50),
    benefit_product_exclude_membership_brand_id_list VARCHAR(50),
    rush_shipping_discount_id NUMBER(38,0),
    meta_original_promo_id INT,
    meta_original_required_promo_id INT,
    meta_row_source VARCHAR(40),
    hvr_change_op INT,
    effective_start_datetime TIMESTAMP_LTZ(3),
    effective_end_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3)
);


SET lake_jfb_watermark = (
    SELECT MIN(last_update)
    FROM (
        SELECT CAST(min(META_SOURCE_CHANGE_DATETIME) AS TIMESTAMP_LTZ(3)) AS last_update
        FROM lake_jfb.ultra_merchant_history.PROMO
    ) AS A
);


CREATE OR REPLACE TEMP TABLE _lake_jfb_promo_history AS
SELECT DISTINCT

    promo_id,
    store_group_id,
    promo_type_id,
    subtotal_discount_id,
    shipping_discount_id,
    product_id,
    product_category_id,
    membership_plan_id,
    required_product_id,
    required_product_category_id,
    required_membership_plan_id,
    shipping_option_id,
    required_promo_id,
    code,
    label,
    description,
    min_subtotal,
    min_purchase_unit_price,
    min_required_purchase_unit_price,
    max_product_quantity,
    min_product_quantity,
    required_product_quantity,
    max_uses_per_customer,
    max_discount_sets_per_use,
    rush_shipping_discount_allowed,
    apply_automatically,
    add_product_automatically,
    remove_product_automatically,
    membership_promo_expiration_days,
    membership_promo_current_period_only,
    allow_membership_credits,
    allow_like_promos,
    date_start,
    date_end,
    datetime_added,
    statuscode,
    max_purchase_unit_price,
    max_required_purchase_unit_price,
    allow_prepaid_creditcard,
    datetime_modified,
    featured_product_location_id,
    required_featured_product_location_id,
    restrict_code_access_to_specific_memberships,
    terms,
    time_start,
    time_end,
    long_label,
    refunds_allowed,
    exchanges_allowed,
    membership_reward_multiplier,
    group_code,
    filtered_featured_product_location_id,
    filtered_product_category_id,
    filtered_benefit_featured_product_location_id,
    filtered_benefit_product_category_id,
    display_on_pdp,
    pdp_label,
    max_uses_per_day,
    store_domain_type_id_list,
    membership_brand_id_list,
    qualification_product_include_membership_brand_id_list,
    qualification_product_exclude_membership_brand_id_list,
    benefit_product_include_membership_brand_id_list,
    benefit_product_exclude_membership_brand_id_list,
    rush_shipping_discount_id,
    meta_row_source,
    hvr_change_op,
    CAST(META_SOURCE_CHANGE_DATETIME AS TIMESTAMP_LTZ(3))  as effective_start_datetime,
    10 as data_source_id
FROM lake_jfb.ultra_merchant_history.PROMO
WHERE META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
QUALIFY ROW_NUMBER() OVER(PARTITION BY promo_id, META_SOURCE_CHANGE_DATETIME
    ORDER BY HVR_CHANGE_SEQUENCE DESC) = 1;


CREATE OR REPLACE TEMP TABLE _lake_jfb_company_promo AS
SELECT promo_id, company_id as meta_company_id
FROM (SELECT DISTINCT L.promo_id, DS.company_id
    from lake_jfb.REFERENCE.dim_store ds
    join lake_jfb.ultra_merchant_history.promo L
            on ds.store_group_id=L.store_group_id
    WHERE l.META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
    ) AS l
QUALIFY ROW_NUMBER() OVER(PARTITION BY promo_id ORDER BY company_id ASC) = 1;


INSERT INTO lake_consolidated.ultra_merchant_history.PROMO (
    data_source_id,
    meta_company_id,
    promo_id,
    store_group_id,
    promo_type_id,
    subtotal_discount_id,
    shipping_discount_id,
    product_id,
    product_category_id,
    membership_plan_id,
    required_product_id,
    required_product_category_id,
    required_membership_plan_id,
    shipping_option_id,
    required_promo_id,
    code,
    label,
    description,
    min_subtotal,
    min_purchase_unit_price,
    min_required_purchase_unit_price,
    max_product_quantity,
    min_product_quantity,
    required_product_quantity,
    max_uses_per_customer,
    max_discount_sets_per_use,
    rush_shipping_discount_allowed,
    apply_automatically,
    add_product_automatically,
    remove_product_automatically,
    membership_promo_expiration_days,
    membership_promo_current_period_only,
    allow_membership_credits,
    allow_like_promos,
    date_start,
    date_end,
    datetime_added,
    statuscode,
    max_purchase_unit_price,
    max_required_purchase_unit_price,
    allow_prepaid_creditcard,
    datetime_modified,
    featured_product_location_id,
    required_featured_product_location_id,
    restrict_code_access_to_specific_memberships,
    terms,
    time_start,
    time_end,
    long_label,
    refunds_allowed,
    exchanges_allowed,
    membership_reward_multiplier,
    group_code,
    filtered_featured_product_location_id,
    filtered_product_category_id,
    filtered_benefit_featured_product_location_id,
    filtered_benefit_product_category_id,
    display_on_pdp,
    pdp_label,
    max_uses_per_day,
    store_domain_type_id_list,
    membership_brand_id_list,
    qualification_product_include_membership_brand_id_list,
    qualification_product_exclude_membership_brand_id_list,
    benefit_product_include_membership_brand_id_list,
    benefit_product_exclude_membership_brand_id_list,
    rush_shipping_discount_id,
    meta_original_promo_id,
    meta_row_source,
    hvr_change_op,
    effective_start_datetime,
    effective_end_datetime,
    meta_update_datetime
)
SELECT DISTINCT
    s.data_source_id,
    s.meta_company_id,

    CASE WHEN NULLIF(s.promo_id, '0') IS NOT NULL THEN CONCAT(s.promo_id, s.meta_company_id) ELSE NULL END as promo_id,
    s.store_group_id,
    s.promo_type_id,
    CASE WHEN NULLIF(s.subtotal_discount_id, '0') IS NOT NULL THEN CONCAT(s.subtotal_discount_id, s.meta_company_id) ELSE NULL END as subtotal_discount_id,
    CASE WHEN NULLIF(s.shipping_discount_id, '0') IS NOT NULL THEN CONCAT(s.shipping_discount_id, s.meta_company_id) ELSE NULL END as shipping_discount_id,
    CASE WHEN NULLIF(s.product_id, '0') IS NOT NULL THEN CONCAT(s.product_id, s.meta_company_id) ELSE NULL END as product_id,
    CASE WHEN NULLIF(s.product_category_id, '0') IS NOT NULL THEN CONCAT(s.product_category_id, s.meta_company_id) ELSE NULL END as product_category_id,
    CASE WHEN NULLIF(s.membership_plan_id, '0') IS NOT NULL THEN CONCAT(s.membership_plan_id, s.meta_company_id) ELSE NULL END as membership_plan_id,
    CASE WHEN NULLIF(s.required_product_id, '0') IS NOT NULL THEN CONCAT(s.required_product_id, s.meta_company_id) ELSE NULL END as required_product_id,
    CASE WHEN NULLIF(s.required_product_category_id, '0') IS NOT NULL THEN CONCAT(s.required_product_category_id, s.meta_company_id) ELSE NULL END as required_product_category_id,
    CASE WHEN NULLIF(s.required_membership_plan_id, '0') IS NOT NULL THEN CONCAT(s.required_membership_plan_id, s.meta_company_id) ELSE NULL END as required_membership_plan_id,
    s.shipping_option_id,
    CASE WHEN NULLIF(s.required_promo_id, '0') IS NOT NULL THEN CONCAT(s.required_promo_id, s.meta_company_id) ELSE NULL END as required_promo_id,
    s.code,
    s.label,
    s.description,
    s.min_subtotal,
    s.min_purchase_unit_price,
    s.min_required_purchase_unit_price,
    s.max_product_quantity,
    s.min_product_quantity,
    s.required_product_quantity,
    s.max_uses_per_customer,
    s.max_discount_sets_per_use,
    s.rush_shipping_discount_allowed,
    s.apply_automatically,
    s.add_product_automatically,
    s.remove_product_automatically,
    s.membership_promo_expiration_days,
    s.membership_promo_current_period_only,
    s.allow_membership_credits,
    s.allow_like_promos,
    s.date_start,
    s.date_end,
    s.datetime_added,
    s.statuscode,
    s.max_purchase_unit_price,
    s.max_required_purchase_unit_price,
    s.allow_prepaid_creditcard,
    s.datetime_modified,
    CASE WHEN NULLIF(s.featured_product_location_id, '0') IS NOT NULL THEN CONCAT(s.featured_product_location_id, s.meta_company_id) ELSE NULL END as featured_product_location_id,
    CASE WHEN NULLIF(s.required_featured_product_location_id, '0') IS NOT NULL THEN CONCAT(s.required_featured_product_location_id, s.meta_company_id) ELSE NULL END as required_featured_product_location_id,
    s.restrict_code_access_to_specific_memberships,
    s.terms,
    s.time_start,
    s.time_end,
    s.long_label,
    s.refunds_allowed,
    s.exchanges_allowed,
    s.membership_reward_multiplier,
    s.group_code,
    CASE WHEN NULLIF(s.filtered_featured_product_location_id, '0') IS NOT NULL THEN CONCAT(s.filtered_featured_product_location_id, s.meta_company_id) ELSE NULL END as filtered_featured_product_location_id,
    CASE WHEN NULLIF(s.filtered_product_category_id, '0') IS NOT NULL THEN CONCAT(s.filtered_product_category_id, s.meta_company_id) ELSE NULL END as filtered_product_category_id,
    s.filtered_benefit_featured_product_location_id,
    s.filtered_benefit_product_category_id,
    s.display_on_pdp,
    s.pdp_label,
    s.max_uses_per_day,
    s.store_domain_type_id_list,
    s.membership_brand_id_list,
    s.qualification_product_include_membership_brand_id_list,
    s.qualification_product_exclude_membership_brand_id_list,
    s.benefit_product_include_membership_brand_id_list,
    s.benefit_product_exclude_membership_brand_id_list,
    s.rush_shipping_discount_id,
    s.promo_id as meta_original_promo_id,
    s.meta_row_source,
    s.hvr_change_op,
    s.effective_start_datetime,
    NULL AS effective_end_datetime,
    CURRENT_TIMESTAMP() AS meta_update_datetime
FROM (
    SELECT
        A.*,
        COALESCE(CJ.meta_company_id, 40) AS meta_company_id
    FROM _lake_jfb_promo_history AS A
    LEFT JOIN _lake_jfb_company_promo AS CJ
        ON cj.promo_id = a.promo_id
    WHERE COALESCE(CJ.meta_company_id, 40) IN (10)
    ) AS s
WHERE NOT EXISTS (
    SELECT
        1
    FROM lake_consolidated.ultra_merchant_history.PROMO AS t
    WHERE
        s.data_source_id = t.data_source_id
        AND          s.promo_id = t.meta_original_promo_id
        AND s.effective_start_datetime = t.effective_start_datetime
    )
ORDER BY s.effective_start_datetime ASC;


CREATE OR REPLACE temp table _promo_updates as
SELECT s.*,
    row_number() over(partition by s.data_source_id,

        s.meta_original_promo_id
ORDER BY s.effective_start_datetime ASC) AS rnk
FROM (
SELECT DISTINCT
    s.data_source_id,

        s.meta_original_promo_id,
    s.effective_start_datetime,
    s.effective_end_datetime
FROM lake_consolidated.ultra_merchant_history.PROMO as s
INNER JOIN (
    SELECT
        data_source_id,

        meta_original_promo_id,
        effective_start_datetime
    FROM lake_consolidated.ultra_merchant_history.PROMO
    WHERE effective_end_datetime is NULL
    ) AS t
    ON s.data_source_id = t.data_source_id
    AND     s.meta_original_promo_id = t.meta_original_promo_id
WHERE (
    s.effective_start_datetime >= t.effective_start_datetime
    OR s.effective_end_datetime = '9999-12-31 00:00:00.000 -0800'
)) s;


CREATE OR REPLACE TEMP TABLE _promo_delta AS
SELECT
    s.data_source_id,

        s.meta_original_promo_id,
    s.effective_start_datetime,
    COALESCE(dateadd(MILLISECOND, -1, t.effective_start_datetime), '9999-12-31 00:00:00.000 -0800') AS new_effective_end_datetime
FROM _promo_updates AS s
    LEFT JOIN _promo_updates AS t
    ON s.data_source_id = t.data_source_id
    AND     s.meta_original_promo_id = t.meta_original_promo_id
    AND S.rnk = T.rnk - 1
WHERE (S.effective_end_datetime <> dateadd(MILLISECOND, -1, T.effective_start_datetime)
    OR S.effective_end_datetime IS NULL) ;


UPDATE lake_consolidated.ultra_merchant_history.PROMO AS t
SET t.effective_end_datetime = s.new_effective_end_datetime,
    META_UPDATE_DATETIME = current_timestamp()
FROM _promo_delta AS s
WHERE s.data_source_id = t.data_source_id
    AND     s.meta_original_promo_id = t.meta_original_promo_id
    AND s.effective_start_datetime = t.effective_start_datetime
    AND COALESCE(t.effective_end_datetime, '1900-01-01 00:00:00 -0800') <> s.new_effective_end_datetime;
