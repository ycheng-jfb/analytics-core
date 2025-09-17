SET target_table = 'stg.dim_promo_history';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));
/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;
*/

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_lake_history_ultra_merchant_promo = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.promo'));
SET wm_lake_ultra_merchant_promo_promo_classfication = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.promo_promo_classfication'));
SET wm_lake_ultra_merchant_promo_classification = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.promo_classification'));
SET wm_lake_ultra_merchant_promo_type = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.promo_type'));
SET wm_lake_history_ultra_merchant_discount = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.discount'));
SET wm_lake_ultra_merchant_featured_product_location = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.featured_product_location'));
SET wm_lake_ultra_merchant_product_category = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.product_category'));
SET wm_lake_history_ultra_cms_ui_promo_management_promos = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_cms_history.ui_promo_management_promos'));

/*
SELECT
    $wm_self,
    $wm_lake_history_ultra_merchant_promo,
    $wm_lake_ultra_merchant_promo_promo_classfication,
    $wm_lake_ultra_merchant_promo_classification,
    $wm_lake_ultra_merchant_promo_type,
    $wm_lake_history_ultra_merchant_discount,
    $wm_lake_ultra_merchant_featured_product_location,
    $wm_lake_ultra_merchant_product_category,
    $wm_lake_history_ultra_cms_ui_promo_management_promos;
*/

-- Create a fake dependency
CREATE OR REPLACE TEMP TABLE _temp_lake_promo_solution AS
    SELECT 1 as id FROM lake_consolidated.ultra_merchant.promo LIMIT 1;

CREATE OR REPLACE TEMP TABLE _dim_promo_history__promo_base
    (promo_id INT, current_effective_start_datetime TIMESTAMP_LTZ(3));

-- Full Refresh
INSERT INTO _dim_promo_history__promo_base (promo_id)
SELECT DISTINCT p.promo_id
FROM lake_consolidated.ultra_merchant_history.promo AS p
WHERE $is_full_refresh = TRUE /* SET is_full_refresh = TRUE; */
ORDER BY p.promo_id;

-- Incremental Refresh
INSERT INTO _dim_promo_history__promo_base (promo_id)
SELECT DISTINCT incr.promo_id
FROM (
     /* Self-check for manual updates */
     SELECT dph.promo_id
     FROM stg.dim_promo_history AS dph
     WHERE dph.meta_update_datetime > $wm_self

     UNION ALL

     SELECT p.promo_id
     FROM lake_consolidated.ultra_merchant_history.promo AS p
     WHERE p.meta_update_datetime > $wm_lake_history_ultra_merchant_promo
       AND p.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

     UNION ALL

     SELECT promo_id
     FROM lake_consolidated.ultra_merchant.promo_promo_classfication AS ppc
     WHERE ppc.meta_update_datetime > $wm_lake_ultra_merchant_promo_promo_classfication

     UNION ALL

     SELECT ppc.promo_id
     FROM lake_consolidated.ultra_merchant.promo_promo_classfication AS ppc
              JOIN lake_consolidated.ultra_merchant.promo_classification AS pcl
                   ON pcl.promo_classification_id = ppc.promo_classification_id
     WHERE pcl.meta_update_datetime > $wm_lake_ultra_merchant_promo_classification

     UNION ALL

     SELECT p.promo_id
     FROM lake_consolidated.ultra_merchant_history.promo AS p
              JOIN lake_consolidated.ultra_merchant.promo_type AS pt
                   ON pt.promo_type_id = p.promo_type_id
     WHERE pt.meta_update_datetime > $wm_lake_ultra_merchant_promo_type
       AND p.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'


     UNION ALL

     SELECT p.promo_id
     FROM lake_consolidated.ultra_merchant_history.promo AS p
              JOIN lake_consolidated.ultra_merchant.featured_product_location AS fpl
                   ON fpl.featured_product_location_id = p.featured_product_location_id
     WHERE fpl.meta_update_datetime > $wm_lake_ultra_merchant_featured_product_location
       AND p.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

     UNION ALL

     SELECT p.promo_id
     FROM lake_consolidated.ultra_merchant_history.promo AS p
              JOIN lake_consolidated.ultra_merchant.product_category AS pc
                   ON pc.product_category_id = p.product_category_id
     WHERE pc.meta_update_datetime > $wm_lake_ultra_merchant_product_category
       AND p.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

     UNION ALL

     SELECT p.promo_id
     FROM lake_consolidated.ultra_merchant_history.promo AS p
              JOIN lake_consolidated.ultra_merchant_history.discount AS d
                   ON d.discount_id = p.subtotal_discount_id
     WHERE d.meta_update_datetime > $wm_lake_history_ultra_merchant_discount
       AND p.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'
       AND d.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

     UNION ALL

     SELECT p.promo_id
     FROM lake_consolidated.ultra_merchant_history.promo AS p
              JOIN lake_consolidated.ultra_merchant_history.discount AS d
                   ON d.discount_id = p.shipping_discount_id
     WHERE d.meta_update_datetime > $wm_lake_history_ultra_merchant_discount
       AND p.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'
       AND d.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'


    UNION ALL

    SELECT p.promo_id
    FROM lake_consolidated.ultra_merchant_history.promo AS p
    LEFT JOIN lake_consolidated.ultra_cms_history.ui_promo_management_promos AS upmp
        ON CAST(upmp.ui_promo_management_promo_id AS VARCHAR(100)) = CONCAT(split_part(p.code, '_', -1), p.meta_company_id)
    WHERE upmp.meta_update_datetime > $wm_lake_history_ultra_cms_ui_promo_management_promos
        AND p.code ILIKE 'REV\\_%%' ESCAPE '\\'

    UNION ALL

    SELECT p.promo_id
    FROM lake_consolidated.ultra_merchant_history.promo AS p
    LEFT JOIN lake_consolidated.ultra_cms_history.ui_promo_management_promos AS upmp
        ON upmp.promo_id = p.promo_id
    WHERE upmp.meta_update_datetime > $wm_lake_history_ultra_cms_ui_promo_management_promos
        AND p.code NOT ILIKE 'REV\\_%%' ESCAPE '\\'

    UNION ALL

    -- previously errored rows
    SELECT promo_id
    FROM excp.dim_promo_history
    WHERE meta_is_current_excp
    AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.promo_id;
-- SELECT * FROM _dim_promo_history__promo_base;
-- SELECT promo_id, current_effective_start_datetime, COUNT(1) FROM _dim_promo_history__promo_base GROUP BY 1, 2 HAVING COUNT(1) > 1;

INSERT OVERWRITE INTO _dim_promo_history__promo_base
SELECT
    base.promo_id,
    IFF($is_full_refresh, '1900-01-01', COALESCE(dcdh.effective_start_datetime, '1900-01-01')) AS current_effective_start_datetime
FROM _dim_promo_history__promo_base AS base
    LEFT JOIN (SELECT promo_id, effective_start_datetime FROM stg.dim_promo_history WHERE is_current) AS dcdh
        ON dcdh.promo_id = base.promo_id
ORDER BY base.promo_id;
-- SELECT * FROM _dim_promo_history__promo_base;

-- Action Dates (base table combining effective_start_dates of all hist tables)
CREATE OR REPLACE TEMP TABLE _dim_promo_history__action_datetime AS
SELECT DISTINCT
    promo_id,
    effective_start_datetime AS action_datetime
FROM (
    SELECT
        base.promo_id,
        p.effective_start_datetime
    FROM _dim_promo_history__promo_base AS base
        JOIN lake_consolidated.ultra_merchant_history.promo AS p
            ON p.promo_id = base.promo_id
    WHERE NOT p.effective_end_datetime < base.current_effective_start_datetime
    UNION ALL
    SELECT
        base.promo_id,
        d.effective_start_datetime
    FROM _dim_promo_history__promo_base AS base
        JOIN lake_consolidated.ultra_merchant_history.promo AS p
            ON p.promo_id = base.promo_id
        JOIN lake_consolidated.ultra_merchant_history.discount AS d
            ON d.discount_id IN (p.subtotal_discount_id, p.shipping_discount_id)
    WHERE NOT d.effective_end_datetime < base.current_effective_start_datetime
    UNION ALL
        SELECT
        base.promo_id,
        upmp.effective_start_datetime
    FROM _dim_promo_history__promo_base AS base
        JOIN lake_consolidated.ultra_merchant_history.promo AS p
            ON p.promo_id = base.promo_id
        JOIN lake_consolidated.ultra_cms_history.ui_promo_management_promos AS upmp
            ON CAST(upmp.ui_promo_management_promo_id AS VARCHAR(100)) = CONCAT(split_part(p.code, '_', -1), p.meta_company_id)
    WHERE NOT upmp.effective_end_datetime < base.current_effective_start_datetime
    AND p.code ILIKE 'REV\\_%%' ESCAPE '\\'
    UNION ALL
    SELECT
        base.promo_id,
        upmp.effective_start_datetime
    FROM _dim_promo_history__promo_base AS base
        JOIN lake_consolidated.ultra_merchant_history.promo AS p
            ON p.promo_id = base.promo_id
        JOIN lake_consolidated.ultra_cms_history.ui_promo_management_promos AS upmp
            ON upmp.promo_id = p.promo_id
    WHERE NOT upmp.effective_end_datetime < base.current_effective_start_datetime
    AND p.code NOT ILIKE 'REV\\_%%' ESCAPE '\\'
    ) AS dt;
-- SELECT * FROM _dim_promo_history__action_datetime;

CREATE OR REPLACE TEMP TABLE _dim_promo_history__ranked_classification AS
SELECT
    p.promo_id,
    p.promo_classification
FROM (
    SELECT
        ppc.promo_id,
        pc.label AS promo_classification,
        CASE LOWER(pc.label)
            WHEN 'vip conversion' THEN 1
            WHEN 'free trial' THEN 2
            WHEN 'sales promo' THEN 3
            WHEN 'upsell' THEN 4
            ELSE 5
        END AS sort_order,
        ROW_NUMBER() OVER (PARTITION BY ppc.promo_id ORDER BY sort_order) AS rank
    FROM _dim_promo_history__promo_base AS base
        JOIN lake_consolidated.ultra_merchant.promo_promo_classfication AS ppc
            ON ppc.promo_id = base.promo_id
        JOIN lake_consolidated.ultra_merchant.promo_classification AS pc
            ON pc.promo_classification_id = ppc.promo_classification_id
    QUALIFY rank = 1
    ) AS p;

CREATE OR REPLACE TEMP TABLE _dim_promo_history__first_promo_code AS
SELECT
    p.promo_id,
    p.code AS first_promo_code
FROM _dim_promo_history__promo_base AS base
    JOIN lake_consolidated.ultra_merchant_history.promo AS p
        ON p.promo_id = base.promo_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY p.promo_id ORDER BY p.effective_start_datetime) = 1;

CREATE OR REPLACE TEMP TABLE _dim_promo_history__cms_attributes_base AS
SELECT
    base.promo_id,
    p.promo_name as promo_name_cms,
    parse_json(p.target_user_json):PROMOTARGET_MEMBERCHOICE::STRING AS membership_choice,
    parse_json(p.target_user_json):PROMOTARGET_REGDATEOPTIONS::STRING AS registration_date_option,
    parse_json(p.target_user_json):PROMOTARGET_REGEXACTDATE::STRING AS registration_exact_date,
    parse_json(p.target_user_json):PROMOTARGET_REGSTARTDATE::STRING AS registration_start_date,
    parse_json(p.target_user_json):PROMOTARGET_REGENDDATE::STRING registration_end_date,
    parse_json(p.target_user_json):PROMOTARGET_REGDAYS::STRING AS registration_day,
    parse_json(p.target_user_json):PROMOTARGET_REGDAYSTART::STRING AS registration_day_start,
    parse_json(p.target_user_json):PROMOTARGET_REGDAYEND::STRING AS registration_day_end,
    parse_json(p.target_user_json):PROMOTARGET_GENDER::STRING AS gender_selected,
    replace(parse_json(p.target_user_json):promotarget_userlistid::string, '"', '') as userlistid,
    replace(parse_json(p.target_user_json):promotarget_sailthruid::string, '"', '') as sailthruid,
    replace(parse_json(p.target_user_json):promotarget_emarsysid::string, '"', '') as emarsysid,
    replace(parse_json(p.target_user_json):promotarget_signup_dm_gateway_id::string, '"', '') as signup_dm_gateway_id,
    replace(parse_json(p.target_user_json):promotarget_current_dm_gateway_id::string, '"', '') as current_dm_gateway_id,
    replace(parse_json(p.target_user_json):promotarget_customerdetailkey::string, '"', '')  as customerdetailkey,
    replace(parse_json(p.target_user_json):promotarget_customerdetailvalue::string, '"', '') as customerdetailvalue,
    NULL AS membership_reward_multiplier,
    p.effective_start_datetime,
    p.effective_end_datetime
FROM _dim_promo_history__action_datetime AS base
    JOIN lake_consolidated.ultra_merchant_history.promo AS ph
        ON ph.promo_id = base.promo_id
        AND base.action_datetime BETWEEN ph.effective_start_datetime AND ph.effective_end_datetime
    JOIN lake_consolidated.ultra_cms_history.ui_promo_management_promos AS p
        ON cast(p.ui_promo_management_promo_id as varchar(100)) = CONCAT(split_part(ph.code, '_', -1), ph.meta_company_id)
        AND base.action_datetime BETWEEN p.effective_start_datetime AND p.effective_end_datetime
WHERE ph.code ILIKE 'REV\\_%%' ESCAPE '\\'

UNION ALL

SELECT
    base.promo_id,
    p.promo_name as promo_name_cms,
    parse_json(p.target_user_json):PROMOTARGET_MEMBERCHOICE::STRING AS membership_choice,
    parse_json(p.target_user_json):PROMOTARGET_REGDATEOPTIONS::STRING AS registration_date_option,
    parse_json(p.target_user_json):PROMOTARGET_REGEXACTDATE::STRING AS registration_exact_date,
    parse_json(p.target_user_json):PROMOTARGET_REGSTARTDATE::STRING AS registration_start_date,
    parse_json(p.target_user_json):PROMOTARGET_REGENDDATE::STRING registration_end_date,
    parse_json(p.target_user_json):PROMOTARGET_REGDAYS::STRING AS registration_day,
    parse_json(p.target_user_json):PROMOTARGET_REGDAYSTART::STRING AS registration_day_start,
    parse_json(p.target_user_json):PROMOTARGET_REGDAYEND::STRING AS registration_day_end,
    parse_json(p.target_user_json):PROMOTARGET_GENDER::STRING AS gender_selected,
    replace(parse_json(p.target_user_json):promotarget_userlistid::string, '"', '') as userlistid,
    replace(parse_json(p.target_user_json):promotarget_sailthruid::string, '"', '') as sailthruid,
    replace(parse_json(p.target_user_json):promotarget_emarsysid::string, '"', '') as emarsysid,
    replace(parse_json(p.target_user_json):promotarget_signup_dm_gateway_id::string, '"', '') as signup_dm_gateway_id,
    replace(parse_json(p.target_user_json):promotarget_current_dm_gateway_id::string, '"', '') as current_dm_gateway_id,
    replace(parse_json(p.target_user_json):promotarget_customerdetailkey::string, '"', '')  as customerdetailkey,
    replace(parse_json(p.target_user_json):promotarget_customerdetailvalue::string, '"', '') as customerdetailvalue,
    NULL AS membership_reward_multiplier,
    p.effective_start_datetime,
    p.effective_end_datetime
FROM _dim_promo_history__action_datetime AS base
    JOIN lake_consolidated.ultra_merchant_history.promo AS ph
        ON ph.promo_id = base.promo_id
        AND base.action_datetime BETWEEN ph.effective_start_datetime AND ph.effective_end_datetime
    JOIN lake_consolidated.ultra_cms_history.ui_promo_management_promos AS p
        ON p.promo_id = ph.promo_id
        AND base.action_datetime BETWEEN p.effective_start_datetime AND p.effective_end_datetime
WHERE ph.code NOT ILIKE 'REV\\_%%' ESCAPE '\\';

CREATE OR REPLACE TEMP TABLE _dim_promo_history__cms_attributes AS
SELECT
    p.promo_id,
    p.promo_name_cms,
    CASE
        WHEN replace(membership_choice, '"', '') = '' THEN NULL
        ELSE membership_choice
    END AS membership_choice,
    CASE
        WHEN replace(registration_date_option, '"', '') = '' THEN NULL
        ELSE registration_date_option
    END AS registration_date_option,
    CASE
        WHEN replace(registration_exact_date, '"', '') = '' THEN NULL
        ELSE registration_exact_date
    END AS registration_exact_date,
    CASE
        WHEN replace(registration_start_date, '"', '') = '' THEN NULL
        ELSE registration_start_date
    END AS registration_start_date,
    CASE
        WHEN replace(registration_end_date, '"', '') = '' THEN NULL
        ELSE registration_end_date
    END AS registration_end_date,
    CASE
        WHEN replace(registration_day, '"', '') = '' THEN NULL
        ELSE registration_day
    END AS registration_day,
    CASE
        WHEN replace(registration_day_start, '"', '') = '' THEN NULL
        ELSE registration_day_start
    END AS registration_day_start,
    CASE
        WHEN replace(registration_day_end, '"', '') = '' THEN NULL
        ELSE registration_day_end
    END AS registration_day_end,
    CASE
        WHEN replace(gender_selected, '"', '') = '' THEN NULL
        ELSE gender_selected
    END AS gender_selected,
    coalesce(
            CASE
                WHEN userlistid = '' THEN NULL
                ELSE 'user_list' END,
            CASE
                WHEN sailthruid = '' THEN NULL
                ELSE sailthruid END,
            CASE
                WHEN emarsysid = '' THEN NULL
                ELSE emarsysid END,
            CASE
                WHEN signup_dm_gateway_id = '' THEN NULL
                ELSE signup_dm_gateway_id END,
            CASE
                WHEN current_dm_gateway_id = '' THEN NULL
                ELSE current_dm_gateway_id END,
            CASE
                WHEN customerdetailkey = '' THEN NULL
                ELSE customerdetailkey END
        ) AS user_target_l1,
        coalesce(
            CASE
                WHEN current_dm_gateway_id = '' THEN NULL
                ELSE current_dm_gateway_id END,
            CASE
                WHEN customerdetailvalue = '' THEN NULL
                ELSE customerdetailvalue END
        ) AS user_target_l2,
    NULL AS membership_reward_multiplier,
    p.effective_start_datetime,
    p.effective_end_datetime
FROM _dim_promo_history__cms_attributes_base p;

CREATE OR REPLACE TEMP TABLE _dim_promo_history__data AS
SELECT
    p.promo_id,
    p.meta_original_promo_id,
    COALESCE(p.label, 'Unknown') AS promo_name,
    COALESCE(fpc.first_promo_code, p.code, 'Unknown') AS first_promo_code,
    COALESCE(p.code, 'Unknown') AS promo_code,
    COALESCE(NULLIF(p.description,''), 'Unknown') AS promo_description,
    COALESCE(pt.label, 'Unknown')  AS promo_type,
    COALESCE(pt.cms_label, 'Unknown') AS promo_cms_type,
    COALESCE(pt.cms_description, 'Unknown') AS promo_cms_description,
    COALESCE(rc.promo_classification, 'Unknown') AS promo_classification,
    COALESCE(d_sub.calculation_method, 'Unknown') AS discount_method,
    COALESCE(d_sub.label, 'Unknown') AS subtotal_discount,
    COALESCE(d_shp.label, 'Unknown') AS shipping_discount,
    COALESCE(p.statuscode, -1) AS promo_status_code,
    COALESCE(sc.label, 'Unknown')  AS promo_status,
    COALESCE(p.date_start, '1900-01-01') AS promo_start_datetime,
    COALESCE(p.date_end, '1900-01-01') AS promo_end_datetime,
    CASE WHEN p.hvr_change_op = 0 THEN TRUE ELSE FALSE END AS is_deleted,
    COALESCE(p.store_domain_type_id_list, 'Unknown') AS promo_store_applied,
    COALESCE(p.required_product_quantity, -1) AS required_product_quantity,
    COALESCE(p.min_product_quantity, -1) AS min_product_quantity,
    COALESCE(fpli.label, 'Unknown') AS  fpl_included,
    COALESCE(fple.label, 'Unknown') AS  fpl_excluded,
    COALESCE(fplr.label, 'Unknown') AS  fpl_required,
    COALESCE(pci.label, 'Unknown') AS  category_included,
    COALESCE(pce.label, 'Unknown') AS  category_excluded,
    COALESCE(pcr.label, 'Unknown') AS  category_required,
    COALESCE(cms.promo_name_cms, 'Unknown') AS promo_name_cms,
    COALESCE(cms.membership_choice, 'Unknown') AS membership_choice,
    cms.registration_date_option,
    cms.registration_exact_date,
    cms.registration_start_date,
    cms.registration_end_date,
    cms.registration_day,
    cms.registration_day_start,
    cms.registration_day_end,
    cms.gender_selected,
    cms.membership_reward_multiplier,
    base.action_datetime AS meta_event_datetime
FROM _dim_promo_history__action_datetime AS base
    JOIN lake_consolidated.ultra_merchant_history.promo AS p
        ON p.promo_id = base.promo_id
        AND base.action_datetime BETWEEN p.effective_start_datetime AND p.effective_end_datetime
    LEFT JOIN lake_consolidated.ultra_merchant_history.discount AS d_sub
        ON d_sub.discount_id = p.subtotal_discount_id
        AND base.action_datetime BETWEEN d_sub.effective_start_datetime AND d_sub.effective_end_datetime
    LEFT JOIN lake_consolidated.ultra_merchant_history.discount AS d_shp
        ON d_shp.discount_id = p.shipping_discount_id
        AND base.action_datetime BETWEEN d_shp.effective_start_datetime AND d_shp.effective_end_datetime
    LEFT JOIN _dim_promo_history__ranked_classification AS rc
        ON rc.promo_id = p.promo_id
    LEFT JOIN lake_consolidated.ultra_merchant.promo_type AS pt
        ON pt.promo_type_id = p.promo_type_id
    LEFT JOIN lake_consolidated.ultra_merchant.statuscode AS sc
        ON sc.statuscode = p.statuscode
    LEFT JOIN _dim_promo_history__first_promo_code AS fpc
        ON fpc.promo_id = base.promo_id
    LEFT JOIN lake_consolidated.ultra_merchant.featured_product_location as fpli
        ON p.featured_product_location_id = fpli.featured_product_location_id
    LEFT JOIN lake_consolidated.ultra_merchant.featured_product_location as fple
        ON p.filtered_featured_product_location_id = fple.featured_product_location_id
    LEFT JOIN lake_consolidated.ultra_merchant.featured_product_location as fplr
        ON p.required_featured_product_location_id = fplr.featured_product_location_id
    LEFT JOIN lake_consolidated.ultra_merchant.product_category as pci
        ON pci.product_category_id = p.product_category_id
    LEFT JOIN lake_consolidated.ultra_merchant.product_category as pce
        ON pce.product_category_id = p.filtered_product_category_id
    LEFT JOIN lake_consolidated.ultra_merchant.product_category as pcr
        ON pcr.product_category_id = p.required_product_category_id
    LEFT JOIN _dim_promo_history__cms_attributes AS cms
        ON cms.promo_id = base.promo_id
        AND base.action_datetime BETWEEN cms.effective_start_datetime AND cms.effective_end_datetime;
-- SELECT * FROM _dim_promo_history__data;
-- SELECT promo_id, meta_event_datetime, COUNT(1) FROM _dim_promo_history__data GROUP BY 1, 2 HAVING COUNT(1) > 1;

CREATE OR REPLACE TEMP TABLE _dim_promo_history__stg AS
SELECT
    promo_id,
    meta_original_promo_id,
    promo_name,
    first_promo_code,
    promo_code,
    promo_description,
    promo_type,
    promo_cms_type,
    promo_cms_description,
    promo_classification,
    discount_method,
    subtotal_discount,
    shipping_discount,
    promo_status_code,
    promo_status,
    promo_start_datetime,
    promo_end_datetime,
    is_deleted,
    meta_event_datetime,
    promo_store_applied,
    required_product_quantity,
    min_product_quantity,
    fpl_included,
    fpl_excluded,
    fpl_required,
    category_included,
    category_excluded,
    category_required,
    promo_name_cms,
    membership_choice,
    registration_date_option,
    registration_exact_date,
    registration_start_date,
    registration_end_date,
    registration_day,
    registration_day_start,
    registration_day_end,
    gender_selected,
    membership_reward_multiplier,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM (
    SELECT
        promo_id,
        meta_original_promo_id,
        promo_name,
        first_promo_code,
        promo_code,
        promo_description,
        promo_type,
        promo_cms_type,
        promo_cms_description,
        promo_classification,
        discount_method,
        subtotal_discount,
        shipping_discount,
        promo_status_code,
        promo_status,
        promo_start_datetime,
        promo_end_datetime,
        is_deleted,
        HASH (
            promo_id,
            meta_original_promo_id,
            promo_name,
            first_promo_code,
            promo_code,
            promo_description,
            promo_type,
            promo_cms_type,
            promo_cms_description,
            promo_classification,
            discount_method,
            subtotal_discount,
            shipping_discount,
            promo_status_code,
            promo_status,
            promo_start_datetime,
            promo_end_datetime,
            is_deleted,
            promo_store_applied,
            required_product_quantity,
            min_product_quantity,
            fpl_included,
            fpl_excluded,
            fpl_required,
            category_included,
            category_excluded,
            category_required,
            promo_name_cms,
            membership_choice,
            registration_date_option,
            registration_exact_date,
            registration_start_date,
            registration_end_date,
            registration_day,
            registration_day_start,
            registration_day_end,
            gender_selected,
            membership_reward_multiplier
            ) AS meta_row_hash,
        LAG(meta_row_hash) OVER (PARTITION BY promo_id ORDER BY meta_event_datetime) AS prev_meta_row_hash,
        meta_event_datetime,
        promo_store_applied,
        required_product_quantity,
        min_product_quantity,
        fpl_included,
        fpl_excluded,
        fpl_required,
        category_included,
        category_excluded,
        category_required,
        promo_name_cms,
        membership_choice,
        registration_date_option,
        registration_exact_date,
        registration_start_date,
        registration_end_date,
        registration_day,
        registration_day_start,
        registration_day_end,
        gender_selected,
        membership_reward_multiplier
    FROM _dim_promo_history__data
    ) AS data
WHERE NOT EQUAL_NULL(prev_meta_row_hash, meta_row_hash)
QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_id, meta_event_datetime ORDER BY promo_name_cms DESC) = 1;

-- SELECT * FROM _dim_promo_history__stg WHERE promo_id = 2684;
-- SELECT * FROM _dim_promo_history__action_datetime WHERE promo_id = 2684;
-- SELECT * FROM _dim_promo_history__data WHERE promo_id = 2684;
-- SELECT COUNT(1) FROM _dim_promo_history__stg;
-- SELECT promo_id, meta_event_datetime, COUNT(1) FROM _dim_promo_history__stg GROUP BY 1, 2 HAVING COUNT(1) > 1;

INSERT INTO stg.dim_promo_history_stg (
    promo_id,
    meta_original_promo_id,
    promo_name,
    first_promo_code,
    promo_code,
    promo_description,
    promo_type,
    promo_cms_type,
    promo_cms_description,
    promo_classification,
    discount_method,
    subtotal_discount,
    shipping_discount,
    promo_status_code,
    promo_status,
    promo_start_datetime,
    promo_end_datetime,
    is_deleted,
    meta_event_datetime,
    promo_store_applied,
    required_product_quantity,
    min_product_quantity,
    fpl_included,
    fpl_excluded,
    fpl_required,
    category_included,
    category_excluded,
    category_required,
    promo_name_cms,
    membership_choice,
    registration_date_option,
    registration_exact_date,
    registration_start_date,
    registration_end_date,
    registration_day,
    registration_day_start,
    registration_day_end,
    gender_selected,
    membership_reward_multiplier,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    promo_id,
    meta_original_promo_id,
    promo_name,
    first_promo_code,
    promo_code,
    promo_description,
    promo_type,
    promo_cms_type,
    promo_cms_description,
    promo_classification,
    discount_method,
    subtotal_discount,
    shipping_discount,
    promo_status_code,
    promo_status,
    promo_start_datetime,
    promo_end_datetime,
    is_deleted,
    meta_event_datetime,
    promo_store_applied,
    required_product_quantity,
    min_product_quantity,
    fpl_included,
    fpl_excluded,
    fpl_required,
    category_included,
    category_excluded,
    category_required,
    promo_name_cms,
    membership_choice,
    registration_date_option,
    registration_exact_date,
    registration_start_date,
    registration_end_date,
    registration_day,
    registration_day_start,
    registration_day_end,
    gender_selected,
    membership_reward_multiplier,
    meta_create_datetime,
    meta_update_datetime
FROM _dim_promo_history__stg
ORDER BY
    promo_id,
    meta_event_datetime;

-- Whenever a full refresh (using '1900-01-01') is performed, we will truncate the existing table.  This is
-- because the Snowflake SCD operator cannot process historical data prior to the current row.  We truncate
-- at the end of the transform to prevent the table from being empty longer than necessary during processing.
DELETE
FROM stg.dim_promo_history
WHERE $is_full_refresh = TRUE
  AND promo_history_key <> - 1;

/*
-- HOW-TO: Backfill a new column in a type-2 history table
UPDATE stg.dim_promo_history AS tgt
SET tgt.first_promo_code = fpc.first_promo_code
FROM (
    SELECT
        p.promo_id,
        p.code AS first_promo_code
    FROM lake_consolidated.ultra_merchant_history.promo AS p
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.promo_id ORDER BY p.effective_start_datetime) = 1
    ) AS fpc
WHERE tgt.promo_id = fpc.promo_id
    AND tgt.first_promo_code IS NULL;
*/
