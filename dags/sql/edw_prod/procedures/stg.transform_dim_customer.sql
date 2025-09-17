SET target_table = 'stg.dim_customer';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

-- Switch warehouse if performing a full refresh
SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', current_warehouse());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;
*/

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL)); /* Use NULL for self-table */
SET wm_lake_ultra_merchant_customer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer'));
SET wm_lake_ultra_merchant_fraud_customer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.fraud_customer'));
SET wm_lake_ultra_merchant_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant."ORDER"'));
SET wm_lake_ultra_merchant_order_classification = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_classification'));
SET wm_lake_ultra_merchant_order_discount = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_discount'));
SET wm_lake_ultra_merchant_membership = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership'));
SET wm_lake_ultra_merchant_membership_level = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_level'));
SET wm_lake_ultra_merchant_customer_detail = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer_detail'));
SET wm_lake_ultra_merchant_customer_referrer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer_referrer'));
SET wm_lake_ultra_merchant_hdyh_global_list_referrer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.hdyh_global_list_referrer'));
SET wm_lake_ultra_merchant_hdyh_global_list = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.hdyh_global_list'));
SET wm_lake_builder_hdyh_flattened = (SELECT stg.udf_get_watermark($target_table, 'lake.builder.hdyh_flattened'));
SET wm_lake_ultra_merchant_session = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.session'));
SET wm_lake_ultra_merchant_membership_signup = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_signup'));
SET wm_lake_ultra_merchant_membership_brand_signup = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_brand_signup'));
SET wm_lake_ultra_merchant_membership_plan_membership_brand = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_plan_membership_brand'));
SET wm_lake_ultra_merchant_membership_trial = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_trial'));
SET wm_lake_ultra_merchant_gift_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.gift_order'));
SET wm_reporting_base_staging_site_visit_metrics = (SELECT stg.udf_get_watermark($target_table, 'reporting_base_prod.staging.site_visit_metrics'));
SET wm_reporting_base_shared_mobile_app_session_os_updated = (SELECT stg.udf_get_watermark($target_table, 'reporting_base_prod.shared.mobile_app_session_os_updated'));
SET wm_edw_stg_lkp_membership_state= (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.lkp_membership_state'));
SET wm_edw_stg_dim_address = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_address'));
SET wm_edw_stg_dim_customer_detail_history = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_customer_detail_history'));
SET wm_edw_reference_test_customer = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.test_customer'));
SET wm_lake_ultra_merchant_history_customer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.customer'));
SET wm_edw_stg_lkp_order_classification = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.lkp_order_classification'));

/*
SELECT
    $wm_self,
    $wm_lake_ultra_merchant_customer,
    $wm_lake_ultra_merchant_fraud_customer,
    $wm_lake_ultra_merchant_order,
    $wm_lake_ultra_merchant_order_classification,
    $wm_lake_ultra_merchant_order_discount,
    $wm_lake_ultra_merchant_membership,
    $wm_lake_ultra_merchant_membership_level,
    $wm_lake_ultra_merchant_customer_detail,
    $wm_lake_ultra_merchant_session,
    $wm_lake_ultra_merchant_membership_signup,
    $wm_lake_ultra_merchant_membership_brand_signup,
    $wm_lake_ultra_merchant_membership_plan_membership_brand,
    $wm_lake_ultra_merchant_membership_trial,
    $wm_lake_ultra_merchant_gift_order,
    $wm_reporting_base_staging_site_visit_metrics,
    $wm_reporting_base_shared_mobile_app_session_os_updated,
    $wm_edw_stg_lkp_membership_state,
    $wm_edw_stg_dim_address,
    $wm_edw_stg_dim_customer_detail_history,
    $wm_edw_reference_test_customer
*/

CREATE OR REPLACE TEMP TABLE _dim_customer__customer_base (customer_id INT);

-- Full Refresh
INSERT INTO _dim_customer__customer_base (customer_id)
SELECT DISTINCT c.customer_id
FROM lake_consolidated.ultra_merchant.customer AS c
WHERE $is_full_refresh = TRUE  /* SET is_full_refresh = TRUE; */
AND customer_id <> 94980436730
ORDER BY c.customer_id;


CREATE OR REPLACE TEMP TABLE _customer_detail__customer_referrer AS
SELECT customer_id,
       TRY_TO_NUMERIC(value||meta_company_id) as customer_referrer_id,
       meta_update_datetime
FROM lake_consolidated.ultra_merchant.customer_detail
WHERE name = 'customer_referrer_id'
AND NOT $is_full_refresh;

-- Incremental Refresh
INSERT INTO _dim_customer__customer_base (customer_id)
SELECT DISTINCT incr.customer_id
FROM (
    -- Self-check for manual updates
    SELECT customer_id
    FROM stg.dim_customer
    WHERE meta_update_datetime > $wm_self

    UNION ALL

    SELECT customer_id
    FROM lake_consolidated.ultra_merchant_history.customer
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_history_customer
      AND effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

    UNION ALL

    SELECT c.customer_id
    FROM lake_consolidated.ultra_merchant.customer AS c
        LEFT JOIN lake_consolidated.ultra_merchant.fraud_customer AS fc
            ON fc.customer_id = c.customer_id
        LEFT JOIN stg.dim_address AS da
            ON da.address_id = c.default_address_id
        LEFT JOIN stg.dim_customer_detail_history AS dcdh
            ON dcdh.customer_id = c.customer_id
            AND dcdh.is_current
    WHERE c.meta_update_datetime > $wm_lake_ultra_merchant_customer
        OR fc.meta_update_datetime > $wm_lake_ultra_merchant_fraud_customer
        OR da.meta_update_datetime > $wm_edw_stg_dim_address
        OR dcdh.meta_update_datetime > $wm_edw_stg_dim_customer_detail_history

    UNION ALL

    SELECT o.customer_id
    FROM lake_consolidated.ultra_merchant."ORDER" AS o
    WHERE o.meta_update_datetime > $wm_lake_ultra_merchant_order

    UNION ALL

    SELECT o.customer_id
    FROM lake_consolidated.ultra_merchant."ORDER" AS o
        LEFT JOIN lake_consolidated.ultra_merchant.order_classification AS oc
            ON oc.order_id = o.order_id
        LEFT JOIN lake_consolidated.ultra_merchant.order_discount AS od
            ON od.order_id = o.order_id
    WHERE oc.meta_update_datetime > $wm_lake_ultra_merchant_order_classification
        OR od.meta_update_datetime > $wm_lake_ultra_merchant_order_discount

    UNION ALL

    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant.membership AS m
        LEFT JOIN lake_consolidated.ultra_merchant.membership_level AS ml
            ON ml.membership_level_id = m.membership_level_id
    WHERE m.meta_update_datetime > $wm_lake_ultra_merchant_membership
        OR ml.meta_update_datetime > $wm_lake_ultra_merchant_membership_level

    UNION ALL

    SELECT cd.customer_id
    FROM lake_consolidated.ultra_merchant.customer_detail AS cd
    WHERE cd.meta_update_datetime > $wm_lake_ultra_merchant_customer_detail

    UNION ALL

    SELECT DISTINCT cd.customer_id
    FROM _customer_detail__customer_referrer as cd
    JOIN lake_consolidated.ultra_merchant.customer_referrer as cr on cr.customer_referrer_id = cd.customer_referrer_id
        LEFT JOIN lake_consolidated.ultra_merchant.hdyh_global_list_referrer AS hglr
            ON hglr.customer_referrer_id = cr.customer_referrer_id
        LEFT JOIN lake_consolidated.ultra_merchant.hdyh_global_list AS hgl
            ON hgl.hdyh_global_list_id = hglr.hdyh_global_list_id
        LEFT JOIN lake.builder.hdyh_flattened AS hf
            ON lower(hf.sub_global_code) = lower(hgl.global_code)
    WHERE (cr.meta_update_datetime > $wm_lake_ultra_merchant_customer_referrer
            OR hglr.meta_update_datetime > $wm_lake_ultra_merchant_hdyh_global_list_referrer
            OR hgl.meta_update_datetime > $wm_lake_ultra_merchant_hdyh_global_list
            OR hf.meta_update_datetime > $wm_lake_builder_hdyh_flattened)

    UNION ALL

    SELECT s.customer_id
    FROM lake_consolidated.ultra_merchant.session AS s
    WHERE s.meta_update_datetime > $wm_lake_ultra_merchant_session

    UNION ALL

    SELECT ms.customer_id
    FROM lake_consolidated.ultra_merchant.membership_signup AS ms
    WHERE ms.meta_update_datetime > $wm_lake_ultra_merchant_membership_signup

    UNION ALL
    SELECT ms.customer_id
    FROM lake_consolidated.ultra_merchant.membership_signup AS ms
        LEFT JOIN reporting_base_prod.staging.site_visit_metrics AS v
            ON v.session_id = ms.session_id
    WHERE ms.customer_id IS NOT NULL
        AND v.is_lead_registration_action = TRUE
        AND v.meta_update_datetime > $wm_reporting_base_staging_site_visit_metrics

    UNION ALL

    SELECT ms.customer_id
    FROM lake_consolidated.ultra_merchant.membership AS ms
        JOIN lake_consolidated.ultra_merchant.membership_brand_signup AS mbs
            ON mbs.membership_id = ms.membership_id
        JOIN lake_consolidated.ultra_merchant.membership_plan_membership_brand AS mpmb
            ON mpmb.membership_brand_id = mbs.membership_brand_id
    WHERE mbs.meta_update_datetime > $wm_lake_ultra_merchant_membership_brand_signup
        OR mpmb.meta_update_datetime > $wm_lake_ultra_merchant_membership_plan_membership_brand

    UNION ALL

    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant.gift_order AS gfto
        LEFT JOIN lake_consolidated.ultra_merchant.membership_trial AS mt
            ON mt.order_id = gfto.order_id
        LEFT JOIN lake_consolidated.ultra_merchant.membership AS m
            ON m.membership_id = mt.membership_id
    WHERE gfto.meta_update_datetime > $wm_lake_ultra_merchant_gift_order
        OR mt.meta_update_datetime > $wm_lake_ultra_merchant_membership_trial

    UNION ALL

    SELECT os.customer_id
    FROM reporting_base_prod.shared.mobile_app_session_os_updated AS os
    WHERE os.customer_id IS NOT NULL
        AND os.meta_update_datetime > $wm_reporting_base_shared_mobile_app_session_os_updated

    UNION ALL

    SELECT lme.customer_id
    FROM stg.lkp_membership_state AS lme
    WHERE lme.meta_update_datetime > $wm_edw_stg_lkp_membership_state

    UNION ALL

    SELECT tc.customer_id
    FROM reference.test_customer AS tc
    WHERE tc.meta_update_datetime > $wm_edw_reference_test_customer

    UNION ALL

    SELECT oc.customer_id
    FROM stg.lkp_order_classification oc
    WHERE meta_update_datetime > $wm_edw_stg_lkp_order_classification
    AND oc.is_activating = TRUE
	AND oc.is_guest = FALSE
    AND LOWER(oc.order_status) IN ('success', 'pending')
    AND LOWER(oc.order_classification_name) = 'product order'

    UNION ALL

    SELECT e.customer_id
    FROM excp.dim_customer AS e
    WHERE e.meta_is_current_excp
        AND e.meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
    AND incr.customer_id IS NOT NULL
    AND incr.customer_id <> 94980436730
ORDER BY incr.customer_id;

-- Switch warehouse if the delta is more than medium warehouse can handle
SET warehouse_to_be_used = (SELECT IFF(COUNT(1) > 75000000, 'da_wh_adhoc_large', CURRENT_WAREHOUSE()) FROM _dim_customer__customer_base);
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

CREATE OR REPLACE TEMP TABLE _dim_customer__test_customers AS
SELECT base.customer_id
FROM _dim_customer__customer_base AS base
    JOIN reference.test_customer AS test
        ON test.customer_id = base.customer_id;

CREATE OR REPLACE TEMP TABLE _dim_customer__membership_level AS
SELECT
    c.customer_id,
    ml.label AS membership_level
FROM _dim_customer__customer_base AS base
    JOIN lake_consolidated.ultra_merchant.customer AS c
        ON c.customer_id = base.customer_id
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.customer_id = c.customer_id
        AND m.store_id = c.store_id
    JOIN lake_consolidated.ultra_merchant.membership_level AS ml
        ON ml.membership_level_id = m.membership_level_id;

CREATE OR REPLACE TEMP TABLE _dim_customer__registration_type AS
SELECT
    ms.customer_id,
    CASE
        WHEN v.is_quiz_registration_action THEN 'Quiz'
        WHEN v.is_speedy_registration_action THEN 'Speedy Sign-Up'
        WHEN v.is_skip_quiz_registration_action THEN 'Skip Quiz'
        ELSE NULL END AS registration_type,
    ROW_NUMBER() OVER (PARTITION BY ms.customer_id ORDER BY ms.datetime_added DESC) AS row_num
FROM _dim_customer__customer_base AS base
    JOIN lake_consolidated.ultra_merchant.membership_signup AS ms
        ON ms.customer_id = base.customer_id
    JOIN reporting_base_prod.staging.site_visit_metrics AS v
        ON v.session_id = ms.session_id
WHERE v.is_lead_registration_action
    AND ms.customer_id IS NOT NULL
QUALIFY row_num = 1;

CREATE OR REPLACE TEMP TABLE _dim_customer__blacklist_reason AS
SELECT
	fc.customer_id,
	fcr.label AS blacklist_reason
FROM _dim_customer__customer_base AS base
    JOIN lake_consolidated.ultra_merchant.fraud_customer AS fc
        ON fc.customer_id = base.customer_id
    JOIN lake_consolidated.ultra_merchant.fraud_customer_reason AS fcr
        ON fcr.fraud_customer_reason_id = fc.fraud_customer_reason_id
WHERE fc.statuscode = 5233;


CREATE OR REPLACE TEMP TABLE _dim_customer__tmp_customer_details AS
SELECT
    cd.customer_detail_id,
    cd.customer_id,
    cd.name,
    cd.value,
    cd.datetime_modified,
    cd.datetime_added
FROM _dim_customer__customer_base AS base
    JOIN lake_consolidated.ultra_merchant.customer_detail AS cd
        ON cd.customer_id = base.customer_id
        AND cd.name IN ('Initial Medium', 'origin', 'funnel', 'onetrust_consent', 'birth_day', 'birth_year', 'birth_month', 'gender',
                        'customer_referrer_id', 'heard-us', 'shipping_zip', 'top-size','topSize','top_size', 'bottoms-size','bottom-size','bottom_size','bottomSize',
                        'bra-size','braSize','bra_size','denim-size', 'dress-size', 'shoe-size','shoeSize','shoe_size', 'country_code',
                        'bralette-size', 'lingerie-sleep-size', 'isScrubs', 'nmp_opt_in_true','onePieceSize','one-piece-size');
-- SELECT * FROM _dim_customer__tmp_customer_details;

CREATE OR REPLACE TEMP TABLE _dim_customer__customer_detail AS
SELECT
	a.customer_id,
	CASE
	    WHEN is_integer(try_to_numeric(a."'birth_day'"))=TRUE THEN REPLACE(a."'birth_day'",'.','')::INT
	    ELSE NULL END AS birth_day,
	CASE
	    WHEN is_integer(try_to_numeric(a."'birth_year'"))=TRUE THEN REPLACE(a."'birth_year'",'.','')::INT
	    ELSE NULL END AS birth_year,
	CASE
        WHEN is_integer(try_to_numeric(a."'birth_month'"))=TRUE THEN a."'birth_month'"::INT
        ELSE NULL END AS birth_month,
    a."'gender'" AS gender,
	CASE
        WHEN cr.label IS NOT NULL THEN cr.label
		WHEN LEN(a."'heard-us'") BETWEEN 1 AND 50 THEN a."'heard-us'"
		ELSE NULL END AS referrer,
    cr.customer_referrer_id,
	CASE
        WHEN LEN(a."'shipping_zip'") BETWEEN 1 AND 15 THEN a."'shipping_zip'"
        ELSE NULL END AS shipping_zip,
	COALESCE(a."'top-size'",a."'topSize'",a."'top_size'") AS top_size,
	COALESCE(a."'bottoms-size'",a."'bottom-size'",a."'bottom_size'",a."'bottomSize'") AS bottom_size,
	COALESCE(a."'bra-size'",a."'braSize'",a."'bra_size'") AS bra_size,
    COALESCE(a."'onePieceSize'", a."'one-piece-size'") AS onepiece_size,
	CASE
        WHEN LEN(a."'denim-size'") BETWEEN 1 AND 3 THEN a."'denim-size'"
        ELSE NULL END AS denim_size,
	CASE
        WHEN LEN(a."'dress-size'") BETWEEN 1 AND 13 THEN a."'dress-size'"
        ELSE NULL END AS dress_size,
	    CASE WHEN COALESCE(LEN(a."'shoe-size'"),LEN(a."'shoeSize'"),LEN(a."'shoe_size'")) BETWEEN 1 AND 4 THEN a."'shoe-size'"
        ELSE NULL
    END AS shoe_size,
	CASE
        WHEN LEN(a."'country_code'") BETWEEN 1 AND 5 THEN UPPER(a."'country_code'")
        ELSE NULL END AS country_code,
	a."'bralette-size'" AS bralette_size,
	CASE
        WHEN s.alias = 'Savage X' THEN COALESCE(a."'bottoms-size'",a."'bottom-size'",a."'bottom_size'",a."'bottomSize'")
        ELSE NULL END AS undie_size,
    CASE
        WHEN s.alias = 'Savage X' THEN a."'lingerie-sleep-size'"
        ELSE NULL END AS lingerie_sleep_size,
    CASE
        WHEN a."'isScrubs'" = 1 THEN TRUE
        ELSE FALSE END AS is_scrubs_customer
FROM (
	SELECT
		customer_id,
		"'birth_day'",
		"'birth_year'",
        "'birth_month'",
        "'gender'",
        "'customer_referrer_id'",
        "'heard-us'",
        "'shipping_zip'",
        "'top-size'",
        "'topSize'",
        "'top_size'",
        "'bottoms-size'",
        "'bottom-size'",
        "'bottom_size'",
        "'bottomSize'",
        "'bra-size'",
        "'braSize'",
        "'bra_size'",
        "'denim-size'",
        "'dress-size'",
        "'shoe-size'",
        "'shoeSize'",
        "'shoe_size'",
        "'country_code'",
        "'bralette-size'",
        "'lingerie-sleep-size'",
        "'isScrubs'",
        "'nmp_opt_in_true'",
        "'onePieceSize'",
        "'one-piece-size'"
	FROM (
		SELECT
			customer_id,
			name,
			value
		FROM _dim_customer__tmp_customer_details
        WHERE name IN ('birth_day', 'birth_year', 'birth_month', 'gender','customer_referrer_id', 'heard-us', 'shipping_zip', 'top-size','topSize','top_size', 'bottoms-size','bottom-size','bottom_size','bottomSize',
                        'bra-size','braSize','bra_size','denim-size', 'dress-size', 'shoe-size','shoeSize','shoe_size', 'country_code',
                        'bralette-size', 'lingerie-sleep-size', 'isScrubs', 'nmp_opt_in_true','onePieceSize','one-piece-size')
	    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, name ORDER BY datetime_added DESC) = 1
	    ) AS src
	PIVOT (
		MAX(value)
		FOR name IN ('birth_day', 'birth_year', 'birth_month', 'gender','customer_referrer_id', 'heard-us', 'shipping_zip', 'top-size','topSize','top_size', 'bottoms-size','bottom-size','bottom_size','bottomSize',
                        'bra-size','braSize','bra_size','denim-size', 'dress-size', 'shoe-size','shoeSize','shoe_size', 'country_code',
                        'bralette-size', 'lingerie-sleep-size', 'isScrubs', 'nmp_opt_in_true','onePieceSize','one-piece-size')
	    ) AS pvt
    ) AS a
    JOIN lake_consolidated.ultra_merchant.customer AS c
        ON c.customer_id = a.customer_id
    LEFT JOIN lake_consolidated.ultra_merchant.customer_referrer AS cr
        ON cr.customer_referrer_id = TRY_TO_NUMERIC(concat(a."'customer_referrer_id'", c.meta_company_id))
    JOIN lake_consolidated.ultra_merchant.store AS s
        ON s.store_id = c.store_id;
-- SELECT * FROM _dim_customer__customer_detail;

CREATE OR REPLACE TEMP TABLE _dim_customer__hdyh_parent AS
SELECT DISTINCT cd.customer_id, hf.label AS hdyh_parent_value
FROM _dim_customer__customer_detail AS cd
    JOIN lake_consolidated.ultra_merchant.hdyh_global_list_referrer as hglr
        ON hglr.customer_referrer_id = cd.customer_referrer_id
    JOIN lake_consolidated.ultra_merchant.hdyh_global_list AS hgl
        ON hgl.hdyh_global_list_id = hglr.hdyh_global_list_id
    JOIN lake.builder.hdyh_flattened AS hf
        ON lower(hf.sub_global_code) = lower(hgl.global_code)
WHERE cd.customer_referrer_id IS NOT NULL
    AND hglr.hvr_is_deleted = FALSE
    AND hgl.hvr_is_deleted = FALSE;

CREATE OR REPLACE TEMP TABLE _dim_customer__first_gender AS
SELECT
    customer_id,
    value AS gender,
    datetime_added
FROM _dim_customer__tmp_customer_details
WHERE LOWER(name) = 'gender'
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY datetime_added ASC) = 1;
-- SELECT * FROM _dim_customer__first_gender;

UPDATE _dim_customer__customer_detail
SET birth_year = NULL
WHERE LEN(birth_year) > 4;

CREATE OR REPLACE TEMP TABLE _dim_customer__meta_nav_customer AS
SELECT DISTINCT
    cd.customer_id
FROM _dim_customer__tmp_customer_details AS cd
WHERE cd.name = 'Initial Medium'
    AND cd.value ILIKE '%%meta%%';

CREATE OR REPLACE TEMP TABLE _dim_customer__friend_referrals AS
SELECT DISTINCT
    m.customer_id
FROM _dim_customer__customer_base AS base
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.customer_id = base.customer_id
    JOIN lake_consolidated.ultra_merchant.membership_invitation AS mi
        ON mi.invitee_membership_id = m.membership_id
        AND mi.invitee_membership_id IS NOT NULL;

CREATE OR REPLACE TEMP TABLE _dim_customer__free_trial AS
SELECT DISTINCT
    o.customer_id,
    'promo'::VARCHAR(15) AS free_trial_type
FROM _dim_customer__customer_base AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.customer_id = base.customer_id
    JOIN lake_consolidated.ultra_merchant.order_discount AS od
        ON od.order_id = o.order_id
    JOIN lake_consolidated.ultra_merchant.promo_promo_classfication AS ppc
        ON ppc.promo_id = od.promo_id
    JOIN lake_consolidated.ultra_merchant.promo_classification AS pc
        ON pc.promo_classification_id = ppc.promo_classification_id
WHERE pc.label = 'Free Trial';

INSERT INTO _dim_customer__free_trial
SELECT DISTINCT m.customer_id,
      'gift order' AS free_trial_type
FROM _dim_customer__customer_base AS base
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.customer_id = base.customer_id
    JOIN lake_consolidated.ultra_merchant.membership_trial AS mt
        ON mt.membership_id = m.membership_id
    JOIN lake_consolidated.ultra_merchant.gift_order AS gfto
        ON gfto.order_id = mt.order_id
WHERE base.customer_id NOT IN (SELECT customer_id FROM _dim_customer__free_trial);

CREATE OR REPLACE TEMP TABLE _dim_customer__fk_free_trial_base AS
SELECT DISTINCT
    cd.customer_id,
    cd.datetime_added
FROM _dim_customer__tmp_customer_details AS cd
WHERE cd.value ILIKE '%%Free%%'
    AND IFF(datetime_added >= '2023-09-26', cd.name in ('origin', 'funnel'), cd.name = 'origin');

CREATE OR REPLACE TEMP TABLE _dim_customer__fk_free_trial AS
SELECT DISTINCT
    ft.customer_id
FROM _dim_customer__fk_free_trial_base AS ft
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.customer_id = ft.customer_id
    JOIN lake_consolidated.ultra_merchant.store AS s
        ON s.store_id = m.store_id
    WHERE s.label = 'FabKids'
        AND ABS(DATEDIFF('seconds', m.datetime_added, ft.datetime_added)) < 30;

CREATE OR REPLACE TEMP TABLE _dim_customer__country_code AS
WITH cc AS (
    SELECT
        cd.customer_id,
        MAX(cd.country_code) AS country_code
    FROM _dim_customer__customer_detail AS cd
    WHERE cd.country_code IS NOT NULL
    GROUP BY cd.customer_id
    )
SELECT
    cc.customer_id,
    COALESCE(ctz.alpha2_code, 'Unknown') AS country_code
FROM cc
    LEFT JOIN reference.country_timezone AS ctz
        ON (cc.country_code = ctz.alpha2_code
            OR cc.country_code = ctz.alpha3_code
            OR cc.country_code = ctz.numeric_code);

-- JFB CA to US migration  -- migration happened on 2023-10-27
CREATE OR REPLACE TEMP TABLE _dim_customer__jf_migrated_customers AS
SELECT DISTINCT cb.customer_id
FROM _dim_customer__customer_base cb
         JOIN lake_jfb.ultra_merchant.customer_migration_prior_values cm
              ON stg.udf_unconcat_brand(cb.customer_id) = cm.customer_id;


CREATE OR REPLACE TEMP TABLE _dim_customer__specialty_country AS
SELECT base.customer_id,
       CASE
           WHEN MAX(cd.country_code) = 'CA' AND MAX(m.store_id = 26)
                    AND
                MAX(c.datetime_added) >= '2023-10-28'
                 THEN 'CA'
           WHEN mc.customer_id IS NOT NULL THEN 'CA'
           WHEN MAX(cd.country_code) = 'AU' AND MAX(m.store_id) = 52 THEN 'AU'
           WHEN MAX(cd.country_code) = 'CA' AND MAX(m.store_id) IN (41, 46, 55, 121) THEN 'CA'
           WHEN MAX(cd.country_code) = 'AT' AND MAX(m.store_id) IN (36, 65) THEN 'AT'
           WHEN MAX(cd.country_code) = 'BE' AND MAX(m.store_id) IN (48, 59, 69, 73) THEN 'BE'
           ELSE NULL END   AS specialty_country_code,
       CASE
           WHEN MAX(cd.country_code) = 'CA' AND MAX(m.store_id = 26)
                    AND
                MAX(c.datetime_added) >= '2023-10-28'
--
                 THEN 'CA'
           WHEN mc.customer_id IS NOT NULL THEN 'CA'
           WHEN MAX(m.store_id) = 121 AND base.customer_id IN (50362353130, 87518041030) THEN 'BF'
           WHEN MAX(cd.country_code) = 'AU' AND MAX(m.store_id) = 52 THEN 'AU'
           WHEN MAX(cd.country_code) = 'CA' AND MAX(m.store_id) IN (41, 46,55, 121) THEN 'CA'
           WHEN MAX(cd.country_code) = 'AT' AND MAX(m.store_id) IN (36, 65) THEN 'AT'
           WHEN MAX(cd.country_code) = 'BE' AND MAX(m.store_id) IN (48, 59, 69, 73) THEN 'BE'
           ELSE 'None' END AS finance_specialty_store
FROM _dim_customer__customer_base AS base
         JOIN lake_consolidated.ultra_merchant.membership AS m
              ON m.customer_id = base.customer_id
         JOIN lake_consolidated.ultra_merchant.customer c
              ON c.customer_id = base.customer_id
         LEFT JOIN _dim_customer__customer_detail AS cd
                   ON cd.customer_id = base.customer_id
         LEFT JOIN _dim_customer__jf_migrated_customers mc
                   ON mc.customer_id = base.customer_id
GROUP BY base.customer_id, mc.customer_id;

CREATE OR REPLACE TEMP TABLE _dim_customer__mobile_app_sessions AS
SELECT
    s.customer_id,
    s.session_id,
    s.store_id,
    s.datetime_added AS session_hq_datetime,
    CAST(DATE_TRUNC('month', s.datetime_added) AS DATE) AS session_month_date,
    RANK() OVER (PARTITION BY s.customer_id ORDER BY s.session_id ASC) AS session_rank
FROM _dim_customer__customer_base AS base
    JOIN lake_consolidated.ultra_merchant.session AS s
        ON s.customer_id = base.customer_id
    JOIN stg.dim_store AS ds
        ON ds.store_id = s.store_id
WHERE ds.store_type = 'Mobile App';

CREATE OR REPLACE TEMP TABLE _dim_customer__mobile_app_orders AS
SELECT
    o.customer_id,
    o.session_id,
    o.order_id,
    o.store_id,
    IFF(oc.order_classification_id IS NOT NULL, o.date_placed, o.datetime_added) AS order_hq_datetime,
    CAST(DATE_TRUNC('month', IFF(oc.order_classification_id IS NOT NULL, o.date_placed, o.datetime_added)) AS DATE) AS order_month_date,
    RANK() OVER (PARTITION BY o.customer_id ORDER BY o.session_id ASC, o.order_id ASC) AS session_rank
FROM _dim_customer__customer_base AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.customer_id = base.customer_id
    LEFT JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = o.order_id
        AND oc.ORDER_TYPE_ID = 4 /* 'Historical' */
    JOIN stg.dim_store AS ds
        ON ds.store_id = o.store_id
WHERE ds.store_type = 'Mobile App';

CREATE OR REPLACE TEMP TABLE _dim_customer__first_mobile_app_data AS
SELECT
    base.customer_id,
    CONVERT_TIMEZONE(stz.time_zone, s.session_hq_datetime::TIMESTAMP_TZ) AS first_mobile_app_session_local_datetime,
    s.session_month_date AS first_mobile_app_session_hq_month_date,
    s.session_id AS first_mobile_app_session_id,
    convert_timezone(otz.time_zone, o.order_hq_datetime::TIMESTAMP_TZ) AS first_mobile_app_order_local_datetime,
    o.order_month_date AS first_mobile_app_order_hq_month_date,
    o.order_id AS first_mobile_app_order_id,
    app.os
FROM _dim_customer__customer_base AS base
    LEFT JOIN _dim_customer__mobile_app_sessions AS s
        ON s.customer_id = base.customer_id
        AND s.session_rank = 1
    LEFT JOIN reference.store_timezone AS stz
		ON stz.store_id = s.store_id
    LEFT JOIN _dim_customer__mobile_app_orders AS o
        ON o.customer_id = base.customer_id
        AND o.session_rank = 1
    LEFT JOIN reference.store_timezone AS otz
		ON otz.store_id = o.store_id
    LEFT JOIN reporting_base_prod.shared.mobile_app_session_os_updated AS app
        ON app.session_id = s.session_id
        AND app.customer_id = s.customer_id
        AND s.session_rank = 1;

CREATE OR REPLACE TEMP TABLE _dim_customer__onetrust_customer_detail AS
SELECT
    cd.customer_id,
    cd.value,
    cd.datetime_modified
FROM  _dim_customer__tmp_customer_details AS cd
WHERE LOWER(cd.name) = 'onetrust_consent'
QUALIFY ROW_NUMBER() OVER (PARTITION BY cd.customer_id ORDER BY cd.datetime_modified DESC, cd.value DESC) = 1;

--todo: Yigit to add a comment here regarding scrubs gateway issue
CREATE OR REPLACE TEMP TABLE _dim_customer__scrubs_registration_date AS
SELECT cd.customer_id,
       cd.datetime_added AS scrubs_registration_datetime
FROM _dim_customer__tmp_customer_details cd
WHERE LOWER(cd.name) = 'isscrubs'
  AND value = 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY cd.customer_id ORDER BY cd.datetime_added DESC) = 1;

-- customers with currently optin = false status. Their email optin field would be NULL
CREATE OR REPLACE TEMP TABLE _dim_customer__optout AS
SELECT base.customer_id,
       dcdh.is_opt_out
FROM _dim_customer__customer_base AS base
         JOIN stg.dim_customer_detail_history AS dcdh
              ON dcdh.customer_id = base.customer_id
                  AND dcdh.is_current
                  AND dcdh.is_opt_out = TRUE;

CREATE OR REPLACE TEMP TABLE _dim_customer__optin AS
SELECT dcdh.customer_id,
       dcdh.meta_event_datetime,
       dcdh.effective_start_datetime
FROM _dim_customer__customer_base b
         JOIN stg.dim_customer_detail_history AS dcdh
              ON dcdh.customer_id = b.customer_id
WHERE b.customer_id NOT IN (SELECT customer_id FROM _dim_customer__optout);


-- process customers that are currently optedin = true. Get minimum opt in field after an opt out(if there is one)
CREATE OR REPLACE TEMP TABLE _dim_customer__email_optin_date AS
WITH _optin_start_date AS
         (SELECT dcdh.customer_id,
                 DATEADD(MILLISECOND, 1, MAX(dcdh.effective_end_datetime)) AS latest_opt_out_end_datetime
          FROM stg.dim_customer_detail_history AS dcdh
                   JOIN _dim_customer__optin opt
                        ON dcdh.customer_id = opt.customer_id-- customers that are currently opt in
                            AND is_opt_out = TRUE
          GROUP BY dcdh.customer_id)

SELECT base.customer_id,
       COALESCE(MIN(latest_opt_out_end_datetime), MIN(base.meta_event_datetime)) AS opt_in_datetime
FROM _dim_customer__optin base
         LEFT JOIN _optin_start_date s
                   ON base.customer_id = s.customer_id
                       AND base.effective_start_datetime >= s.latest_opt_out_end_datetime
GROUP BY base.customer_id

UNION

SELECT customer_id,
       NULL AS opt_in_datetime
FROM _dim_customer__optout;

-- customers with currently sms_optin = false status. Their sms optin field would be NULL
CREATE OR REPLACE TEMP TABLE _dim_customer__sms_optout AS
SELECT base.customer_id,
       dcdh.is_sms_opt_out
FROM _dim_customer__customer_base AS base
         JOIN stg.dim_customer_detail_history AS dcdh
              ON dcdh.customer_id = base.customer_id
                  AND dcdh.is_current
                  AND dcdh.is_sms_opt_out = TRUE;


CREATE OR REPLACE TEMP TABLE _dim_customer__sms_optin AS
SELECT dcdh.customer_id,
       dcdh.meta_event_datetime,
       dcdh.effective_start_datetime
FROM _dim_customer__customer_base b
         JOIN stg.dim_customer_detail_history AS dcdh
              ON dcdh.customer_id = b.customer_id
WHERE b.customer_id NOT IN (SELECT customer_id FROM _dim_customer__optout);


-- process customers that are currently smsoptedout = False. Get minimum opt in field after an opt out(if there is one)
CREATE OR REPLACE TEMP TABLE _dim_customer__sms_optin_date AS
WITH _sms_optin_start_date AS
         (SELECT dcdh.customer_id,
                 DATEADD(MILLISECOND, 1, MAX(dcdh.effective_end_datetime)) AS latest_opt_out_end_datetime
          FROM stg.dim_customer_detail_history AS dcdh
                   JOIN _dim_customer__sms_optin opt
                        ON dcdh.customer_id = opt.customer_id-- customers that are currently opt in
                            AND is_sms_opt_out = FALSE
          GROUP BY dcdh.customer_id)

SELECT base.customer_id,
       COALESCE(MIN(latest_opt_out_end_datetime), MIN(base.meta_event_datetime)) AS opt_in_datetime
FROM _dim_customer__sms_optin base
         LEFT JOIN _sms_optin_start_date s
                   ON base.customer_id = s.customer_id
                       AND base.effective_start_datetime >= s.latest_opt_out_end_datetime
GROUP BY base.customer_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY base.customer_id ORDER BY opt_in_datetime DESC) = 1
UNION
SELECT customer_id,
       NULL AS sms_opt_in_datetime
FROM _dim_customer__sms_optout
WHERE customer_id NOT IN (SELECT customer_id FROM _dim_customer__sms_optin);

CREATE OR REPLACE TEMP TABLE _dim_customer__membership_events AS
SELECT
    lme.customer_id,
    lme.store_id,
    lme.membership_event_type_key,
    lme.membership_event_type,
    lme.membership_type_detail,
    lme.event_local_datetime,
    lme.membership_state,
    lme.is_ignored_activation,
    lme.is_ignored_cancellation,
    LAG(lme.membership_event_type) OVER (PARTITION BY lme.customer_id ORDER BY lme.event_local_datetime) AS prior_event,
    LEAD(lme.membership_event_type) OVER (PARTITION BY lme.customer_id ORDER BY lme.event_local_datetime) AS next_event,
    LAG(lme.membership_type_detail) OVER (PARTITION BY lme.customer_id ORDER BY lme.event_local_datetime) AS prior_event_type,
    LEAD(lme.membership_type_detail) OVER (PARTITION BY lme.customer_id ORDER BY lme.event_local_datetime) AS next_event_type,
    LAG(lme.event_local_datetime) OVER (PARTITION BY lme.customer_id ORDER BY lme.event_local_datetime) AS prior_event_local_datetime,
    LEAD(lme.event_local_datetime) OVER (PARTITION BY lme.customer_id ORDER BY lme.event_local_datetime) AS next_event_local_datetime
FROM _dim_customer__customer_base as cb
JOIN stg.lkp_membership_state AS lme ON lme.customer_id = cb.customer_id
WHERE lme.customer_id <> -1
    AND NOT NVL(lme.is_deleted, FALSE);

CREATE OR REPLACE TEMP TABLE _dim_customer__first_activation AS
SELECT
    me.customer_id,
    MIN(event_local_datetime) AS first_activation_local_datetime
FROM _dim_customer__membership_events AS me
WHERE membership_event_type = 'Activation'
AND NOT EQUAL_NULL(next_event,'Failed Activation')
GROUP BY me.customer_id;

CREATE OR REPLACE TEMP TABLE _dim_customer__activations AS
SELECT
    customer_id,
    store_id,
    is_ignored_activation,
    membership_event_type,
    next_event,
    DATEADD(SECOND,-900,event_local_datetime) as event_local_datetime, -- to mimic FA logic
    COALESCE(next_event_local_datetime,'9999-12-31') as next_event_local_datetime
FROM _dim_customer__membership_events
WHERE membership_event_type = 'Activation';

CREATE OR REPLACE TEMP TABLE _dim_customer__activating_success_orders AS
SELECT
	base.customer_id,
	oc.order_id,
    oc.session_id,
	oc.order_local_datetime,
	oc.is_activating
FROM stg.lkp_order_classification AS oc
    JOIN _dim_customer__activations AS base
        ON base.customer_id = oc.customer_id
WHERE oc.is_activating = TRUE
	AND oc.is_guest = FALSE
    AND LOWER(oc.order_status) IN ('success', 'pending')
    AND LOWER(oc.order_classification_name) = 'product order';


CREATE OR REPLACE TEMPORARY TABLE _cust__emp_flag AS
SELECT DISTINCT customer_id,
                'customer_detail_flag' AS reason,
                MAX(datetime_added)::DATE optin_date_max
FROM _dim_customer__tmp_customer_details
WHERE name = 'nmp_opt_in_true'
GROUP BY customer_id;

CREATE OR REPLACE TEMPORARY TABLE _cust__no_emp_flag AS
SELECT DISTINCT fa.customer_id,
                'activate_post_emp' AS                  reason,
                MIN(fa.event_local_datetime)::DATE activation_date_min
FROM _dim_customer__customer_base cb
         JOIN _dim_customer__activations fa
              ON cb.customer_id = fa.customer_id and not is_ignored_activation
         LEFT JOIN _dim_customer__activating_success_orders o
              ON o.customer_id = fa.customer_id and DATEDIFF(SECOND, o.order_local_datetime, fa.event_local_datetime) BETWEEN -900 AND 3600
         JOIN data_model.dim_store s ON fa.store_id = s.store_id
         LEFT JOIN _dim_customer__tmp_customer_details cd
                   ON fa.customer_id = cd.customer_id
                       AND cd.name = 'nmp_opt_in_true'
         LEFT JOIN lake_consolidated_view.ultra_merchant.membership m ON fa.customer_id = m.customer_id
         LEFT JOIN _dim_customer__jf_migrated_customers dcjm ON cb.customer_id = dcjm.customer_id
WHERE ((s.store_brand = 'Fabletics' AND s.store_region = 'NA' AND fa.event_local_datetime::DATE > '2021-01-01')
    OR (s.store_brand = 'Fabletics' AND s.store_region = 'EU' AND fa.event_local_datetime::DATE > '2021-11-01')
    OR (s.store_brand = 'Savage X' AND s.store_region = 'NA' AND fa.event_local_datetime::DATE > '2022-01-01')
    OR (s.store_brand = 'JustFab' AND s.store_country = 'US' AND dcjm.customer_id IS NULL AND fa.event_local_datetime::DATE > '2023-07-18') -- original JFB US customers
    OR (s.store_brand = 'JustFab' AND s.store_country = 'US' AND dcjm.customer_id IS NOT NULL AND fa.event_local_datetime::DATE >= '2023-10-26') -- JFB CA to US migrated customers
    OR (s.store_brand = 'ShoeDazzle' AND s.store_country = 'US' AND fa.event_local_datetime::DATE > '2023-09-21')
    OR (s.store_brand = 'FabKids' AND s.store_country = 'US' AND fa.event_local_datetime::DATE > '2023-09-26'))
    -- missing JFEU and SXEU time point
  AND cd.customer_id IS NULL
  AND NOT (next_event = 'Cancellation' AND o.order_id IS NULL AND
           next_event_local_datetime BETWEEN event_local_datetime AND DATEADD(DAY, 1, event_local_datetime))
GROUP BY fa.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _dim_customer__emp_optin_date AS
SELECT customer_id, reason, optin_date_max AS emp_optin_datetime
FROM _cust__emp_flag
UNION
SELECT customer_id, reason, activation_date_min AS emp_optin_datetime
FROM _cust__no_emp_flag;


CREATE OR REPLACE TEMP TABLE _dim_customer__membership_state AS
SELECT
    customer_id,
    membership_state
FROM _dim_customer__membership_events me
WHERE NOT is_ignored_activation AND NOT is_ignored_cancellation
QUALIFY ROW_NUMBER() OVER(PARTITION BY me.customer_id ORDER BY me.event_local_datetime DESC) = 1;

CREATE OR REPLACE TEMP TABLE _dim_customer__secondary_registrations AS
SELECT DISTINCT
    base.customer_id,
    mbs.datetime_signup,
    CASE WHEN IFNULL(fix.store_id, -1) = 241 THEN 52
        WHEN IFNULL(fix.store_id, -1) = 52 THEN 241
    ELSE mpmb.store_id END AS store_id
FROM _dim_customer__customer_base AS base
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.customer_id = base.customer_id
    JOIN lake_consolidated.ultra_merchant.membership_brand_signup AS mbs
        ON mbs.membership_id = m.membership_id
    LEFT JOIN _dim_customer__activations AS a
        ON a.customer_id = base.customer_id
        AND mbs.datetime_signup >= a.event_local_datetime
        AND mbs.datetime_signup < a.next_event_local_datetime
    LEFT JOIN lake_consolidated.ultra_merchant.membership_plan_membership_brand AS mpmb
        ON mpmb.membership_brand_id = mbs.membership_brand_id
    LEFT JOIN reference.yitty_fl_lead_fix_20240304 fix
        ON fix.customer_id = base.customer_id
WHERE mbs.membership_signup_id IS NULL AND a.customer_id IS NULL;

CREATE OR REPLACE TEMP TABLE _dim_customer__registration AS
SELECT
    base.customer_id,
    COALESCE(fix.store_id, mpmbp.store_id) AS primary_registration_store_id,
    CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(sr.datetime_signup::TIMESTAMP_NTZ, 'America/Los_Angeles')) AS secondary_registration_local_datetime,
    sr.store_id AS secondary_registration_store_id,
    IFF(fa.first_activation_local_datetime IS NOT NULL AND secondary_registration_local_datetime > fa.first_activation_local_datetime, TRUE, FALSE) AS is_secondary_registration_after_vip
FROM _dim_customer__customer_base AS base
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.customer_id = base.customer_id
    JOIN reference.store_timezone AS stz
        ON stz.store_id = m.store_id
    LEFT JOIN _dim_customer__first_activation AS fa
        ON fa.customer_id = base.customer_id
    JOIN lake_consolidated.ultra_merchant.membership_brand_signup AS mbsp /* primary registration */
        ON mbsp.membership_signup_id = m.membership_signup_id
    JOIN lake_consolidated.ultra_merchant.membership_plan_membership_brand AS mpmbp
        ON mpmbp.membership_brand_id = mbsp.membership_brand_id
    LEFT JOIN _dim_customer__secondary_registrations AS sr  /* secondary registration */
        ON sr.customer_id = base.customer_id
    LEFT JOIN reference.yitty_fl_lead_fix_20240304 fix
        ON fix.customer_id = base.customer_id;

-- There are a small handful of customer_ids that have >1 membership_id. So creating a temp table that only takes the most recently added record
CREATE OR REPLACE TEMP TABLE _lake_ultra_merchant_membership AS
SELECT
    m.customer_id,
    m.membership_id,
    m.store_id,
    m.price,
    m.datetime_added,
    m.membership_type_id,
    m.membership_plan_id,
    ROW_NUMBER()OVER(PARTITION BY m.customer_id ORDER BY m.datetime_added DESC) AS row_num
FROM _dim_customer__customer_base as cb
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.customer_id = cb.customer_id
QUALIFY row_num = 1;

CREATE OR REPLACE TEMP TABLE _dim_customer__stg AS
SELECT
	c.customer_id AS customer_id,
	c.meta_original_customer_id,
	COALESCE(iff(ascii(c.firstname) = 0, 'Unknown', LOWER(c.firstname)), 'Unknown') AS first_name,
	COALESCE(iff(ascii(c.lastname) = 0, 'Unknown', LOWER(c.lastname)), 'Unknown') AS last_name,
	COALESCE(LOWER(c.email), 'Unknown') AS email,
	COALESCE(IFF(c.store_id = 41, 26, c.store_id), -1) AS store_id,
	COALESCE(da.address_id, c.default_address_id, -1) AS default_address_id,
	COALESCE(da.street_address_1, 'Unknown') AS default_address1,
	COALESCE(da.street_address_2, 'Unknown') AS default_address2,
	COALESCE(da.city, 'Unknown') AS default_city,
	COALESCE(da.state, 'Unknown') AS default_state_province,
	COALESCE(da.zip_code, 'Unknown') AS default_postal_code,
	COALESCE(UPPER(da.country_code), 'Unknown') AS default_country_code,
    COALESCE(cc.country_code, 'Unknown') AS detail_country_code,
	COALESCE(sps.specialty_country_code, s.store_country) AS specialty_country_code,
    COALESCE(sps.finance_specialty_store, 'None') AS finance_specialty_store,
	COALESCE(cdl.birth_day, -1) AS birth_day,
	COALESCE(cdl.birth_year, -1) AS birth_year,
	COALESCE(cdl.birth_month, -1) AS birth_month,
	COALESCE(iff(ascii(cdl.shipping_zip) = 0, 'Unknown', UPPER(cdl.shipping_zip)), 'Unknown') AS quiz_zip,
	COALESCE(iff(ascii(cdl.top_size) = 0, '-1', cdl.top_size), '-1') AS top_size,
	COALESCE(iff(ascii(cdl.bottom_size) = 0, '-1', cdl.bottom_size), '-1') AS bottom_size,
	COALESCE(iff(ascii(cdl.bra_size) = 0, '-1', cdl.bra_size), '-1') AS bra_size,
    COALESCE(iff(ascii(cdl.onepiece_size) = 0, '-1', cdl.onepiece_size), '-1') AS onepiece_size,
	COALESCE(iff(ascii(cdl.denim_size) = 0, 'Unknown', upper(cdl.denim_size)), 'Unknown') AS denim_size,
	COALESCE(iff(ascii(cdl.dress_size) = 0, 'Unknown', upper(cdl.dress_size)), 'Unknown') AS dress_size,
	COALESCE(iff(ascii(cdl.shoe_size) = 0, 'Unknown', upper(cdl.shoe_size)), 'Unknown') AS shoe_size,
	CASE WHEN COALESCE(s.store_brand, '') NOT IN ('FabKids', 'Fabletics') THEN 'Unknown'
        WHEN s.store_brand = 'Fabletics' AND COALESCE(m.datetime_added, c.datetime_added) < '2020-01-01' THEN 'F'
        WHEN LOWER(LEFT(cdl.gender, 1)) = 'm' THEN 'M'
        ELSE 'F' END AS gender,
	CASE WHEN COALESCE(s.store_brand, '') NOT IN ('FabKids', 'Fabletics') THEN 'Unknown'
        WHEN s.store_brand = 'Fabletics' AND COALESCE(m.datetime_added, c.datetime_added) < '2020-01-01' THEN 'F'
        WHEN LOWER(LEFT(fg.gender, 1)) = 'm' THEN 'M'
        WHEN LOWER(LEFT(fg.gender, 1)) = 'f' THEN 'F'
        ELSE COALESCE(src.first_gender, 'Unknown') END AS first_gender,
	CASE WHEN fr.customer_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_friend_referral,
	COALESCE(ml.membership_level, 'Unknown') AS membership_level,
	COALESCE(hdyhp.hdyh_parent_value, cdl.referrer, 'Unknown') AS how_did_you_hear_parent,
	COALESCE(cdl.referrer, 'Unknown') AS how_did_you_hear,
	COALESCE(CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(m.datetime_added::TIMESTAMP_NTZ, 'America/Los_Angeles')),
	    CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(c.datetime_added::TIMESTAMP_NTZ, 'America/Los_Angeles')),
	    '9999-12-31') AS registration_local_datetime,
	cr.secondary_registration_local_datetime,
    IFF(COALESCE(cr.primary_registration_store_id,m.store_id,c.store_id) = 41, 26,
        COALESCE(cr.primary_registration_store_id,m.store_id,c.store_id)) AS primary_registration_store_id,
    cr.secondary_registration_store_id,
    cr.is_secondary_registration_after_vip,
	CASE WHEN br.customer_id IS NULL THEN FALSE ELSE TRUE END AS is_blacklisted,
	CASE WHEN br.customer_id IS NULL THEN 'N/A' ELSE COALESCE(br.blacklist_reason, 'Unknown') END AS blacklist_reason,
	CASE WHEN ft.customer_id IS NULL THEN FALSE ELSE TRUE END AS is_free_trial,
	CASE WHEN ft.customer_id IS NOT NULL AND ft.free_trial_type = 'promo' THEN TRUE ELSE FALSE END AS is_free_trial_promo,
	CASE WHEN ft.customer_id IS NOT NULL AND ft.free_trial_type = 'gift order' THEN TRUE ELSE FALSE END AS is_free_trial_gift_order,
	CASE WHEN fkft.customer_id IS NULL THEN FALSE ELSE TRUE END AS is_cross_promo,
	IFF(mnc.customer_id IS NOT NULL, TRUE, FALSE) AS is_metanav,
    CASE WHEN base.customer_id = 99704791520 THEN FALSE
        WHEN cdl.is_scrubs_customer = TRUE AND
        CAST(scrubs_registration_datetime AS DATE) = CAST(COALESCE(m.datetime_added, c.datetime_added) AS DATE)
    THEN TRUE ELSE FALSE END AS is_scrubs_customer,
	IFF(test.customer_id IS NOT NULL, TRUE, FALSE) AS is_test_customer,
    COALESCE(oo.is_opt_out, FALSE) AS is_opt_out,
    COALESCE(iff(ascii(cdl.bralette_size) = 0, 'Unknown', upper(cdl.bralette_size)), 'Unknown') AS bralette_size,
    COALESCE(iff(ascii(cdl.undie_size) = 0, 'Unknown', upper(cdl.undie_size)), 'Unknown') AS undie_size,
    COALESCE(iff(ascii(cdl.lingerie_sleep_size) = 0, 'Unknown', upper(cdl.lingerie_sleep_size)), 'Unknown') AS lingerie_sleep_size,
    CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(eod.emp_optin_datetime::timestamp_ntz, 'America/Los_Angeles')) AS emp_optin_datetime,
	fma.first_mobile_app_session_local_datetime,
    fma.first_mobile_app_session_id,
    fma.first_mobile_app_order_local_datetime,
    fma.first_mobile_app_order_id,
    CASE
        WHEN fma.first_mobile_app_order_hq_month_date <= fma.first_mobile_app_session_hq_month_date
            THEN fma.first_mobile_app_order_hq_month_date
        WHEN fma.first_mobile_app_session_hq_month_date IS NULL
            THEN fma.first_mobile_app_order_hq_month_date
        ELSE fma.first_mobile_app_session_hq_month_date
        END AS mobile_app_cohort_month_date,
    fma.os AS mobile_app_cohort_os,
	CONTAINS(ocd.value, 'C0001') AS onetrust_consent_strictlynecessary,
    CONTAINS(ocd.value, 'C0002') AS onetrust_consent_performance,
    CONTAINS(ocd.value, 'C0003') AS onetrust_consent_functional,
    CONTAINS(ocd.value, 'C0004') AS onetrust_consent_targeting,
    CONTAINS(ocd.value, 'C0005') AS onetrust_consent_social,
    ocd.datetime_modified AS onetrust_update_datetime,
    rt.registration_type,
    IFF(c.customer_id IS NULL, TRUE, FALSE) AS is_deleted, /* This is not the typical way we capture deletes, as there is no delete log for the customer table */
    COALESCE(lmt.label, 'Unknown') AS current_membership_type,
    m.price AS membership_price,
    m.membership_plan_id AS membership_plan_id,
    NVL(CONVERT_TIMEZONE(stz.time_zone,eodt.opt_in_datetime),NULL ) AS email_opt_in_datetime,
    NVL(CONVERT_TIMEZONE(stz.time_zone,sodt.opt_in_datetime), NULL )  AS sms_opt_in_datetime,
    dms.membership_state,
    IFF(c.datetime_added::DATE < '2023-07-27', (c.meta_original_customer_id % 20)+1,ROUND(((c.meta_original_customer_id % 80) / 4),0)+1) AS customer_bucket_group
FROM _dim_customer__customer_base AS base
    JOIN lake_consolidated.ultra_merchant.customer AS c
        ON c.customer_id = base.customer_id
    LEFT JOIN _dim_customer__test_customers AS test
        ON test.customer_id = c.customer_id
    LEFT JOIN stg.dim_address AS da
        ON da.address_id = c.default_address_id
    LEFT JOIN _dim_customer__blacklist_reason AS br
        ON br.customer_id = c.customer_id
    LEFT JOIN _dim_customer__customer_detail AS cdl
        ON cdl.customer_id = c.customer_id
    LEFT JOIN _dim_customer__first_gender AS fg
        ON fg.customer_id = c.customer_id
    LEFT JOIN _dim_customer__free_trial AS ft
        ON ft.customer_id = c.customer_id
    LEFT JOIN reference.store_timezone AS stz
        ON stz.store_id = c.store_id
    LEFT JOIN _dim_customer__fk_free_trial AS fkft
        ON fkft.customer_id = c.customer_id
    LEFT JOIN _dim_customer__membership_level AS ml
        ON ml.customer_id = c.customer_id
    LEFT JOIN _dim_customer__country_code AS cc
        ON cc.customer_id = c.customer_id
    LEFT JOIN _dim_customer__specialty_country AS sps
        ON sps.customer_id = c.customer_id
    LEFT JOIN _dim_customer__friend_referrals AS fr
        ON fr.customer_id = c.customer_id
    LEFT JOIN _dim_customer__meta_nav_customer AS mnc
        ON mnc.customer_id = c.customer_id
    LEFT JOIN stg.dim_store AS s
        ON s.store_id = c.store_id
    LEFT JOIN _lake_ultra_merchant_membership AS m
        ON m.customer_id = c.customer_id
    LEFT JOIN _dim_customer__registration AS cr
        ON cr.customer_id = c.customer_id
    LEFT JOIN lake_consolidated.ultra_merchant.membership_type  lmt
        ON lmt.membership_type_id = m.membership_type_id
    LEFT JOIN _dim_customer__first_mobile_app_data AS fma
        ON fma.customer_id = c.customer_id
    LEFT JOIN _dim_customer__onetrust_customer_detail AS ocd
        ON ocd.customer_id = c.customer_id
    LEFT JOIN _dim_customer__registration_type AS rt
        ON rt.customer_id = c.customer_id
    LEFT JOIN _dim_customer__emp_optin_date AS eod
        ON eod.customer_id = c.customer_id
    LEFT JOIN _dim_customer__optout AS oo
        ON oo.customer_id = c.customer_id
    LEFT JOIN _dim_customer__scrubs_registration_date  srd
        ON srd.customer_id = c.customer_id
    LEFT JOIN stg.dim_customer AS src /* Join to preserve any existing values */
        ON src.customer_id = c.customer_id
    LEFT JOIN _dim_customer__email_optin_date eodt
        ON eodt.customer_id = c.customer_id
    LEFT JOIN _dim_customer__sms_optin_date sodt
        ON sodt.customer_id = c.customer_id
     LEFT JOIN _dim_customer__membership_state dms
        ON dms.customer_id = c.customer_id
    LEFT JOIN _dim_customer__hdyh_parent AS hdyhp
        ON hdyhp.customer_id = c.customer_id
-- SELECT * FROM _dim_customer__stg;
UNION ALL
SELECT
    customer_id,
	meta_original_customer_id,
	first_name,
	last_name,
	email,
	store_id,
    default_address_id,
	default_address1,
	default_address2,
	default_city,
	default_state_province,
	default_postal_code,
	default_country_code,
    detail_country_code,
	specialty_country_code,
    finance_specialty_store,
    birth_day,
	birth_year,
	birth_month,
	quiz_zip,
	top_size,
	bottom_size,
	bra_size,
    onepiece_size,
	denim_size,
	dress_size,
	shoe_size,
	gender,
	first_gender,
    is_friend_referral,
	membership_level,
	how_did_you_hear,
	how_did_you_hear_parent,
	registration_local_datetime,
	secondary_registration_local_datetime,
	primary_registration_store_id,
	secondary_registration_store_id,
	is_secondary_registration_after_vip,
	is_blacklisted,
	blacklist_reason,
	is_free_trial,
    is_free_trial_promo,
    is_free_trial_gift_order,
	is_cross_promo,
	is_metanav,
	is_scrubs_customer,
	is_test_customer,
    is_opt_out,
	bralette_size,
	undie_size,
    lingerie_sleep_size,
    emp_optin_datetime,
    first_mobile_app_session_local_datetime,
    first_mobile_app_session_id,
    first_mobile_app_order_local_datetime,
    first_mobile_app_order_id,
    mobile_app_cohort_month_date,
    mobile_app_cohort_os,
	onetrust_consent_strictlynecessary,
    onetrust_consent_performance,
    onetrust_consent_functional,
    onetrust_consent_targeting,
    onetrust_consent_social,
    onetrust_update_datetime,
    registration_type,
    is_deleted,
    current_membership_type,
    membership_price,
    membership_plan_id,
    email_opt_in_datetime,
    sms_opt_in_datetime,
    membership_state,
    customer_bucket_group
FROM reference.dummy_customer
WHERE $is_full_refresh;

INSERT INTO stg.dim_customer_stg (
	customer_id,
    meta_original_customer_id,
	first_name,
	last_name,
	email,
	store_id,
    default_address_id,
	default_address1,
	default_address2,
	default_city,
	default_state_province,
	default_postal_code,
	default_country_code,
    detail_country_code,
	specialty_country_code,
    finance_specialty_store,
    birth_day,
	birth_year,
	birth_month,
	quiz_zip,
	top_size,
	bottom_size,
	bra_size,
    onepiece_size,
	denim_size,
	dress_size,
	shoe_size,
	gender,
	first_gender,
    is_friend_referral,
	membership_level,
	how_did_you_hear,
    how_did_you_hear_parent,
	registration_local_datetime,
	secondary_registration_local_datetime,
	primary_registration_store_id,
	secondary_registration_store_id,
	is_secondary_registration_after_vip,
	is_blacklisted,
	blacklist_reason,
	is_free_trial,
    is_free_trial_promo,
    is_free_trial_gift_order,
	is_cross_promo,
	is_metanav,
    is_scrubs_customer,
	is_test_customer,
    is_opt_out,
	bralette_size,
	undie_size,
    lingerie_sleep_size,
    emp_optin_datetime,
    first_mobile_app_session_local_datetime,
    first_mobile_app_session_id,
    first_mobile_app_order_local_datetime,
    first_mobile_app_order_id,
    mobile_app_cohort_month_date,
    mobile_app_cohort_os,
	onetrust_consent_strictlynecessary,
    onetrust_consent_performance,
    onetrust_consent_functional,
    onetrust_consent_targeting,
    onetrust_consent_social,
    onetrust_update_datetime,
    registration_type,
    is_deleted,
    current_membership_type,
    membership_price,
    membership_plan_id,
    email_opt_in_datetime,
    sms_opt_in_datetime,
    membership_state,
    customer_bucket_group,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
	customer_id,
	meta_original_customer_id,
	first_name,
	last_name,
	email,
	store_id,
    default_address_id,
	default_address1,
	default_address2,
	default_city,
	default_state_province,
	default_postal_code,
	default_country_code,
    detail_country_code,
	specialty_country_code,
    finance_specialty_store,
    birth_day,
	birth_year,
	birth_month,
	quiz_zip,
	top_size,
	bottom_size,
	bra_size,
    onepiece_size,
	denim_size,
	dress_size,
	shoe_size,
	gender,
	first_gender,
    is_friend_referral,
	membership_level,
	how_did_you_hear,
    how_did_you_hear_parent,
	registration_local_datetime,
	secondary_registration_local_datetime,
	primary_registration_store_id,
	secondary_registration_store_id,
	is_secondary_registration_after_vip,
	is_blacklisted,
	blacklist_reason,
	is_free_trial,
    is_free_trial_promo,
    is_free_trial_gift_order,
	is_cross_promo,
	is_metanav,
	is_scrubs_customer,
	is_test_customer,
    is_opt_out,
	bralette_size,
	undie_size,
    lingerie_sleep_size,
    emp_optin_datetime,
    first_mobile_app_session_local_datetime,
    first_mobile_app_session_id,
    first_mobile_app_order_local_datetime,
    first_mobile_app_order_id,
    mobile_app_cohort_month_date,
    mobile_app_cohort_os,
	onetrust_consent_strictlynecessary,
    onetrust_consent_performance,
    onetrust_consent_functional,
    onetrust_consent_targeting,
    onetrust_consent_social,
    onetrust_update_datetime,
    registration_type,
    is_deleted,
    current_membership_type,
    membership_price,
    membership_plan_id,
    email_opt_in_datetime,
    sms_opt_in_datetime,
    membership_state,
    customer_bucket_group,
    $execution_start_time AS meta_create_datetime,
	$execution_start_time AS meta_create_datetime
FROM _dim_customer__stg
ORDER BY customer_id;
