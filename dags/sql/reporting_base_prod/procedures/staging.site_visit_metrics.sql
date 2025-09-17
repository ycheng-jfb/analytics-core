SET target_table = 'reporting_base_prod.staging.site_visit_metrics';
SET execution_start_time = CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(public.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE,
                                  FALSE));

MERGE INTO public.meta_table_dependency_watermark AS t
    USING (SELECT $target_table                               AS table_name,
                  NULLIF(dependent_table_name, $target_table) AS dependent_table_name,
                  high_watermark_datetime                     AS new_high_watermark_datetime
           FROM (SELECT $target_table         AS dependent_table_name,
                        MAX(meta_update_datetime) AS high_watermark_datetime
                 FROM staging.site_visit_metrics
                 UNION ALL
                 SELECT 'lake_consolidated_view.ultra_merchant.session' AS dependent_table_name,
                        MAX(meta_update_datetime)                       AS high_watermark_datetime
                 FROM lake_consolidated_view.ultra_merchant.session
                 UNION ALL
                 SELECT 'lake_consolidated_view.ultra_merchant.customer_quiz' AS dependent_table_name,
                        MAX(meta_update_datetime)                             AS high_watermark_datetime
                 FROM lake_consolidated_view.ultra_merchant.customer_quiz
                 UNION ALL
                 SELECT 'lake_consolidated_view.ultra_merchant.membership_signup' AS dependent_table_name,
                        MAX(meta_update_datetime)                                 AS high_watermark_datetime
                 FROM lake_consolidated_view.ultra_merchant.membership_signup
                 UNION ALL
                 SELECT 'lake_consolidated_view.ultra_merchant.session_detail' AS dependent_table_name,
                        MAX(meta_update_datetime)                              AS high_watermark_datetime
                 FROM lake_consolidated_view.ultra_merchant.session_detail) AS h) AS s
    ON t.table_name = s.table_name
        AND EQUAL_NULL(t.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED
        AND NOT EQUAL_NULL(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
        THEN UPDATE SET
        t.new_high_watermark_datetime = s.new_high_watermark_datetime,
        t.meta_update_datetime = $execution_start_time
    WHEN NOT MATCHED
        THEN INSERT (
                     table_name,
                     dependent_table_name,
                     high_watermark_datetime,
                     new_high_watermark_datetime
        )
        VALUES (s.table_name,
                s.dependent_table_name,
                '1900-01-01'::timestamp_ltz,
                s.new_high_watermark_datetime);

SET wm_lake_consolidated_view_ultra_merchant_session_detail = public.udf_get_watermark($target_table,
                                                                                       'lake_consolidated_view.ultra_merchant.session_detail');
SET wm_lake_consolidated_view_ultra_merchant_session = public.udf_get_watermark($target_table,
                                                                                'lake_consolidated_view.ultra_merchant.session');
SET wm_lake_consolidated_view_ultra_merchant_customer_quiz = public.udf_get_watermark($target_table,
                                                                                      'lake_consolidated_view.ultra_merchant.customer_quiz');
SET wm_lake_consolidated_view_ultra_merchant_membership_signup = public.udf_get_watermark($target_table,
                                                                                          'lake_consolidated_view.ultra_merchant.membership_signup');

CREATE OR REPLACE TEMP TABLE _session_base
(
    session_id NUMBER
);

INSERT INTO _session_base (session_id)
SELECT DISTINCT session_id
FROM lake_consolidated_view.ultra_merchant.session
WHERE $is_full_refresh;

INSERT INTO _session_base (session_id)
SELECT DISTINCT session_id
FROM (SELECT session_id
      FROM lake_consolidated_view.ultra_merchant.session
      WHERE meta_update_datetime > $wm_lake_consolidated_view_ultra_merchant_session
      UNION ALL
      SELECT session_id
      FROM lake_consolidated_view.ultra_merchant.customer_quiz
      WHERE meta_update_datetime > $wm_lake_consolidated_view_ultra_merchant_customer_quiz
      UNION ALL
      SELECT session_id
      FROM lake_consolidated_view.ultra_merchant.membership_signup
      WHERE meta_update_datetime > $wm_lake_consolidated_view_ultra_merchant_membership_signup
      UNION ALL
      SELECT session_id
      FROM lake_consolidated_view.ultra_merchant.session_detail
      WHERE meta_update_datetime > $wm_lake_consolidated_view_ultra_merchant_session_detail
      UNION ALL
      SELECT session_id
      FROM staging.site_visit_metrics_excp) AS a
WHERE session_id > 0
  AND NOT $is_full_refresh
ORDER BY session_id ASC;

--getting sessions where there is a skip quiz record
--(20210309) this is not currently working for FK but will get fixed soon
CREATE OR REPLACE TEMPORARY TABLE _skip_quiz AS
SELECT DISTINCT base.session_id
FROM _session_base base
JOIN lake_consolidated_view.ultra_merchant.session_detail sd
    ON base.session_id = sd.session_id
WHERE sd.name IN ('skipquiz','skip style quiz','quiz_skip','skip_quiz','quiz_skip_count','skipquiz_greenchef');

--getting speedy sign up registration records
CREATE OR REPLACE TEMPORARY TABLE _ssu AS
SELECT DISTINCT
    session_id,
    store_id,
    is_quiz_signup
FROM (
    SELECT
        base.session_id,
        t.store_id,
        CASE WHEN c.value = 'quiz' THEN 1 ELSE 0 END AS is_quiz_signup
    FROM _session_base base
    JOIN lake_consolidated_view.ultra_merchant.session AS n
        ON n.session_id = base.session_id
    JOIN lake_consolidated_view.ultra_merchant.customer_detail AS c
        ON n.customer_id = c.customer_id
    JOIN lake_consolidated_view.ultra_merchant.store AS t
        ON t.store_id = n.store_id
    WHERE c.name = 'signup_source'
        AND VALUE NOT IN ('website','mobile_website','quiz')

    UNION ALL

    SELECT
        base.session_id,
        t.store_id,
        0 AS is_quiz_signup
    FROM _session_base base
    JOIN lake_consolidated_view.ultra_merchant.session AS n
        ON n.session_id = base.session_id
    JOIN lake_consolidated_view.ultra_merchant.store AS t
        ON t.store_id = n.store_id
    JOIN lake_consolidated_view.ultra_merchant.session_detail AS c
        ON n.session_id = c.session_id
    WHERE (alias = 'Fabletics' AND name IN ('speedysignupvue','speedy_signup','ss_reg','ss_start') and c.DATETIME_ADDED <= '2021-12-20')
        or (alias = 'Fabletics' AND name = 'speedysignupvue' and c.DATETIME_ADDED > '2021-12-20')
    ) AS A;

--getting quiz starts and completions
--(20210309) quiz completion records for SXF are 99% null. will be fixed soon. going to use skipquiz and SSU records to 'fake' quiz completions
CREATE OR REPLACE TEMPORARY TABLE _customer_quiz as
SELECT
    base.session_id,
    customer_id,
    customer_quiz_id,
    datetime_completed,
    ROW_NUMBER() OVER(PARTITION BY cq.session_id ORDER BY customer_quiz_id DESC) AS rn
FROM _session_base base
 JOIN lake_consolidated_view.ultra_merchant.customer_quiz cq
    ON cq.session_id = base.session_id
QUALIFY ROW_NUMBER() OVER(PARTITION BY cq.session_id ORDER BY COALESCE(datetime_completed, '1900-01-01 00:00:00.000') DESC, customer_quiz_id DESC) = 1;

--lead registrations records
CREATE OR REPLACE TEMPORARY TABLE _membership_signup AS
SELECT DISTINCT
    base.session_id,
    ms.customer_id,
    row_number() over(partition by base.session_id order by customer_id) as rn
FROM _session_base base
JOIN lake_consolidated_view.ultra_merchant.membership_signup AS  ms
    ON base.session_id = ms.session_id
WHERE ms.customer_id is not null
QUALIFY row_number() over(partition by base.session_id order by customer_id) = 1;

CREATE OR REPLACE TEMPORARY TABLE _site_visit_metrics_stg AS
SELECT
    sb.session_id,
    IFF((cq.customer_quiz_id IS NOT NULL OR ssu.is_quiz_signup = 1), 1, 0)    AS                                         is_quiz_start_action,
    IFF(datetime_completed IS NOT NULL, 1, 0)                                 AS                                         is_quiz_complete_action,
    IFF(ms.customer_id IS NULL,FALSE,TRUE)                                    AS                                         is_lead_registration_action,
    CASE
        WHEN r.alias <> 'Savage X' AND sk.session_id IS NULL AND (ssu.session_id IS NOT NULL OR (cq.customer_quiz_id IS NOT NULL AND datetime_completed IS NULL)) AND is_lead_registration_action = 1 THEN 1
        WHEN r.alias = 'Savage X' AND ssu.is_quiz_signup = 0 AND sk.session_id IS NULL AND is_lead_registration_action = 1 THEN 1
        ELSE 0
    END                                                                       AS                                         is_speedy_registration_action,
    IFF(sk.session_id IS NOT NULL AND (cq.customer_quiz_id IS NOT NULL OR ssu.is_quiz_signup = 0) AND
        datetime_completed IS NULL AND is_lead_registration_action = 1, 1, 0) AS                                         is_skip_quiz_registration_action, --skip quiz records in session detail are broken for FK, so it will be counted as SSU registration
    IFF(ms.customer_id IS NOT NULL AND is_speedy_registration_action = 0 AND is_skip_quiz_registration_action = 0, 1, 0) is_quiz_registration_action,
    HASH(sb.session_id,is_quiz_start_action,is_quiz_complete_action,is_lead_registration_action,is_speedy_registration_action,
         is_skip_quiz_registration_action,is_quiz_registration_action)        AS                                         meta_row_hash,
    meta_original_session_id
FROM _session_base AS sb
JOIN lake_consolidated_view.ultra_merchant.session AS s
    ON s.session_id = sb.session_id
LEFT JOIN lake_consolidated_view.ultra_merchant.store AS r
    ON r.store_id = s.store_id
LEFT JOIN _customer_quiz AS cq
    ON cq.session_id = sb.session_id
LEFT JOIN _membership_signup AS ms
    ON ms.session_id = sb.session_id
LEFT JOIN _skip_quiz AS sk
    ON sb.session_id = sk.session_id
LEFT JOIN _ssu AS ssu
    ON sb.session_id = ssu.session_id;


MERGE INTO staging.site_visit_metrics t
    USING _site_visit_metrics_stg src
    ON EQUAL_NULL(t.session_id, src.session_id)
    WHEN NOT MATCHED THEN
        INSERT (session_id,
                is_quiz_start_action,
                is_quiz_complete_action,
                is_lead_registration_action,
                is_speedy_registration_action,
                is_quiz_registration_action,
                is_skip_quiz_registration_action,
                meta_original_session_id,
                meta_row_hash,
                meta_create_datetime,
                meta_update_datetime)
            VALUES (src.session_id,
                    src.is_quiz_start_action,
                    src.is_quiz_complete_action,
                    src.is_lead_registration_action,
                    src.is_speedy_registration_action,
                    src.is_quiz_registration_action,
                    src.is_skip_quiz_registration_action,
                    src.meta_original_session_id,
                    src.meta_row_hash,
                    $execution_start_time,
                    $execution_start_time)
    WHEN MATCHED AND NOT EQUAL_NULL(t.meta_row_hash, src.meta_row_hash)
        THEN
        UPDATE SET t.is_quiz_start_action = src.is_quiz_start_action,
            t.is_quiz_complete_action = src.is_quiz_complete_action,
            t.is_lead_registration_action = src.is_lead_registration_action,
            t.is_speedy_registration_action = src.is_speedy_registration_action,
            t.is_quiz_registration_action = src.is_quiz_registration_action,
            t.is_skip_quiz_registration_action = src.is_skip_quiz_registration_action,
            t.meta_row_hash = src.meta_row_hash,
            t.meta_update_datetime = $execution_start_time;

UPDATE public.meta_table_dependency_watermark
SET
    high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime = $execution_start_time
WHERE table_name = $target_table;


TRUNCATE TABLE staging.site_visit_metrics_excp;
