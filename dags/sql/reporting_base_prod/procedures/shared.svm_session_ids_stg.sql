SET refresh_time = (
    SELECT min(meta_update_datetime)
    FROM (
        SELECT MAX(meta_update_datetime) AS meta_update_datetime FROM reporting_base_prod.shared.session
        UNION ALL
        SELECT MAX(meta_update_datetime) AS meta_update_datetime FROM edw_prod.data_model.dim_customer
        UNION ALL
        SELECT MAX(meta_update_datetime) AS meta_update_datetime FROM edw_prod.data_model.fact_order
        UNION ALL
        SELECT MAX(meta_update_datetime) AS meta_update_datetime FROM edw_prod.data_model.fact_order_product_cost
        UNION ALL
        SELECT MAX(meta_update_datetime) AS meta_update_datetime FROM edw_prod.data_model.fact_activation
        UNION ALL
        SELECT MAX(meta_update_datetime) AS meta_update_datetime FROM lake_consolidated_view.ultra_merchant.dm_site
        UNION ALL
        SELECT MAX(meta_update_datetime) AS meta_update_datetime FROM lake_consolidated_view.ultra_merchant.dm_gateway_test
        UNION ALL
        SELECT MAX(meta_update_datetime) AS meta_update_datetime FROM reporting_base_prod.staging.site_visit_metrics
        UNION ALL
        SELECT MAX(meta_update_datetime) AS meta_update_datetime FROM edw_prod.data_model.fact_registration
        )
    );

/* Need to revert it to 2 years after DBSplit. Temporarily changed to 2 weeks */
SET (two_weeks_ago) = (SELECT DATEADD(DAY,-14,CURRENT_DATE()));

SET max_refresh_time = (
    SELECT min(refeshtime)
    FROM(
        SELECT ifnull(MAX(refreshtime),$two_weeks_ago) AS refeshtime FROM reporting_base_prod.shared.session_refresh_base
        UNION ALL
        SELECT ifnull(MAX(refreshtime),$two_weeks_ago) AS refeshtime FROM reporting_base_prod.shared.session_single_view_media
        )
    );

/* this is the staging table which has only delta data */
INSERT OVERWRITE INTO shared.svm_session_ids_stg
SELECT DISTINCT
    session_id,
    left(session_id,len(session_id)-2),
    $refresh_time as refresh_time
FROM (
    SELECT session_id
    FROM staging.svm_session_ids_excp

    UNION ALL
    SELECT s.session_id
    FROM shared.session AS s
    JOIN edw_prod.data_model.dim_store AS t
        ON t.store_id = s.store_id
    JOIN shared.media_source_channel_mapping AS m
        ON m.media_source_hash = s.media_source_hash
    WHERE t.STORE_TYPE <> 'Retail'
        AND IS_TEST_CUSTOMER_ACCOUNT = FALSE
        AND NVL(is_in_segment, TRUE) != FALSE
        AND session_local_datetime >= $two_weeks_ago
        AND s.meta_update_datetime > $max_refresh_time

    UNION ALL
    SELECT DISTINCT s.session_id
    FROM shared.session AS s
    JOIN edw_prod.data_model.dim_store AS t
        ON t.store_id = s.store_id
    JOIN shared.media_source_channel_mapping AS m
        ON m.media_source_hash = s.media_source_hash
    JOIN edw_prod.data_model.fact_order fo
        ON fo.session_id = s.session_id
    LEFT JOIN edw_prod.data_model.fact_order_product_cost pc
        ON fo.order_id = pc.order_id
    WHERE t.STORE_TYPE <> 'Retail'
      AND IS_TEST_CUSTOMER_ACCOUNT = FALSE
      AND NVL(is_in_segment, TRUE) != FALSE
      AND session_local_datetime >= $two_weeks_ago
        AND (
                fo.meta_update_datetime > $max_refresh_time
                    OR pc.meta_update_datetime > $max_refresh_time
                )

    UNION ALL
    SELECT DISTINCT s.session_id
    FROM shared.session AS s
    JOIN edw_prod.data_model.dim_store AS t
        ON t.store_id = s.store_id
    JOIN shared.media_source_channel_mapping AS m
        ON m.media_source_hash = s.media_source_hash
    JOIN edw_prod.data_model.dim_customer AS dc
        ON dc.customer_id = s.customer_id
    WHERE t.STORE_TYPE <> 'Retail'
      AND IS_TEST_CUSTOMER_ACCOUNT = FALSE
      AND NVL(is_in_segment, TRUE) != FALSE
      AND session_local_datetime >= $two_weeks_ago
        AND dc.meta_update_datetime > $max_refresh_time

    UNION ALL
    SELECT DISTINCT s.session_id
    FROM shared.session AS s
    JOIN edw_prod.data_model.dim_store AS t
        ON t.store_id = s.store_id
    JOIN shared.media_source_channel_mapping AS m
        ON m.media_source_hash = s.media_source_hash
    JOIN edw_prod.data_model.fact_registration fr
        ON fr.session_id = s.session_id
    LEFT JOIN staging.site_visit_metrics vm
        ON vm.session_id = fr.session_id
    LEFT JOIN edw_prod.data_model.fact_activation fa
        ON fr.customer_id = fa.customer_id
    LEFT JOIN edw_prod.data_model.fact_order o
        ON fa.session_id = o.session_id
    LEFT JOIN edw_prod.data_model.fact_order_product_cost opc
        ON o.order_id = opc.order_id
    WHERE t.STORE_TYPE <> 'Retail'
        AND IS_TEST_CUSTOMER_ACCOUNT = FALSE
        AND NVL(is_in_segment, TRUE) != FALSE
        AND session_local_datetime >= $two_weeks_ago
        AND (
            fr.meta_update_datetime > $max_refresh_time
            OR vm.meta_update_datetime > $max_refresh_time
            OR fa.meta_update_datetime > $max_refresh_time
            OR o.meta_update_datetime > $max_refresh_time
            OR opc.meta_update_datetime > $max_refresh_time
            )

    UNION ALL
    SELECT DISTINCT s.session_id
    FROM shared.session AS s
    JOIN edw_prod.data_model.dim_store AS t
        ON t.store_id = s.store_id
    JOIN shared.media_source_channel_mapping AS m
        ON m.media_source_hash = s.media_source_hash
    JOIN lake_consolidated_view.ultra_merchant.dm_site dms
        ON dms.dm_site_id = s.dm_site_id --get start AND end date FROM here
    WHERE t.STORE_TYPE <> 'Retail'
        AND IS_TEST_CUSTOMER_ACCOUNT = FALSE
        AND NVL(is_in_segment, TRUE) != FALSE
        AND session_local_datetime >= $two_weeks_ago
        AND dms.meta_update_datetime > $max_refresh_time

    UNION ALL
    SELECT DISTINCT s.session_id
    FROM shared.session AS s
    JOIN edw_prod.data_model.dim_store AS t
        ON t.store_id = s.store_id
    JOIN shared.media_source_channel_mapping AS m
        ON m.media_source_hash = s.media_source_hash
    JOIN shared.dim_gateway_test_site dgg
        ON dgg.dm_gateway_test_site_id = s.dm_gateway_test_site_id
        AND dgg.effective_start_datetime <= s.session_local_datetime
        AND dgg.effective_end_datetime > s.session_local_datetime
    JOIN lake_consolidated_view.ultra_merchant.dm_gateway_test AS g
        ON g.dm_gateway_test_id = dgg.gateway_test_id
    WHERE t.STORE_TYPE <> 'Retail'
        AND IS_TEST_CUSTOMER_ACCOUNT = FALSE
        AND NVL(is_in_segment, TRUE) != FALSE
        AND session_local_datetime >= $two_weeks_ago
        AND g.meta_update_datetime > $max_refresh_time
    ) AS A
ORDER BY session_id ASC;

TRUNCATE TABLE staging.svm_session_ids_excp;
