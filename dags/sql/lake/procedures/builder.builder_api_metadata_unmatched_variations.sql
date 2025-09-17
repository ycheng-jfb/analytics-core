CREATE OR REPLACE TRANSIENT TABLE builder.builder_api_metadata_unmatched_variations AS
WITH variations AS (
    SELECT
        COALESCE(properties_test_variation_id,
            JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id')
        ) AS test_variation_id,
        COALESCE(
            properties_test_variation_name,
            JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_name')
        ) AS test_variation_name,
        'Fabletics' AS brand,
        receivedat AS event_ts
    FROM lake.segment_fl.javascript_fabletics_builder_test_entered
    WHERE receivedat > (CURRENT_DATE - INTERVAL '3 month')
    UNION ALL
    SELECT
        JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id')
            AS test_variation_id,
        JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_name')
            AS test_variation_name,
        'JustFab' AS brand,
        receivedat AS event_ts
    FROM lake.segment_gfb.javascript_justfab_builder_test_entered
    WHERE
        receivedat > (CURRENT_DATE - INTERVAL '3 month')
        AND test_variation_id IS NOT NULL
    UNION ALL
    SELECT
        JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id')
            AS test_variation_id,
        JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_name')
            AS test_variation_name,
        'ShoeDazzle' AS brand,
        receivedat AS event_ts
    FROM lake.segment_gfb.javascript_shoedazzle_builder_test_entered
    WHERE receivedat > (CURRENT_DATE - INTERVAL '3 month')
    UNION ALL
    SELECT
        JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id')
            AS test_variation_id,
        JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_name')
            AS test_variation_name,
        'FabKids' AS brand,
        receivedat AS event_ts
    FROM lake.segment_gfb.javascript_fabkids_builder_test_entered
    WHERE receivedat > (CURRENT_DATE - INTERVAL '3 month')
    UNION ALL
    SELECT
        JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id')
            AS test_variation_id,
        JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_name')
            AS test_variation_name,
        'SXF' AS brand,
        receivedat AS event_ts
    FROM lake.segment_sxf.javascript_sxf_builder_test_entered
    WHERE receivedat > (CURRENT_DATE - INTERVAL '3 month')
)
SELECT
    brand,
    test_variation_id,
    test_variation_name,
    MIN(event_ts) AS min_event_ts,
    MAX(event_ts) AS max_event_ts,
    COUNT(*) AS num_sessions
FROM variations WHERE
    test_variation_id NOT IN (
        SELECT DISTINCT test_variation_id
        FROM lake.builder.builder_api_metadata api
    )
GROUP BY ALL
ORDER BY num_sessions DESC;
