SET execution_timestamp = current_timestamp;
TRUNCATE TABLE validation.new_online_or_unknown_store;
INSERT INTO validation.new_online_or_unknown_store
(
    store_id,
    store_name,
    alias,
    store_group,
    code,
    store_type,
    meta_create_datetime,
    meta_update_datetime
)

WITH _offset_store_classification AS (
    SELECT DISTINCT store_id
    FROM lake_consolidated.ultra_merchant.store_classification AT(OFFSET => -24*60*60)
    WHERE store_type_id NOT IN (6,8) /* not retail or mobile app */
),
    _new_online_store AS (
    SELECT DISTINCT sc.store_id
    FROM lake_consolidated.ultra_merchant.store_classification AS sc
        LEFT JOIN _offset_store_classification AS osc
            ON osc.store_id = sc.store_id
    WHERE
        osc.store_id IS NULL
        AND sc.store_type_id NOT IN (6,8)
)

SELECT
    s.store_id,
    s.label AS store_name,
    s.alias,
    sg.label AS store_group,
    s.code,
    'Online' AS store_type,
    $execution_timestamp,
    $execution_timestamp
FROM lake_consolidated.ultra_merchant.store AS s
    JOIN _new_online_store AS nos
        ON nos.store_id = s.store_id
    JOIN lake_consolidated.ultra_merchant.store_group AS sg
        ON sg.store_group_id = s.store_group_id

UNION

SELECT
    s.store_id,
    s.label AS store_name,
    s.alias,
    sg.label AS store_group,
    s.code,
    'Unknown' AS store_type,
    $execution_timestamp,
    $execution_timestamp
FROM lake_consolidated.ultra_merchant.store AS s
    JOIN lake_consolidated.ultra_merchant.store_group AS sg
        ON sg.store_group_id = s.store_group_id
    LEFT JOIN lake_consolidated.ultra_merchant.store AT(OFFSET => -24*60*60) AS s2
        ON s2.store_id = s.store_id
WHERE
    s2.store_id IS NULL
    AND s.store_id NOT IN (SELECT store_id FROM lake_consolidated.ultra_merchant.store_classification);


