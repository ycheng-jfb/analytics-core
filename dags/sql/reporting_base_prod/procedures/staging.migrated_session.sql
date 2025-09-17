SET low_watermark_ltz = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _session_base (session_id number) CLUSTER BY (TRUNC(session_id, -4));

INSERT INTO _session_base (session_id)
SELECT DISTINCT s.session_id
FROM (
    SELECT new_session_id as session_id
    FROM lake_consolidated_view.ultra_merchant.session_migration_log
    WHERE meta_update_datetime > $low_watermark_ltz
    ) AS s
ORDER BY s.session_id;

MERGE INTO staging.migrated_session tgt USING (
    SELECT
        s.session_id,
        edw_prod.stg.udf_unconcat_brand(s.session_id) AS meta_original_session_id,
        ml.max_previous_session_id as migration_previous_session_id,
        HASH(session_id, migration_previous_session_id) as meta_row_hash
    FROM _session_base s
    JOIN (
            SELECT sml.new_session_id, max(sml.previous_session_id) AS max_previous_session_id
            FROM lake_consolidated_view.ultra_merchant.session_migration_log sml
            JOIN _session_base sb ON sb.session_id = sml.new_session_id
            GROUP BY sml.new_session_id
        ) AS ml
    ON s.session_id = ml.new_session_id
    ORDER BY session_id
    ) AS src
    ON src.session_id = tgt.session_id
    WHEN NOT MATCHED THEN
        INSERT (
            session_id,
            migration_previous_session_id,
            meta_original_session_id,
            meta_row_hash)
        VALUES (
            src.session_id,
            src.migration_previous_session_id,
            src.meta_original_session_id,
            src.meta_row_hash
            )
    WHEN MATCHED AND src.meta_row_hash != tgt.meta_row_hash
        THEN UPDATE SET
            tgt.migration_previous_session_id = src.migration_previous_session_id,
            tgt.meta_row_hash = src.meta_row_hash,
            tgt.meta_original_session_id = src.meta_original_session_id,
            tgt.meta_update_datetime = current_timestamp;
