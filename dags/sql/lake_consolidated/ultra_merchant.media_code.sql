SET lake_placeholder = %(lake_placeholder)s :: string ;
SET company_id = %(company_id)s :: INT;
SET data_source_id = %(data_source_id)s :: INT;
use IDENTIFIER($lake_placeholder);

SET lake_fl_watermark = (
SELECT min(last_update)
FROM (
    SELECT COALESCE(MAX(meta_update_datetime), '1970-01-01') AS last_update
    FROM lake_consolidated.ultra_merchant.media_code WHERE data_source_id = $data_source_id
    UNION
    SELECT CAST(MAX(hvr_change_time) AS TIMESTAMP_LTZ(3)) AS last_update
    FROM ultra_merchant.media_code
    ) AS A
);

CREATE OR REPLACE TEMP TABLE _session_media_data AS
SELECT smd.session_id, st.store_id, st.company_id
FROM ultra_merchant.session_media_data AS smd
    JOIN ultra_merchant.session AS s
        ON s.session_id = smd.session_id
    JOIN reference.dim_store AS st
        ON st.store_id = s.store_id
        and st.company_id=$company_id
    WHERE smd.hvr_change_time >= DATEADD(MINUTE, -5, $lake_fl_watermark);
--    QUALIFY row_number() over(PARTITION BY media_code_id
--    ORDER BY CAST(l.hvr_change_time AS TIMESTAMP_LTZ(3)) DESC) = 1;



CREATE OR REPLACE TEMP TABLE _session_media_data_media_code AS
SELECT $data_source_id as data_source_id, src.company_id,  mc.media_code_id, mc.media_code_type_id, mc.media_type_id, mc.media_publisher_id, mc.administrator_id, mc.code, mc.enable_entry_popups,
mc.enable_exit_popups, mct.label, mc.hvr_is_deleted, mc.meta_row_source, mc.hvr_change_time, mc.datetime_added,mc.datetime_modified, mc.media_code_id || src.company_id as META_ORIGINAL_MEDIA_CODE_ID
FROM _session_media_data AS src
    JOIN ultra_merchant.session_media_data AS smd
        ON src.session_id = smd.session_id
    JOIN ultra_merchant.media_code AS mc
        ON mc.media_code_id = smd.placement_media_code_id
    LEFT JOIN ultra_merchant.media_code_type AS mct
        ON mct.media_code_type_id = mc.media_code_type_id
WHERE smd.placement_media_code_id IS NOT NULL
UNION
SELECT $data_source_id as data_source_id, src.company_id,  mc.media_code_id, mc.media_code_type_id, mc.media_type_id, mc.media_publisher_id, mc.administrator_id, mc.code, mc.enable_entry_popups,
mc.enable_exit_popups, mct.label, mc.hvr_is_deleted, mc.meta_row_source, mc.hvr_change_time,mc.datetime_added,mc.datetime_modified,mc.media_code_id || src.company_id as META_ORIGINAL_MEDIA_CODE_ID
FROM _session_media_data AS src
    JOIN ultra_merchant.session_media_data AS smd
        ON src.session_id = smd.session_id
    JOIN ultra_merchant.media_code AS mc
        ON mc.media_code_id = smd.creative_media_code_id
    LEFT JOIN ultra_merchant.media_code_type AS mct
        ON mct.media_code_type_id = mc.media_code_type_id
WHERE smd.creative_media_code_id IS NOT NULL
UNION
SELECT $data_source_id as data_source_id, src.company_id,  mc.media_code_id, mc.media_code_type_id, mc.media_type_id, mc.media_publisher_id, mc.administrator_id, mc.code, mc.enable_entry_popups,
mc.enable_exit_popups, mct.label, mc.hvr_is_deleted, mc.meta_row_source, mc.hvr_change_time, mc.datetime_added,mc.datetime_modified, mc.media_code_id || src.company_id as META_ORIGINAL_MEDIA_CODE_ID
FROM _session_media_data AS src
    JOIN ultra_merchant.session_media_data AS smd
        ON src.session_id = smd.session_id
    JOIN ultra_merchant.media_code AS mc
        ON mc.media_code_id = smd.ad_media_code_id
    LEFT JOIN ultra_merchant.media_code_type AS mct
        ON mct.media_code_type_id = mc.media_code_type_id
WHERE smd.ad_media_code_id IS NOT NULL
UNION
SELECT $data_source_id as data_source_id, src.company_id,  mc.media_code_id, mc.media_code_type_id, mc.media_type_id, mc.media_publisher_id, mc.administrator_id, mc.code, mc.enable_entry_popups,
mc.enable_exit_popups, mct.label, mc.hvr_is_deleted, mc.meta_row_source, mc.hvr_change_time, mc.datetime_added,mc.datetime_modified,
mc.media_code_id || src.company_id as META_ORIGINAL_MEDIA_CODE_ID
FROM _session_media_data AS src
    JOIN ultra_merchant.session_media_data AS smd
        ON src.session_id = smd.session_id
    JOIN ultra_merchant.media_code AS mc
        ON mc.media_code_id = smd.sub_media_code_id
    LEFT JOIN ultra_merchant.media_code_type AS mct
        ON mct.media_code_type_id = mc.media_code_type_id
WHERE smd.sub_media_code_id IS NOT NULL;


MERGE INTO lake_consolidated.ultra_merchant.media_code t
    USING (
        SELECT A.* FROM _session_media_data_media_code AS A ) s
    ON s.data_source_id = t.data_source_id
    AND CASE WHEN s.media_code_id IS NOT NULL THEN CONCAT(s.media_code_id, s.company_id) ELSE NULL END = t.media_code_id
    WHEN MATCHED
        AND cast(s.hvr_change_time as TIMESTAMP_LTZ(3))  > cast(t.meta_create_datetime as TIMESTAMP_LTZ(3))
    THEN UPDATE SET
        t.data_source_id = s.data_source_id,
        t.meta_company_id=s.company_id,
        t.media_code_id = CASE WHEN s.media_code_id IS NOT NULL THEN CONCAT(s.media_code_id, s.company_id) ELSE NULL END,
        t.media_code_type_id = s.media_code_type_id,
        t.media_type_id = s.media_type_id,
        t.media_publisher_id = s.media_publisher_id,
        t.administrator_id = s.administrator_id,
        t.code = s.code,
        t.label = s.label,
        t.enable_entry_popups = s.enable_entry_popups,
        t.enable_exit_popups = s.enable_exit_popups,
        t.datetime_added = s.datetime_added,
        t.datetime_modified = s.datetime_modified,
        t.meta_original_media_code_id=s.meta_original_media_code_id,
        t.HVR_IS_DELETED = s.HVR_IS_DELETED,
        t.meta_create_datetime = cast (s.hvr_change_time as TIMESTAMP_LTZ(3)),
        t.meta_update_datetime = cast (s.hvr_change_time as TIMESTAMP_LTZ(3))
    WHEN NOT MATCHED THEN INSERT (
        data_source_id,
		meta_company_id,
		media_code_id,
        media_code_type_id,
        media_type_id,
        media_publisher_id,
        administrator_id,
        code,
        label,
        enable_entry_popups,
        enable_exit_popups,
        datetime_added,
        datetime_modified,
        meta_original_media_code_id,
        hvr_is_deleted,
        meta_create_datetime,
        meta_update_datetime    )
    VALUES (
      s.data_source_id,
      s.company_id,
      CASE WHEN s.media_code_id IS NOT NULL THEN CONCAT(s.media_code_id, s.company_id) ELSE NULL END,
        s.media_code_type_id,
        s.media_type_id,
        s.media_publisher_id,
        s.administrator_id,
        s.code,
        s.label,
        s.enable_entry_popups,
        s.enable_exit_popups,
        s.datetime_added,
        s.datetime_modified,
        s.meta_original_media_code_id,
        s.hvr_is_deleted ,
        cast(s.hvr_change_time as TIMESTAMP_LTZ(3) ),
        cast(s.hvr_change_time as TIMESTAMP_LTZ(3) )
    );
