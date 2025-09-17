-- CREATE TABLE IF NOT EXISTS lake_stg.elasticsearch.sam_tool_stg (
--     sam_id VARCHAR,
--     aem_uuid VARCHAR,
--     membership_brand_id VARCHAR,
--     master_store_group_id VARCHAR,
--     product_name VARCHAR,
--     is_ecat BOOLEAN,
--     is_plus BOOLEAN,
--     sort VARCHAR,
--     datetime_added_sam TIMESTAMP_LTZ(3),
--     datetime_modified_sam TIMESTAMP_LTZ(3),
--     json_blob VARIANT
-- );
-- CREATE TABLE IF NOT EXISTS lake.elasticsearch.sam_tool (
--     image_id NUMBER(38,0) AUTOINCREMENT,
--     sam_id VARCHAR,
--     aem_uuid VARCHAR,
--     membership_brand_id VARCHAR,
--     master_store_group_id VARCHAR,
--     product_name VARCHAR,
--     is_ecat BOOLEAN,
--     is_plus BOOLEAN,
--     sort VARCHAR,
--     datetime_added_sam TIMESTAMP_LTZ(3),
--     datetime_modified_sam TIMESTAMP_LTZ(3),
--     json_blob VARIANT,
--     is_testable BOOLEAN,
--     _effective_from_date TIMESTAMP_LTZ(3),
--     _effective_to_date TIMESTAMP_LTZ(3),
--     _is_current BOOLEAN,
--     _is_deleted BOOLEAN,
--     meta_row_hash INT,
--     meta_create_datetime TIMESTAMP_LTZ(3),
--     meta_update_datetime TIMESTAMP_LTZ(3),
--     PRIMARY KEY (image_id)
-- );
-- CREATE VIEW IF NOT EXISTS lake_view.elasticsearch.sam_tool AS
-- SELECT
--     image_id,
--     sam_id,
--     aem_uuid,
--     membership_brand_id,
--     master_store_group_id,
--     product_name,
--     is_ecat,
--     is_plus,
--     sort,
--     datetime_added_sam,
--     datetime_modified_sam,
--     json_blob,
--     is_testable,
--     _effective_from_date,
--     _effective_to_date,
--     _is_current,
--     _is_deleted,
--     meta_create_datetime,
--     meta_update_datetime
-- FROM lake.elasticsearch.sam_tool;


DELETE FROM lake_stg.elasticsearch.sam_tool_stg;


COPY INTO lake_stg.elasticsearch.sam_tool_stg (sam_id, aem_uuid, membership_brand_id, master_store_group_id, product_name, is_ecat, is_plus, sort, datetime_added_sam, datetime_modified_sam, json_blob)
FROM '@lake_stg.public.tsos_da_int_inbound/lake/lake.elasticsearch.sam_tool/v1'
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '\t',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
)
ON_ERROR = 'SKIP_FILE_1%';


UPDATE lake.elasticsearch.sam_tool target
SET
    target._is_current = false,
    target._effective_to_date =
        IFF(target._effective_from_date < source.datetime_modified_sam, source.datetime_modified_sam, current_timestamp),
    target.meta_update_datetime = current_timestamp
FROM lake_stg.elasticsearch.sam_tool_stg AS source
WHERE EQUAL_NULL(target.sam_id, source.sam_id)
AND (
    target.aem_uuid IS DISTINCT FROM source.aem_uuid OR
    target.membership_brand_id IS DISTINCT FROM source.membership_brand_id OR
    target.master_store_group_id IS DISTINCT FROM COALESCE(source.master_store_group_id, '16') OR
    target.product_name IS DISTINCT FROM source.product_name OR
    target.is_ecat IS DISTINCT FROM source.is_ecat OR
    target.is_plus IS DISTINCT FROM source.is_plus OR
    target.sort IS DISTINCT FROM source.sort OR
    target.datetime_added_sam IS DISTINCT FROM source.datetime_added_sam OR
    target.datetime_modified_sam IS DISTINCT FROM source.datetime_modified_sam
);


MERGE INTO lake.elasticsearch.sam_tool AS target
USING (
    SELECT
        *,
        COALESCE(master_store_group_id, '16') AS master_store_group_id_final,
        current_timestamp AS _effective_from_date,
        '9999-12-31' AS _effective_to_date,
        True AS _is_current,
        False AS _is_deleted,
        True AS is_testable,
        hash(*) AS meta_row_hash,
        current_timestamp AS meta_create_datetime,
        current_timestamp AS meta_update_datetime
    From lake_stg.elasticsearch.sam_tool_stg
    ) AS source
ON EQUAL_NULL(target.sam_id, source.sam_id)
    AND EQUAL_NULL(target.aem_uuid, source.aem_uuid)
    AND EQUAL_NULL(target.membership_brand_id, source.membership_brand_id)
    AND EQUAL_NULL(target.master_store_group_id, source.master_store_group_id_final)
    AND EQUAL_NULL(target.product_name, source.product_name)
    AND EQUAL_NULL(target.is_ecat, source.is_ecat)
    AND EQUAL_NULL(target.is_plus, source.is_plus)
    AND EQUAL_NULL(target.sort, source.sort)
    AND EQUAL_NULL(target.datetime_added_sam, source.datetime_added_sam)
    AND EQUAL_NULL(target.datetime_modified_sam, source.datetime_modified_sam)
WHEN NOT MATCHED THEN INSERT (
    sam_id, aem_uuid, membership_brand_id, master_store_group_id, product_name, is_ecat, is_plus, sort, datetime_added_sam, datetime_modified_sam, json_blob, _effective_from_date, _effective_to_date, _is_current, _is_deleted, is_testable, meta_row_hash, meta_create_datetime, meta_update_datetime
)
VALUES (
    sam_id, aem_uuid, membership_brand_id, master_store_group_id_final, product_name, is_ecat, is_plus, sort, datetime_added_sam, datetime_modified_sam, json_blob, _effective_from_date, _effective_to_date, _is_current, _is_deleted, is_testable, meta_row_hash, meta_create_datetime, meta_update_datetime
);
