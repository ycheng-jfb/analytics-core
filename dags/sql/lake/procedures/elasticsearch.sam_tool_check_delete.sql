UPDATE lake.elasticsearch.sam_tool
SET
    _is_deleted = true,
    _is_current = false,
    _effective_to_date = CURRENT_TIMESTAMP(),
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE _is_current = true
    AND aem_uuid NOT IN (SELECT aem_uuid FROM lake_stg.elasticsearch.sam_tool_stg);
