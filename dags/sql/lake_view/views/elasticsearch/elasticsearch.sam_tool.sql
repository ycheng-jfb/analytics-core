CREATE OR REPLACE VIEW lake_view.elasticsearch.sam_tool(
	image_id,
	sam_id,
	aem_uuid,
	membership_brand_id,
	master_store_group_id,
	product_name,
	is_ecat,
	is_plus,
	sort,
	datetime_added_sam,
	datetime_modified_sam,
	is_testable,
	json_blob,
	_effective_from_date,
	_effective_to_date,
	_is_current,
	_is_deleted,
	meta_create_datetime,
	meta_update_datetime
) AS
SELECT
    image_id,
    sam_id,
    aem_uuid,
    membership_brand_id,
    master_store_group_id,
    product_name,
    is_ecat,
    is_plus,
    sort,
    datetime_added_sam,
    datetime_modified_sam,
    CASE
        WHEN TRIM(JSON_BLOB:_source.exclude_from_image_test::TEXT) = 'true' THEN 'false'
        ELSE 'true'
    END AS is_testable,
    json_blob,
    _effective_from_date,
    _effective_to_date,
    _is_current,
    _is_deleted,
    meta_create_datetime,
    meta_update_datetime
FROM lake.elasticsearch.sam_tool;
