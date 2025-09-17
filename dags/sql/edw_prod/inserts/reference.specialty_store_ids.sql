 /* We are creating a filler store_id for YITTY mobile app for when yitty vips shop on FL mobile app.
 We want the order attributed to Yitty, but dont want to lose visibility of the mobile app shopping experience.
 Store_id will be 24101

DA-23565 - Creating a filler store_id for media spend reporting so they can correctly attribute media spend to retail*/
set execution_start_time = current_timestamp;
TRUNCATE TABLE reference.specialty_store_ids;
INSERT INTO reference.specialty_store_ids
(
        store_id,
        store_full_name,
        store_group_id,
        store_group,
        store_type,
        store_sub_type,
        is_core_store,
        store_division,
        store_division_abbr,
        store_brand,
        store_brand_abbr,
        store_region,
        store_country,
        store_currency,
        store_time_zone,
        store_retail_state,
        store_retail_city,
        store_retail_location,
        store_retail_zip_code,
        retail_location_code,
        retail_status,
        retail_region,
        retail_district,
        meta_create_datetime,
        meta_update_datetime
)

VALUES
(
    24101,
    'Yitty Mobile App',
    16,
    'Fabletics',
    'Mobile App',
    'Mobile App',
    1,
    'Fabletics',
    'FL',
    'Yitty',
    'YTY',
    'NA',
    'US',
    'USD',
    'America/Los_Angeles',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    $execution_start_time,
    $execution_start_time
),

(
    9999,
    'Fabletics US Media Spend',
    16,
    'Fabletics',
    'Retail',
    'Media Spend',
    1,
    'Fabletics',
    'FL',
    'Fabletics',
    'FL',
    'NA',
    'US',
    'USD',
    'America/Los_Angeles',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    $execution_start_time,
    $execution_start_time
);
