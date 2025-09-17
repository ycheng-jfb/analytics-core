TRUNCATE TABLE validation.new_store_in_dim_store;
INSERT INTO validation.new_store_in_dim_store
SELECT
    ds.store_id,
    ds.store_full_name,
    ds.store_type,
    ds.store_sub_type,
    ds.store_brand,
    ds.store_country,
    ds.store_region
FROM stg.dim_store ds
LEFT JOIN stg.dim_store AT(OFFSET => -24*60*60) AS pds
    ON pds.store_id = ds.store_id
WHERE pds.store_id IS NULL;
