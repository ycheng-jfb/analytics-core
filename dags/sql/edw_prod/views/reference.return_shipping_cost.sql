CREATE VIEW IF NOT EXISTS reference.return_shipping_cost AS
SELECT
    CASE
        WHEN store_id = 116 THEN -26
        ELSE store_id
    END AS Store_id,
    store_full_name AS store_name,
    store_country,
    store_region,
    store_type,
    CASE
        WHEN (store_full_name ILIKE 'Lingerie%' OR store_brand ILIKE 'Savage%') AND upper(store_region) = 'NA'
            THEN to_number(5.0831, 13, 4)
         WHEN store_full_name ILIKE 'Lingerie%' OR store_brand ILIKE 'Savage%' THEN to_number(4.7919, 13, 4)
         WHEN upper(store_region) = 'EU' AND store_brand ILIKE 'JustFab%' THEN to_number(5.3430, 13, 4)
         WHEN upper(store_region) = 'EU' AND store_brand ILIKE 'Fabletics%' THEN to_number(4.7919, 13, 4)
         WHEN store_full_name ILIKE 'Fabkids%' THEN to_number(4.50, 13, 4)
         WHEN store_full_name ILIKE 'ShoeDazzle%' THEN to_number(5.37, 13, 4)
         WHEN upper(store_Country) = 'US' AND (store_full_name ILIKE 'JustFab%' OR store_full_name ILIKE 'PS%') THEN to_number(5.60, 13, 4)
         WHEN upper(store_Country) = 'US' AND store_full_name ILIKE 'Fabletics%' THEN to_number(4.50, 13, 4)
         WHEN upper(store_Country) = 'CA' AND store_full_name ILIKE 'JustFab%' THEN to_number(8.01, 13, 4)
         WHEN upper(store_Country) = 'CA' AND store_full_name ILIKE 'Fabletics%' THEN to_number(8.46, 13, 4)
    ELSE 0
    END AS cost_per_return_shipment,
    CASE
         WHEN upper(store_region) = 'EU' THEN 'EUR'
         WHEN upper(store_region) = 'NA' THEN 'USD'
    END AS currency_code
FROM stg.dim_store
WHERE store_id != 0
AND is_current = 1;
