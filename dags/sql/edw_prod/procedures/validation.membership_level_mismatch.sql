TRUNCATE TABLE validation.membership_level_mismatch;
INSERT INTO validation.membership_level_mismatch
SELECT
    c.customer_id,
    c.membership_level AS edw_membership_level,
    sml.label AS src_membership_level
FROM stg.dim_customer AS c
    JOIN lake_consolidated.ultra_merchant.customer AS sc
        ON sc.customer_id = c.customer_id
    LEFT JOIN lake_consolidated.ultra_merchant.membership AS sm
        ON sm.customer_id = sc.customer_id
        AND sm.store_id = sc.store_id
    LEFT JOIN lake_consolidated.ultra_merchant.membership_level AS sml
        ON sml.membership_level_id = sm.membership_level_id
WHERE c.membership_level <> sml.label
    AND c.meta_update_datetime > DATEADD(HOUR, 4, sm.meta_update_datetime) /* offset by 4 hours due to volatile source data */
ORDER BY c.customer_id;
