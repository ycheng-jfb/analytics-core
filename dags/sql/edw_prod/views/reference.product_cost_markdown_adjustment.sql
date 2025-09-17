CREATE VIEW IF NOT EXISTS reference.product_cost_markdown_adjustment AS
SELECT
    store_id,
	store_full_name AS store_name,
    store_country,
    store_region,
    store_type,
    CASE
            WHEN upper(store_brand_abbr) = 'SX' THEN to_number(2.50, 13,4)
            ELSE to_number(1.025, 13,4)
    END AS markdown_adjustment_factor
FROM stg.dim_store
WHERE
    store_id != 0
    AND is_current = 1;
