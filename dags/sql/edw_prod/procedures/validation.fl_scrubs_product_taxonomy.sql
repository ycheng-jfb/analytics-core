TRUNCATE TABLE validation.fl_scrubs_product_taxonomy;
INSERT INTO validation.fl_scrubs_product_taxonomy
(product_id,
 meta_original_product_id,
 master_product_id,
 store_id,
 product_alias,
 product_sku,
 sku,
 product_name,
 department,
 category,
 current_showroom_date,
 meta_create_datetime,
 meta_update_datetime)
SELECT
       product_id,
       meta_original_product_id,
       master_product_id,
       store_id,
       product_alias,
       product_sku,
       sku,
       product_name,
       department,
       category,
       current_showroom_date,
       current_timestamp,
       current_timestamp
FROM stg.dim_product
WHERE department ilike 'scrubs%'
AND product_type <> 'Customization'
AND product_name NOT ILIKE '%embroidery%'
AND product_name NOT IN ('Classic Work Clog','Classic Clog 9', 'Fabletics Jibbitz™', 'Classic Clog + Fabletics Jibbitz™','Classic Work Clog + Fabletics Jibbitz™') -- Unisex
AND product_id NOT IN (
    1360284720, --test
    1332870420, -- recovery:set up incorrectly
    1332899220 -- recovery: setup incorrectly
    )
AND SUBSTRING(department, CHARINDEX(' ', department) +1, 20) NOT IN ('Mens','Womens')
AND product_status = 'Active'
and current_showroom_date < current_date;
