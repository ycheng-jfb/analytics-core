TRUNCATE TABLE validation.new_membership_brand;

INSERT INTO validation.new_membership_brand
SELECT membership_brand_id,
       label,
       datetime_added,
       meta_create_datetime
FROM lake_consolidated.ultra_merchant.membership_brand
WHERE meta_create_datetime::DATE = CURRENT_DATE
   OR DATEADD(DAY, 1, meta_create_datetime::DATE) = CURRENT_DATE;
