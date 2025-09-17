TRUNCATE TABLE reference.yitty_fl_lead_fix_20240304;

INSERT INTO reference.yitty_fl_lead_fix_20240304
    (customer_id, membership_id, store_id)
SELECT DISTINCT m.customer_id, m.membership_id, sfr.properties_store_id
FROM lake_consolidated.ultra_merchant.membership AS m
LEFT JOIN lake_consolidated.ultra_merchant.membership_brand_signup AS mbs
        ON m.membership_signup_id = mbs.membership_signup_id
LEFT JOIN lake_consolidated.ultra_merchant.membership_plan_membership_brand AS mpmb
        ON mpmb.membership_brand_id = mbs.membership_brand_id
         JOIN lake.segment_fl.java_fabletics_complete_registration sfr
              ON edw_prod.stg.udf_unconcat_brand(m.customer_id) = sfr.properties_customer_id AND
                 CAST(m.datetime_added AS DATE) >= '2024-03-04' AND
                 CAST(m.datetime_added AS DATE) <= '2024-03-27' AND
                 COALESCE(mpmb.store_id,m.store_id) in (52,241)
               AND COALESCE(mpmb.store_id,m.store_id) <> sfr.properties_store_id;
