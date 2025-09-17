CREATE OR REPLACE VIEW LAKE_VIEW.ULTRA_MERCHANT.MEMBERSHIP_DETAIL AS
SELECT
    membership_detail_hash_id,
    membership_detail_id,
    membership_id,
    name,
    value,
    datetime_added,
    datetime_modified,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant.membership_detail s
WHERE NOT exists(
        SELECT
            1
        FROM lake_archive.ultra_merchant_cdc.membership_detail__del cd
        WHERE cd.membership_detail_hash_id = s.membership_detail_hash_id
    );
