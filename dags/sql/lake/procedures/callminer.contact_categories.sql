CREATE TABLE IF NOT EXISTS lake.callminer.contact_categories (
    contact_id int,
    contact_type varchar,
    attributes_udf_text_21 varchar,
    others_udf_date_02 TIMESTAMP_LTZ(3),
    others_udf_text_01 varchar,
    call_id int,
    bucket_id int,
    bucket_full_name varchar,
    section_id int,
    weight int,
    META_UPDATE_DATETIME TIMESTAMP_LTZ(3),
    PRIMARY KEY (contact_id, call_id, bucket_id, section_id)
);

CREATE TEMP TABLE _callminer_contact_categories AS (
    SELECT
        contact_id,
        contact_type,
        attributes_udf_text_21,
        others_udf_date_02,
        others_udf_text_01,
        ca.value['CallId'] as call_id,
        ca.value['BucketId'] as bucket_id,
        iff(ca.value['BucketFullName'] = '', Null, ca.value['BucketFullName']) as bucket_full_name,
        ca.value['SectionId'] as section_id,
        iff(ca.value['Weight'] = '', Null, ca.value['Weight']) as weight,
        META_UPDATE_DATETIME as META_UPDATE_DATETIME
    FROM lake.callminer.contacts co,
    LATERAL FLATTEN (input => categories) ca
    WHERE META_UPDATE_DATETIME = (SELECT max(META_UPDATE_DATETIME) FROM lake.callminer.contacts)
);

DELETE FROM lake.callminer.contact_categories cc
WHERE contact_id IN (SELECT DISTINCT contact_id FROM _callminer_contact_categories);

MERGE INTO lake.callminer.contact_categories t
USING (
    select * from _callminer_contact_categories
) s ON equal_null(t.contact_id, s.contact_id)
    AND equal_null(t.call_id, s.call_id)
    AND equal_null(t.bucket_id, s.bucket_id)
    AND equal_null(t.section_id, s.section_id)
WHEN NOT MATCHED THEN INSERT (
    contact_id,
    contact_type,
    attributes_udf_text_21,
    others_udf_date_02,
    others_udf_text_01,
    call_id,
    bucket_id,
    bucket_full_name,
    section_id,
    weight,
    META_UPDATE_DATETIME
)
VALUES (
    contact_id,
    contact_type,
    attributes_udf_text_21,
    others_udf_date_02,
    others_udf_text_01,
    call_id,
    bucket_id,
    bucket_full_name,
    section_id,
    weight,
    META_UPDATE_DATETIME
)
WHEN MATCHED THEN UPDATE
SET t.contact_id = s.contact_id,
    t.contact_type = s.contact_type,
    t.attributes_udf_text_21 = s.attributes_udf_text_21,
    t.others_udf_date_02 = s.others_udf_date_02,
    t.others_udf_text_01 = s.others_udf_text_01,
    t.call_id = s.call_id,
    t.bucket_id = s.bucket_id,
    t.bucket_full_name = s.bucket_full_name,
    t.section_id = s.section_id,
    t.weight = s.weight,
    t.META_UPDATE_DATETIME = s.META_UPDATE_DATETIME;
