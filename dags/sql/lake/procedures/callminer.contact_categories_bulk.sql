CREATE TABLE IF NOT EXISTS lake.callminer.contact_categories_new (
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


CREATE or REPLACE TEMPORARY TABLE _categories_components_new_with_weight as
(
    with weight_cte as
    (
       select ContactID,
           CategoryID,
           componentid,
           similarityscore,
           CASE WHEN similarityscore = 1 THEN sum(weight)
                WHEN similarityscore is Null THEN sum(weight)/count(COMPONENTID)
                ELSE 0 END as weight
       from lake.callminer.categories_components_new
       group by 1, 2, 3, 4
    )
    select ContactID,
        CategoryID,
        sum(weight) as weight
    from weight_cte
    group by 1, 2
);

CREATE OR REPLACE TEMPORARY TABLE _callminer_contact_categories AS (
    select cb.contact_id,
        cb.contact_type,
        cb.attributes_udf_text_21,
        cb.others_udf_date_02,
        cb.others_udf_text_01,
        cab.contactid as call_id,
        cab.CategoryID as bucket_id,
        cab.categoryfullname as bucket_full_name,
        cab.sectionid as section_id,
        ccb.Weight,
        cb.META_UPDATE_DATETIME as META_UPDATE_DATETIME
    from lake.callminer.contacts_new cb
    join lake.callminer.categories_new cab
        on cb.contact_id = cab.contactid
    join _categories_components_new_with_weight ccb
        on cab.contactid = ccb.contactid
        and cab.CategoryID = ccb.CategoryID
    WHERE cb.META_UPDATE_DATETIME = (SELECT max(META_UPDATE_DATETIME) FROM lake.callminer.contacts_new)
);


DELETE FROM lake.callminer.contact_categories_new cc
WHERE contact_id IN (SELECT DISTINCT contact_id FROM _callminer_contact_categories);

MERGE INTO lake.callminer.contact_categories_new t
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
