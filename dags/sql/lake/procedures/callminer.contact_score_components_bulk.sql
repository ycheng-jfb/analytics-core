CREATE TABLE IF NOT EXISTS lake.callminer.contact_score_components_new (
    contact_id int,
    contact_type varchar,
    attributes_udf_text_21 varchar,
    others_udf_date_02 TIMESTAMP_LTZ(3),
    others_udf_text_01 varchar,
    call_id int,
    score_id int,
    score_components_id int,
    score_components_name varchar,
    weight float,
    META_UPDATE_DATETIME TIMESTAMP_LTZ(3),
    PRIMARY KEY (contact_id, call_id, score_id, score_components_id)
);


CREATE OR REPLACE TEMPORARY TABLE _callminer_contact_score_components AS (
    select
        contact_id,
        contact_type,
        attributes_udf_text_21,
        others_udf_date_02,
        others_udf_text_01,
        s.contactid as call_id,
        s.ScoreID as score_id,
        s.ScoreComponentID as score_components_id,
        iff(s.ComponentFullName = '', Null, s.ComponentFullName) as score_components_name,
        s.VALUE as weight,
        c.META_UPDATE_DATETIME as META_UPDATE_DATETIME
    from lake.callminer.contacts_new c
    join lake.callminer.score_indicators_new s
    on c.contact_id = s.contactid
    where c.META_UPDATE_DATETIME = (select max(META_UPDATE_DATETIME) from lake.callminer.contacts_new)
);

DELETE FROM lake.callminer.contact_score_components_new csc
WHERE contact_id IN (SELECT DISTINCT contact_id FROM _callminer_contact_score_components);

MERGE INTO lake.callminer.contact_score_components_new t
USING (
    SELECT * FROM _callminer_contact_score_components
) s ON equal_null(t.contact_id, s.contact_id)
    AND equal_null(t.call_id, s.call_id)
    AND equal_null(t.score_id, s.score_id)
    AND equal_null(t.score_components_id, s.score_components_id)
WHEN NOT MATCHED THEN INSERT (
    contact_id,
    contact_type,
    attributes_udf_text_21,
    others_udf_date_02,
    others_udf_text_01,
    call_id,
    score_id,
    score_components_id,
    score_components_name,
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
    score_id,
    score_components_id,
    score_components_name,
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
    t.score_id = s.score_id,
    t.score_components_id = s.score_components_id,
    t.score_components_name = s.score_components_name,
    t.weight = s.weight,
    t.META_UPDATE_DATETIME = s.META_UPDATE_DATETIME;
