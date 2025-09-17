SET low_watermark_ltz = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _hdyh AS
SELECT
    *
FROM
    lake.builder.hdyh h
    WHERE meta_update_datetime > $low_watermark_ltz;

CREATE OR REPLACE TEMP TABLE _parent_data AS
SELECT
    id,
    name,
    modelid                                                              model_id,
    published,
    p.value['globalCode']                                                global_code,
    p.value['label']                                                     label,
    p.value['referrerId']                                                referrer_id,
    variations,
    CONVERT_TIMEZONE('UTC', 'US/Pacific', to_timestamp(createddate))     created_date,
    CONVERT_TIMEZONE('UTC', 'US/Pacific', to_timestamp(lastUpdated))     last_updated,
    CONVERT_TIMEZONE('UTC', 'US/Pacific', to_timestamp(firstPublished))  first_published,
    createdBy as                                                         created_by,
    lastUpdatedBy as                                                     last_updated_by,
    rev,
    CASE WHEN p.value['children'] = [] THEN  array_construct({})
        ELSE  p.value['children'] END                                    children,
FROM _hdyh h,
LATERAL FLATTEN(input => h.data['config']) p;

CREATE OR REPLACE TEMP TABLE _child_data AS
    SELECT
    p.id,
    p.name,
    p.model_id,
    p.published,
    p.global_code,
    p.label,
    p.referrer_id,
    c.value['globalCode'] sub_global_code,
    c.value['label']      sub_label,
    c.value['referrerId'] sub_referrer_id,
    p.variations,
    p.created_date,
    p.last_updated,
    p.first_published,
    p.created_by,
    p.last_updated_by,
    p.rev,
    FROM _parent_data p,
    LATERAL FLATTEN(INPUT => p.children) c;

MERGE INTO lake.builder.hdyh_flattened AS t
USING (
    SELECT *,
           hash(*) meta_row_hash,
           current_timestamp() meta_create_datetime,
           current_timestamp() meta_update_datetime
    FROM _child_data
    ) s
ON equal_null(t.id, s.id)
    AND equal_null(t.global_code, s.global_code)
    AND equal_null(t.sub_global_code, s.sub_global_code)
WHEN NOT MATCHED
    THEN INSERT (id, name, model_id, published, global_code, label, referrer_id, sub_global_code, sub_label,
                 sub_referrer_id, variations, created_date, last_updated, first_published, created_by, last_updated_by,
                 rev ,meta_row_hash,meta_create_datetime,meta_update_datetime)
         VALUES (id, name, model_id, published, global_code, label, referrer_id, sub_global_code, sub_label,
                 sub_referrer_id, variations, created_date, last_updated, first_published, created_by, last_updated_by,
                 rev ,meta_row_hash,meta_create_datetime,meta_update_datetime)
WHEN MATCHED AND t.meta_row_hash!=s.meta_row_hash
    THEN UPDATE
    SET
        t.id=s.id,
        t.name=s.name,
        t.model_id=s.model_id,
        t.published=s.published,
        t.label=s.label,
        t.referrer_id=s.referrer_id,
        t.sub_label=s.sub_label,
        t.sub_referrer_id=s.sub_referrer_id,
        t.variations=s.variations,
        t.created_date=s.created_date,
        t.last_updated=s.last_updated,
        t.first_published=s.first_published,
        t.created_by=s.created_by,
        t.last_updated_by=s.last_updated_by,
        t.rev=s.rev,
        t.meta_row_hash=s.meta_row_hash,
        t.meta_update_datetime=s.meta_update_datetime;
