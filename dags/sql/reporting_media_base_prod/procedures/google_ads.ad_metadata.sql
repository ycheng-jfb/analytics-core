/*
 Stored missing/disabled ad ids metadata into MED_DB_STAGING.GOOGLE_ADS.AD_METADATA_HISTORY.
 Processed the missing data(MED_DB_STAGING.GOOGLE_ADS.AD_METADATA_HISTORY) into med_db_cleansed.google_ads.ad_metadata.
 We need to make sure to load the missing ad ids metadata from MED_DB_STAGING.GOOGLE_ADS.AD_METADATA_HISTORY whenever we
 do a full reprocess on med_db_cleansed.google_ads.ad_metadata
do not run this if med_db_cleansed.google_ads.ad_metadata has data
med_db_cleansed.google_ads.ad_metadata grain is different and MED_DB_STAGING.GOOGLE_ADS.AD_METADATA_HISTORY grain is different

 */

set max_update_datetime = (select max(meta_update_datetime) from reporting_media_base_prod.google_ads.ad_metadata);


CREATE OR REPLACE TEMPORARY TABLE _labels AS (
select ad.ad_id,
        ad.ad_group_id,
        array_agg(l.name)::string labels
 from lake_view.google_ads.ad_metadata ad
 join lake_view.google_ads.ad_label_history adl on ad.ad_id = adl.ad_id and ad.ad_group_id = adl.ad_group_id
 join lake_view.google_ads.label l on l.id = adl.label_id
    where ad.meta_update_datetime >= $max_update_datetime
 group by ad.ad_id,ad.ad_group_id
);

create table if not exists reporting_media_base_prod.google_ads.ad_metadata
(
    ad_id               int,
    ad_group_id               int,
    ad_type             varchar,
    image_creative_name varchar,
    labels              varchar,
    status              varchar,
    meta_row_hash       int,
    meta_create_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3)
);


-- Merge Statement
merge into reporting_media_base_prod.google_ads.ad_metadata t
    using (select *
           from (select am.ad_id,
                     am.ad_group_id,
                     ad_type,
                     image_creative_name,
                     l.labels,
                     status,
                     hash(am.ad_id, am.ad_group_id, ad_type, image_creative_name, l.labels, status) AS meta_row_hash,
                     current_timestamp AS meta_create_datetime,
                     current_timestamp AS meta_update_datetime,
                     row_number() over (partition by am.ad_id order by meta_update_datetime desc) as rn_ad
                 from lake_view.google_ads.ad_metadata am
                          left join _labels as l on l.ad_id = am.ad_id and l.ad_group_id = am.ad_group_id
                 where am.meta_update_datetime >= $max_update_datetime
                 ) tmp
           where tmp.rn_ad = 1) s
    on equal_null(t.ad_id, s.ad_id) AND
        equal_null(t.ad_group_id, s.ad_group_id)
    when not matched THEN insert (ad_id, ad_group_id, ad_type, image_creative_name, labels, status, meta_row_hash, meta_create_datetime, meta_update_datetime)
        values (ad_id, ad_group_id, ad_type, image_creative_name, labels, status, meta_row_hash, meta_create_datetime, meta_update_datetime)
    when matched and t.meta_row_hash != s.meta_row_hash
        then update set t.ad_id = s.ad_id,
                      t.ad_group_id = s.ad_group_id,
                      t.ad_type = s.ad_type,
                      t.image_creative_name = s.image_creative_name,
                      t.labels = s.labels,
                      t.status = s.status,
                      t.meta_row_hash = s.meta_row_hash,
                      t.meta_update_datetime = s.meta_update_datetime;
;
