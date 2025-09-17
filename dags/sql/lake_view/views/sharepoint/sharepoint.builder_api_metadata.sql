create or replace view lake_view.sharepoint.builder_api_metadata as
select brand,
       content_type,
       published,
       builder_id,
       builder_page_url,
    domain,
    site_country,
    adjusted_activated_datetime_utc,
    adjusted_activated_datetime_pst,
    test_Ratio as testRatio,
    test_split,
    test_name,
    test_variation_name,
    test_variation_id,
    assignment,
    psources,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
from lake_fivetran.webanalytics_sharepoint_v1.builder_api_metadata_builder_metadata_api
;
