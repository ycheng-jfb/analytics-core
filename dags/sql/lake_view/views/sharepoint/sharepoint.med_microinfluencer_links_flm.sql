CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_MICROINFLUENCER_LINKS_FLM as
SELECT status,
        influencer_name,
        vanity_url,
        medium,
        source,
        NULL as campaign_irmp,
        unique_id_ as unique_id,
        NULL as ir_ad,
        NULL as content,
        shared_id,
        gateway,
        utm,
        link,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from LAKE_FIVETRAN.med_sharepoint_influencer_v1.fl_men_vanity_links_promoter_links
where influencer_name is not null;
