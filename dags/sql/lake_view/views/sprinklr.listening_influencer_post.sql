CREATE OR REPLACE VIEW lake_view.sprinklr.listening_influencer_post AS
select src.*
from lake_view.sprinklr.listening_streams src
 ,lateral flatten(input => topic_ids) vm
where value ILIKE '6011b447a2edc257fa7ce07f';
