CREATE OR REPLACE VIEW LAKE_VIEW.STACKADAPT.ADVERTISER AS
select id as advertiser_id,
    name as advertiser_name,
    CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
from LAKE_FIVETRAN.MED_STACKADAPT_V1.ADVERTISER;
