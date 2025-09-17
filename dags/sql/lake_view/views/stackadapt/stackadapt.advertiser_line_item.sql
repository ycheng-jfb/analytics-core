CREATE OR REPLACE VIEW LAKE_VIEW.STACKADAPT.ADVERTISER_LINE_ITEM AS
select advertiser_id,
    line_item_id,
    CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
from LAKE_FIVETRAN.MED_STACKADAPT_V1.ADVERTISER_LINE_ITEM;
