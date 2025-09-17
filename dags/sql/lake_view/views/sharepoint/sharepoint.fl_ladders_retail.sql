CREATE OR REPLACE VIEW lake_view.sharepoint.fl_ladders_retail AS
SELECT
       cc,
       channel,
       region,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_inbound_sharepoint_v1.planning_ladder_ccs_retail_na;
