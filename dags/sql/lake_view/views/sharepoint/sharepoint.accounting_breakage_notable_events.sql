CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.ACCOUNTING_BREAKAGE_NOTABLE_EVENTS AS
SELECT brand,
       recognition_month::date as recognition_month,
       analysis_url,
       notable_events,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
 FROM lake_fivetran.fpa_nonconfidential_sharepoint_v1.accounting_breakage_notable_events_sheet_1;

