CREATE OR REPLACE VIEW LAKE_VIEW.GPT.OPENPATH_RAW_DATA AS
 SELECT user,
     unlock_attempts,
     success_count,
     denied_count,
     success_rate_ as success_rate,
     last_attempt,
     CASE
         WHEN last_attempt LIKE 'Yesterday%' THEN
             REGEXP_SUBSTR(last_attempt, '\\d{1,2}:\\d{2}:\\d{2} [APap][Mm]')
         ELSE
             REGEXP_SUBSTR(last_attempt, '\\d{1,2}:\\d{2}:\\d{2} [APap][Mm]', 1, 1)
     END AS extracted_time,
     replace(_file, '.csv') as file_date,
     _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
 FROM lake_fivetran.gpt_sharepoint_openpath_v1.openpath_raw_data;
