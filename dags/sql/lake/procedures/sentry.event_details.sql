SET low_watermark_datetime = %(low_watermark)s :: TIMESTAMP_LTZ;


CREATE OR REPLACE TEMP TABLE _flattened_events AS
SELECT eventid event_id,
       e.value:data['headers']                      AS headers,
       e.value:data['data']['username']::VARCHAR    AS email,
       contexts['device']['family']::VARCHAR        AS device_family,
       contexts['device']['model']::VARCHAR         AS device_model,
       contexts['browser']['name']::VARCHAR         AS browser,
       contexts['browser']['version']::VARCHAR      AS browser_version,
       contexts['client_os']['name']::VARCHAR       AS os,
       contexts['client_os']['version']::VARCHAR    AS os_version,
       contexts['response']['status_code']::VARCHAR AS status_code,
       contexts['session']['session']::VARCHAR      AS session,
       datereceived,
       starttimestamp,
       endtimestamp,
       convert_timezone('UTC', 'America/Los_Angeles', datereceived) date_received_pst,
       convert_timezone('UTC', 'America/Los_Angeles', to_timestamp(starttimestamp::int)) start_time_pst,
       convert_timezone('UTC', 'America/Los_Angeles', to_timestamp(endtimestamp::int)) end_time_pst,
       type,
       title,
       meta_create_datetime source_meta_create_datetime,
       meta_update_datetime source_meta_update_datetime
FROM lake.sentry.event_details_raw s
   , LATERAL FLATTEN(INPUT => s.entries, outer=>true) e
WHERE s.meta_update_datetime > $low_watermark_datetime;


MERGE INTO lake.sentry.event_details t
USING (
    SELECT *
            ,
           hash(* exclude source_meta_create_datetime, source_meta_update_datetime) meta_row_hash,
           current_timestamp AS meta_create_datetime,
           current_timestamp AS meta_update_datetime
    from _flattened_events
    QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY coalesce(source_meta_update_datetime, '1900-01-01') DESC) = 1
    ) s
ON s.event_id = t.event_id
WHEN NOT MATCHED
THEN INSERT (event_id,headers,email,device_family,device_model,browser,browser_version,os,os_version,status_code,session,
    datereceived,starttimestamp,endtimestamp,date_received_pst,start_time_pst,end_time_pst,type,title,
    source_meta_create_datetime,source_meta_update_datetime,meta_row_hash,meta_create_datetime,meta_update_datetime)
VALUES (event_id,headers,email,device_family,device_model,browser,browser_version,os,os_version,status_code,session,
    datereceived,starttimestamp,endtimestamp,date_received_pst,start_time_pst,end_time_pst,type,title,
    source_meta_create_datetime,source_meta_update_datetime,meta_row_hash,meta_create_datetime,meta_update_datetime)
WHEN MATCHED AND s.meta_row_hash != t.meta_row_hash
THEN UPDATE SET
        t.event_id = s.event_id,
        t.headers = s.headers,
        t.email = s.email,
        t.device_family = s.device_family,
        t.device_model = s.device_model,
        t.browser = s.browser,
        t.browser_version = s.browser_version,
        t.os = s.os,
        t.os_version = s.os_version,
        t.status_code = s.status_code,
        t.session = s.session,
        t.datereceived = s.datereceived,
        t.starttimestamp = s.starttimestamp,
        t.endtimestamp = s.endtimestamp,
        t.date_received_pst = s.date_received_pst,
        t.start_time_pst = s.start_time_pst,
        t.end_time_pst = s.end_time_pst,
        t.type = s.type,
        t.title = s.title,
        t.source_meta_create_datetime = s.source_meta_create_datetime,
        t.source_meta_update_datetime = s.source_meta_update_datetime,
        t.meta_row_hash = s.meta_row_hash,
        t.meta_update_datetime = s.meta_update_datetime;
