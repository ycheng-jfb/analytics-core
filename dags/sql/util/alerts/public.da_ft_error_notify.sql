CREATE OR REPLACE ALERT UTIL.PUBLIC.DA_FT_ERROR_NOTIFY
    WAREHOUSE = 'DA_WH_ETL_LIGHT',
    SCHEDULE = 'USING CRON 50 */4 * * * UTC'
    IF (EXISTS(
        with errors as (
            select distinct connector_id,sync_id
            from LAKE_VIEW.FIVETRAN_LOG.LOG l
            where event <> 'INFO'
            and time_stamp >= DATEADD(HOUR, -10, SYSDATE())
            ),
        final as (
            select distinct c.connector_name,c.connector_type_id,
                        l.sync_id,last_value(message_event) over(PARTITION BY l.sync_id order by time_stamp) last_message_event
                        ,last_value(message_data) over(PARTITION BY l.sync_id order by time_stamp) last_message_data
                        ,last_value(event) over(PARTITION BY l.sync_id order by time_stamp) last_event
                        ,case when last_message_event = 'sync_end' and last_event = 'INFO' then 'completed'
                                when last_message_event = 'sync_end' and last_event in ('WARNING', 'SEVERE') then 'completed_with_warning'
                        else 'notcompleted' end as sync_status
            from LAKE_VIEW.FIVETRAN_LOG.LOG l
            join errors e on e.sync_id = l.sync_id
            join lake_view.fivetran_log.connector c on c.connector_id = l.connector_id
            )
        select *
        from final
        where sync_status <> 'completed' or connector_type_id not in ('google_ads', 'google_search_ads_360', 'snapchat_ads', 'tiktok_ads', 'twitter_ads')
        ))
    THEN
        CALL SYSTEM$SEND_EMAIL(
        'EMAIL_NOTIFICATION_FT_ERRORS',
        ('fivetran-alerts-aaaaiwznmu6gj3ft3qed4uvk3u@techstyle.slack.com'),
        'NOTIFICATION: Found Errors in FiveTran Log',
        'Found Warning/Error logs in the FiveTran Log'
    )
;

ALTER ALERT UTIL.PUBLIC.DA_FT_ERROR_NOTIFY RESUME;
