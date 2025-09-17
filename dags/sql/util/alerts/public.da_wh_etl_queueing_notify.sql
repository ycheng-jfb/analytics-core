CREATE OR REPLACE ALERT UTIL.PUBLIC.DA_WH_ETL_QUEUEING_NOTIFY
    WAREHOUSE = 'DA_WH_ETL_LIGHT',
    SCHEDULE = 'USING CRON 0,10,20,30,40,50 * * * * UTC'
    IF (EXISTS(
        select COUNT(START_TIME) AS START_TIME
        from table(information_schema.warehouse_load_history(date_range_start=>dateadd('hour',0,current_timestamp()),warehouse_name=>'DA_WH_ETL_LIGHT'))
        WHERE AVG_QUEUED_LOAD > 0
        HAVING COUNT(START_TIME) >= 90
        UNION ALL
        select COUNT(START_TIME) AS START_TIME
        from table(information_schema.warehouse_load_history(date_range_start=>dateadd('hour',0,current_timestamp()),warehouse_name=>'DA_WH_ETL'))
        WHERE AVG_QUEUED_LOAD > 0
        HAVING COUNT(START_TIME) >= 90
        UNION ALL
        select COUNT(START_TIME) AS START_TIME
        from table(information_schema.warehouse_load_history(date_range_start=>dateadd('hour',0,current_timestamp()),warehouse_name=>'DA_WH_ETL_HEAVY'))
        WHERE AVG_QUEUED_LOAD > 0
        HAVING COUNT(START_TIME) >= 90
        UNION ALL
        select COUNT(START_TIME) AS START_TIME
        from table(information_schema.warehouse_load_history(date_range_start=>dateadd('hour',0,current_timestamp()),warehouse_name=>'DA_WH_ADHOC_LARGE'))
        WHERE AVG_QUEUED_LOAD > 0
        HAVING COUNT(START_TIME) >= 90
    ))
    THEN
    CALL SYSTEM$SEND_EMAIL(
        'EMAIL_NOTIFICATION_WH_QUEUEING',
        ('RPOORNIMA@TECHSTYLE.com, SAVANGALA@TECHSTYLE.com, RTANNEERU@TECHSTYLE.com'),
        'NOTIFICATION: ETL Warehouse Queueing',
        'Queueing has occurred on one or more of the ETL Warehouses in the last 10 minutes.'
    )
;

ALTER ALERT UTIL.PUBLIC.DA_WH_ETL_QUEUEING_NOTIFY RESUME;
