SET low_watermark_datetime = %(low_watermark)s :: TIMESTAMP_LTZ;

INSERT INTO REPORTING_PROD.SPS.MILESTONE_PROCESSOR
    (
        SELECT xmlget(milestones.value, 'billOfLading'):"$"::varchar       AS ship_bol,
               xmlget(milestones.value, 'containerID'):"$"::varchar        AS container_label,
               xmlget(milestones.value, 'statusCode'):"$"::varchar         AS status_code,
               xmlget(milestones.value, 'statusDate'):"$"::varchar         AS status_date,
               xmlget(milestones.value, 'statusTime'):"$"::varchar         AS status_time,
               concat(xmlget(milestones.value, 'statusDate'):"$"::varchar,
                      xmlget(milestones.value, 'statusTime'):"$"::varchar) as status_datetime,
               file_name                                                   AS file_name,
               CURRENT_TIMESTAMP()                                         as meta_create_datetime
        FROM LAKE.SPS.MILESTONE as xml,
             lateral flatten(xml.XML_DATA:"$") milestones
        WHERE milestones.VALUE like '<milestone>%%'
          AND meta_create_datetime > $low_watermark_datetime
    )
