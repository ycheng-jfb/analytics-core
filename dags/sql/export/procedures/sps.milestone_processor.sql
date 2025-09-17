select mp.ship_bol as ship_bol,
       mp.container_label as container_label,
       mcr.milestone_code_id as milestone_status,
       TO_CHAR(TO_TIMESTAMP(STATUS_DATETIME,'YYYYMMDDHHMI')) as milestone_datetime
from  REPORTING_PROD.sps.milestone_processor as mp
JOIN  lake_view.ultra_warehouse.in_transit_milestone_cross_reference AS mcr
ON mp.status_code= mcr.STATUS_CODE
WHERE mcr.CARRIER_ID = 23 and mp.meta_create_datetime > %(low_watermark)s
