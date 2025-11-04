-- 将历史数据和未迁移站点数据写入
-- 首次写入时写入所有数据，之后写入排除一切换站点
-- fact_activation
MERGE INTO EDW_PROD.NEW_STG.FACT_ACTIVATION tgt
USING (
SELECT
    fa.activation_key,
    fa.membership_event_key,
    fa.store_id,
    EDW_PROD.stg.udf_unconcat_brand(fa.customer_id) AS customer_id,
    fa.activation_local_datetime,
    fa.next_activation_local_datetime,
    fa.source_activation_local_datetime,
    fa.source_next_activation_local_datetime,
    fa.cancellation_local_datetime,
    fa.is_reactivated_vip,
    EDW_PROD.stg.udf_unconcat_brand(fa.order_id) AS order_id,
    EDW_PROD.stg.udf_unconcat_brand(fa.session_id) AS session_id,
    fa.activation_channel_type,
    fa.activation_channel,
    fa.activation_subchannel,
    fa.vip_cohort_month_date,
    fa.source_vip_cohort_month_date,
    fa.membership_event_type,
    fa.membership_type,
    fa.is_vip_activation_from_reactivated_lead,
    fa.dm_gateway_id,
    fa.device,
    fa.browser,
    fa.operating_system,
    fa.utm_medium,
    fa.utm_source,
    fa.utm_campaign,
    fa.utm_content,
    fa.utm_term,
    fa.pcode,
    fa.ccode,
    fa.acode,
    fa.scode,
    fa.sub_store_id,
    dss.store_type,
    dss.store_sub_type,
    IFF(dss.store_brand IN ('Fabletics','Savage X') AND dss.store_type = 'Retail', TRUE, FALSE) AS is_retail_vip,
    IFF(dss.store_brand = 'Fabletics' AND dss.store_sub_type = 'Varsity', TRUE, FALSE) AS is_retail_varsity_vip,
    IFF(dss.store_brand = 'Fabletics' AND dss.store_type = 'Legging Bar', TRUE, FALSE) AS is_retail_legging_bar_vip,
    IFF(dss.store_type = 'Mobile App', TRUE, FALSE) AS is_mobile_app_vip,
    fa.is_legging_bar_vip,
    fa.is_kiosk_vip,
    fa.is_scrubs_vip,
    fa.order_discount_percent,
    fa.is_current,
    fa.cancel_type,
    fa.cancel_method,
    fa.cancel_reason,
    fa.activation_sequence_number,
    fa.meta_create_datetime,
    fa.meta_update_datetime
FROM EDW_PROD.stg.fact_activation AS fa
    LEFT JOIN EDW_PROD.stg.dim_store AS ds
        ON ds.store_id = fa.store_id
    LEFT JOIN EDW_PROD.stg.dim_store AS dss
        ON dss.store_id = fa.sub_store_id
WHERE NOT fa.is_deleted
    AND NOT NVL(fa.is_test_customer, FALSE)
    AND ds.store_brand NOT IN ('Legacy')
    AND (substring(fa.customer_id, -2) = '10' OR fa.customer_id = -1)
    and fa.store_id not in (26,55)
) src ON tgt.activation_key = src.activation_key
WHEN MATCHED THEN UPDATE SET
    tgt.ACTIVATION_KEY = src.ACTIVATION_KEY,
    tgt.MEMBERSHIP_EVENT_KEY = src.MEMBERSHIP_EVENT_KEY,
    tgt.STORE_ID = src.STORE_ID,
    tgt.CUSTOMER_ID = src.CUSTOMER_ID,
    tgt.ACTIVATION_LOCAL_DATETIME = src.ACTIVATION_LOCAL_DATETIME,
    tgt.NEXT_ACTIVATION_LOCAL_DATETIME = src.NEXT_ACTIVATION_LOCAL_DATETIME,
    tgt.SOURCE_ACTIVATION_LOCAL_DATETIME = src.SOURCE_ACTIVATION_LOCAL_DATETIME,
    tgt.SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME = src.SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME,
    tgt.CANCELLATION_LOCAL_DATETIME = src.CANCELLATION_LOCAL_DATETIME,
    tgt.IS_REACTIVATED_VIP = src.IS_REACTIVATED_VIP,
    tgt.ORDER_ID = src.ORDER_ID,
    tgt.SESSION_ID = src.SESSION_ID,
    tgt.ACTIVATION_CHANNEL_TYPE = src.ACTIVATION_CHANNEL_TYPE,
    tgt.ACTIVATION_CHANNEL = src.ACTIVATION_CHANNEL,
    tgt.ACTIVATION_SUBCHANNEL = src.ACTIVATION_SUBCHANNEL,
    tgt.VIP_COHORT_MONTH_DATE = src.VIP_COHORT_MONTH_DATE,
    tgt.SOURCE_VIP_COHORT_MONTH_DATE = src.SOURCE_VIP_COHORT_MONTH_DATE,
    tgt.MEMBERSHIP_EVENT_TYPE = src.MEMBERSHIP_EVENT_TYPE,
    tgt.MEMBERSHIP_TYPE = src.MEMBERSHIP_TYPE,
    tgt.IS_VIP_ACTIVATION_FROM_REACTIVATED_LEAD = src.IS_VIP_ACTIVATION_FROM_REACTIVATED_LEAD,
    tgt.DM_GATEWAY_ID = src.DM_GATEWAY_ID,
    tgt.DEVICE = src.DEVICE,
    tgt.BROWSER = src.BROWSER,
    tgt.OPERATING_SYSTEM = src.OPERATING_SYSTEM,
    tgt.UTM_MEDIUM = src.UTM_MEDIUM,
    tgt.UTM_SOURCE = src.UTM_SOURCE,
    tgt.UTM_CAMPAIGN = src.UTM_CAMPAIGN,
    tgt.UTM_CONTENT = src.UTM_CONTENT,
    tgt.UTM_TERM = src.UTM_TERM,
    tgt.PCODE = src.PCODE,
    tgt.CCODE = src.CCODE,
    tgt.ACODE = src.ACODE,
    tgt.SCODE = src.SCODE,
    tgt.SUB_STORE_ID = src.SUB_STORE_ID,
    tgt.STORE_TYPE = src.STORE_TYPE,
    tgt.STORE_SUB_TYPE = src.STORE_SUB_TYPE,
    tgt.IS_RETAIL_VIP = src.IS_RETAIL_VIP,
    tgt.IS_RETAIL_VARSITY_VIP = src.IS_RETAIL_VARSITY_VIP,
    tgt.IS_RETAIL_LEGGING_BAR_VIP = src.IS_RETAIL_LEGGING_BAR_VIP,
    tgt.IS_MOBILE_APP_VIP = src.IS_MOBILE_APP_VIP,
    tgt.IS_LEGGING_BAR_VIP = src.IS_LEGGING_BAR_VIP,
    tgt.IS_KIOSK_VIP = src.IS_KIOSK_VIP,
    tgt.IS_SCRUBS_VIP = src.IS_SCRUBS_VIP,
    tgt.ORDER_DISCOUNT_PERCENT = src.ORDER_DISCOUNT_PERCENT,
    tgt.IS_CURRENT = src.IS_CURRENT,
    tgt.CANCEL_TYPE = src.CANCEL_TYPE,
    tgt.CANCEL_METHOD = src.CANCEL_METHOD,
    tgt.CANCEL_REASON = src.CANCEL_REASON,
    tgt.ACTIVATION_SEQUENCE_NUMBER = src.ACTIVATION_SEQUENCE_NUMBER,
    tgt.META_CREATE_DATETIME = src.META_CREATE_DATETIME,
    tgt.META_UPDATE_DATETIME = src.META_UPDATE_DATETIME
WHEN NOT MATCHED THEN                
    INSERT (
        ACTIVATION_KEY,
        MEMBERSHIP_EVENT_KEY,
        STORE_ID,
        CUSTOMER_ID,
        ACTIVATION_LOCAL_DATETIME,
        NEXT_ACTIVATION_LOCAL_DATETIME,
        SOURCE_ACTIVATION_LOCAL_DATETIME,
        SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME,
        CANCELLATION_LOCAL_DATETIME,
        IS_REACTIVATED_VIP,
        ORDER_ID,
        SESSION_ID,
        ACTIVATION_CHANNEL_TYPE,
        ACTIVATION_CHANNEL,
        ACTIVATION_SUBCHANNEL,
        VIP_COHORT_MONTH_DATE,
        SOURCE_VIP_COHORT_MONTH_DATE,
        MEMBERSHIP_EVENT_TYPE,
        MEMBERSHIP_TYPE,
        IS_VIP_ACTIVATION_FROM_REACTIVATED_LEAD,
        DM_GATEWAY_ID,
        DEVICE,
        BROWSER,
        OPERATING_SYSTEM,
        UTM_MEDIUM,
        UTM_SOURCE,
        UTM_CAMPAIGN,
        UTM_CONTENT,
        UTM_TERM,
        PCODE,
        CCODE,
        ACODE,
        SCODE,
        SUB_STORE_ID,
        STORE_TYPE,
        STORE_SUB_TYPE,
        IS_RETAIL_VIP,
        IS_RETAIL_VARSITY_VIP,
        IS_RETAIL_LEGGING_BAR_VIP,
        IS_MOBILE_APP_VIP,
        IS_LEGGING_BAR_VIP,
        IS_KIOSK_VIP,
        IS_SCRUBS_VIP,
        ORDER_DISCOUNT_PERCENT,
        IS_CURRENT,
        CANCEL_TYPE,
        CANCEL_METHOD,
        CANCEL_REASON,
        ACTIVATION_SEQUENCE_NUMBER,
        META_CREATE_DATETIME,
        META_UPDATE_DATETIME
        )
        VALUES(
        src.ACTIVATION_KEY,
        src.MEMBERSHIP_EVENT_KEY,
        src.STORE_ID,
        src.CUSTOMER_ID,
        src.ACTIVATION_LOCAL_DATETIME,
        src.NEXT_ACTIVATION_LOCAL_DATETIME,
        src.SOURCE_ACTIVATION_LOCAL_DATETIME,
        src.SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME,
        src.CANCELLATION_LOCAL_DATETIME,
        src.IS_REACTIVATED_VIP,
        src.ORDER_ID,
        src.SESSION_ID,
        src.ACTIVATION_CHANNEL_TYPE,
        src.ACTIVATION_CHANNEL,
        src.ACTIVATION_SUBCHANNEL,
        src.VIP_COHORT_MONTH_DATE,
        src.SOURCE_VIP_COHORT_MONTH_DATE,
        src.MEMBERSHIP_EVENT_TYPE,
        src.MEMBERSHIP_TYPE,
        src.IS_VIP_ACTIVATION_FROM_REACTIVATED_LEAD,
        src.DM_GATEWAY_ID,
        src.DEVICE,
        src.BROWSER,
        src.OPERATING_SYSTEM,
        src.UTM_MEDIUM,
        src.UTM_SOURCE,
        src.UTM_CAMPAIGN,
        src.UTM_CONTENT,
        src.UTM_TERM,
        src.PCODE,
        src.CCODE,
        src.ACODE,
        src.SCODE,
        src.SUB_STORE_ID,
        src.STORE_TYPE,
        src.STORE_SUB_TYPE,
        src.IS_RETAIL_VIP,
        src.IS_RETAIL_VARSITY_VIP,
        src.IS_RETAIL_LEGGING_BAR_VIP,
        src.IS_MOBILE_APP_VIP,
        src.IS_LEGGING_BAR_VIP,
        src.IS_KIOSK_VIP,
        src.IS_SCRUBS_VIP,
        src.ORDER_DISCOUNT_PERCENT,
        src.IS_CURRENT,
        src.CANCEL_TYPE,
        src.CANCEL_METHOD,
        src.CANCEL_REASON,
        src.ACTIVATION_SEQUENCE_NUMBER,
        src.META_CREATE_DATETIME,
        src.META_UPDATE_DATETIME
        )
;







-- 所有新系统的event
CREATE OR REPLACE TEMPORARY TABLE EDW_PROD.NEW_STG._fact_activation__all_events as 
SELECT
    membership_event_key,
    -- meta_original_customer_id,
    customer_id,
    store_id,
    order_id,
    session_id,
    event_start_local_datetime AS activation_local_datetime,
    membership_event_type,
    membership_type_detail,
    is_scrubs_customer,
    LAG(membership_event_type) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS prior_event,
    LEAD(membership_event_type) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS next_event,
    LAG(membership_type_detail) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS prior_event_type,
    LEAD(membership_type_detail) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS next_event_type,
    LAG(event_start_local_datetime) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS prior_activation_local_datetime,
    LEAD(event_start_local_datetime) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS next_activation_local_datetime
FROM EDW_PROD.DATA_MODEL_JFB.FACT_MEMBERSHIP_EVENT
where (STORE_ID = 55 and cast(event_start_local_datetime as date)>='2025-09-15') 
   or (STORE_ID = 26 and cast(event_start_local_datetime as date)>='2025-09-23') 
;
--新系统取消
CREATE OR REPLACE TEMPORARY TABLE EDW_PROD.NEW_STG. _fact_activation__cancel_events as 
SELECT
    membership_event_key,
    customer_id,
    store_id,
    COALESCE(LAG(DATEADD(ms, 1, activation_local_datetime)) OVER (PARTITION BY customer_id
        ORDER BY activation_local_datetime), '1900-01-01') AS effective_from_cancellation_local_datetime,
    activation_local_datetime AS cancellation_local_datetime,
    membership_event_type,
    membership_type_detail
FROM EDW_PROD.NEW_STG._fact_activation__all_events
WHERE membership_event_type = 'Cancellation'
;

-- 更新 cancellation_local_datetime
UPDATE EDW_PROD.NEW_STG.FACT_ACTIVATION AS tgt   
SET  
   tgt.cancellation_local_datetime = src.cancellation_local_datetime,
   tgt.meta_update_datetime = current_date
from (
    select 
         fa.activation_key
        ,min(ace.cancellation_local_datetime) cancellation_local_datetime
    from EDW_PROD.NEW_STG.FACT_ACTIVATION fa 
    inner JOIN EDW_PROD.NEW_STG._fact_activation__cancel_events AS ace
            ON ace.customer_id = fa.customer_id
            AND fa.activation_local_datetime BETWEEN ace.effective_from_cancellation_local_datetime AND ace.cancellation_local_datetime
    where 
       cast(fa.cancellation_local_datetime as date) ='9999-12-31'
    group by fa.activation_key
) as src
where tgt.activation_key = src.activation_key
;

-- 新系统数据
CREATE OR REPLACE TEMPORARY TABLE EDW_PROD.NEW_STG.FACT_ACTIVATION_REPLACE AS
with _fact_activation__activations_base as (
SELECT
    e.membership_event_key,
    -- e.meta_original_customer_id,
    e.customer_id,
    e.store_id,
    e.order_id,
    e.session_id,
    e.membership_event_type,
    e.activation_local_datetime,
    CASE
        WHEN e.next_event = 'Cancellation' THEN e.next_activation_local_datetime
        WHEN e.membership_event_type = 'Activation' AND c.membership_event_type = 'Cancellation' THEN c.cancellation_local_datetime
        ELSE '9999-12-31' END AS cancellation_local_datetime,
    CASE
        WHEN e.next_event = 'Cancellation' THEN e.next_event_type
        WHEN e.membership_event_type = 'Activation' AND c.membership_event_type = 'Cancellation' THEN c.membership_type_detail
        ELSE NULL END AS cancel_type,
    ROW_NUMBER() OVER (PARTITION BY e.customer_id ORDER BY e.activation_local_datetime) AS activation_sequence_number,
    COALESCE(LEAD(e.activation_local_datetime) OVER (PARTITION BY e.customer_id ORDER BY e.activation_local_datetime),'9999-12-31') AS next_activation_local_datetime,
    CASE WHEN COALESCE(LAG(e.activation_local_datetime) OVER (PARTITION BY e.customer_id ORDER BY e.activation_local_datetime),'9999-12-31') != '9999-12-31' THEN TRUE
        ELSE FALSE END AS is_reactivated_vip,
    e.is_scrubs_customer AS is_scrubs_vip
    -- ,
    -- CASE
    --     WHEN e.next_event = 'Cancellation' THEN e.next_cancel_method
    --     WHEN e.membership_event_type = 'Activation' AND c.membership_event_type = 'Cancellation' THEN c.cancel_method
    --     ELSE NULL END AS cancel_method,
    -- CASE
    --     WHEN e.next_event = 'Cancellation' THEN e.next_cancel_reason
    --     WHEN e.membership_event_type = 'Activation' AND c.membership_event_type = 'Cancellation' THEN c.cancel_reason
    --     ELSE NULL END AS cancel_reason
FROM EDW_PROD.NEW_STG._fact_activation__all_events AS e
    LEFT JOIN EDW_PROD.NEW_STG._fact_activation__cancel_events AS c
        ON c.customer_id = e.customer_id
        AND e.activation_local_datetime BETWEEN c.effective_from_cancellation_local_datetime AND c.cancellation_local_datetime
WHERE e.membership_event_type = 'Activation'
    AND e.membership_type_detail != 'Classic'
    -- Ignore Failed Activations and VIP Level Changes
    AND COALESCE(e.next_event,'') NOT ILIKE '%Failed Activation%'
    AND (COALESCE(e.prior_event,'') != 'Activation'
        OR (COALESCE(e.prior_event,'') = 'Activation' AND e.prior_event_type = 'Classic'))
)
, _fact_activation__valid_events AS (
SELECT
    customer_id,
    activation_local_datetime,
    COALESCE(LEAD(activation_local_datetime) OVER (PARTITION BY customer_id ORDER BY activation_local_datetime), '9999-12-31') AS next_activation_local_datetime,
    cancellation_local_datetime,
    IFF(activation_local_datetime = MAX(activation_local_datetime) OVER (PARTITION BY customer_id), TRUE, FALSE) AS is_current
FROM (
    SELECT
        customer_id,
        MIN(activation_local_datetime) AS activation_local_datetime,
        MAX(cancellation_local_datetime) AS cancellation_local_datetime
    FROM _fact_activation__activations_base
    WHERE NOT (order_id IS NULL AND cancellation_local_datetime BETWEEN activation_local_datetime AND DATEADD(DAY, 1, activation_local_datetime))
    GROUP BY customer_id, DATE_TRUNC('MONTH', DATEADD(SECOND, -900, activation_local_datetime)::DATE)  -- if 900 second delay causes an activation to spill to the previous month, it won't be counted if they made an activation earlier that month
   )
)
SELECT
     null as activation_key
    ,ab.membership_event_key
    ,CASE WHEN ab.store_id = 41 THEN 26 ELSE ab.store_id END AS store_id
    ,ab.customer_id
    ,DATEADD(SECOND, -900, ve.activation_local_datetime) AS activation_local_datetime
    ,DATEADD(SECOND, -900, ve.next_activation_local_datetime) AS next_activation_local_datetime
    ,ve.activation_local_datetime AS source_activation_local_datetime
    ,ve.next_activation_local_datetime AS source_next_activation_local_datetime
    ,ve.cancellation_local_datetime
    ,ab.is_reactivated_vip
    ,ab.order_id
    ,ab.session_id
    ,null activation_channel_type
    ,null activation_channel
    ,null activation_subchannel
    ,DATE_TRUNC('MONTH', DATEADD(SECOND, -900, ve.activation_local_datetime)) AS vip_cohort_month_date
    ,DATE_TRUNC('MONTH', ve.activation_local_datetime) AS source_vip_cohort_month_date
    ,ab.membership_event_type
    ,'Membership Token' AS membership_type
    ,null AS is_vip_activation_from_reactivated_lead
    ,null dm_gateway_id
    ,'Desktop' AS device
    ,null AS browser
    ,null as operating_system
    ,null AS utm_medium
    ,null AS utm_source
    ,null AS utm_campaign
    ,null AS utm_content
    ,null AS utm_term
    ,null as pcode
    ,null as ccode
    ,null as acode
    ,null as scode
    ,null AS sub_store_id
    ,null AS store_type
    ,null AS store_sub_type
    ,null AS is_retail_vip
    ,null AS is_retail_varsity_vip
    ,null AS is_retail_legging_bar_vip
    ,null AS is_mobile_app_vip
    ,null AS is_legging_bar_vip
    ,null AS is_kiosk_vip
    ,ab.is_scrubs_vip
    ,null as order_discount_percent
    ,ve.is_current
    ,ab.cancel_type
    ,null AS cancel_method
    ,null AS cancel_reason
    ,ab.activation_sequence_number
    ,current_date AS meta_create_datetime
    ,current_date AS meta_update_datetime
    ,ROW_NUMBER() over(partition by ab.customer_id order by ab.activation_local_datetime) rk
FROM _fact_activation__activations_base AS ab
    JOIN _fact_activation__valid_events AS ve
        ON ve.customer_id = ab.customer_id
        AND ve.activation_local_datetime = ab.activation_local_datetime
;

-- 更新 source_next_activation_local_datetime 和 next_activation_local_datetime
UPDATE EDW_PROD.NEW_STG.FACT_ACTIVATION AS tgt   
SET 
   tgt.source_next_activation_local_datetime = src.source_next_activation_local_datetime,
   tgt.next_activation_local_datetime = src.next_activation_local_datetime,
   tgt.meta_update_datetime = current_date
from (
    with t1 as (
        select 
             fa.activation_key
            ,fa.customer_id
            ,fa.activation_local_datetime
            ,fa.source_activation_local_datetime
        from EDW_PROD.NEW_STG.FACT_ACTIVATION fa 
        inner JOIN (
            select customer_id 
            from EDW_PROD.NEW_STG.FACT_ACTIVATION_REPLACE
            group by customer_id
            ) t1 on fa.customer_id = t1.customer_id
        where 
           cast(fa.source_next_activation_local_datetime as date) ='9999-12-31'
          or cast(fa.next_activation_local_datetime as date) ='9999-12-30'

        union all 

        select 
             t1.activation_key
            ,t1.customer_id
            ,t1.activation_local_datetime
            ,t1.source_activation_local_datetime
        from EDW_PROD.NEW_STG.FACT_ACTIVATION_REPLACE t1
        left join EDW_PROD.NEW_STG.FACT_ACTIVATION t2 on t1.customer_id = t2.customer_id and t1.activation_local_datetime = t2.activation_local_datetime
        where t2.activation_key is null
    )
    select 
    t1.activation_key::NUMBER(38,0) activation_key
    ,t1.customer_id
    ,LEAD(t1.activation_local_datetime) OVER (PARTITION BY t1.customer_id ORDER BY t1.activation_local_datetime) next_activation_local_datetime
    ,LEAD(t1.source_activation_local_datetime) OVER (PARTITION BY t1.customer_id ORDER BY t1.source_activation_local_datetime) source_next_activation_local_datetime
    from t1 
    QUALIFY activation_key is not null and next_activation_local_datetime is not null and source_next_activation_local_datetime is not null
) as src
where tgt.activation_key = src.activation_key
;


-- 将新数据写入
MERGE INTO EDW_PROD.NEW_STG.FACT_ACTIVATION tgt
USING (
SELECT
        MEMBERSHIP_EVENT_KEY,
        STORE_ID,
        CUSTOMER_ID,
        ACTIVATION_LOCAL_DATETIME,
        NEXT_ACTIVATION_LOCAL_DATETIME,
        SOURCE_ACTIVATION_LOCAL_DATETIME,
        SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME,
        CANCELLATION_LOCAL_DATETIME,
        IS_REACTIVATED_VIP,
        ORDER_ID,
        SESSION_ID,
        ACTIVATION_CHANNEL_TYPE,
        ACTIVATION_CHANNEL,
        ACTIVATION_SUBCHANNEL,
        VIP_COHORT_MONTH_DATE,
        SOURCE_VIP_COHORT_MONTH_DATE,
        MEMBERSHIP_EVENT_TYPE,
        MEMBERSHIP_TYPE,
        IS_VIP_ACTIVATION_FROM_REACTIVATED_LEAD,
        DM_GATEWAY_ID,
        DEVICE,
        BROWSER,
        OPERATING_SYSTEM,
        UTM_MEDIUM,
        UTM_SOURCE,
        UTM_CAMPAIGN,
        UTM_CONTENT,
        UTM_TERM,
        PCODE,
        CCODE,
        ACODE,
        SCODE,
        SUB_STORE_ID,
        STORE_TYPE,
        STORE_SUB_TYPE,
        IS_RETAIL_VIP,
        IS_RETAIL_VARSITY_VIP,
        IS_RETAIL_LEGGING_BAR_VIP,
        IS_MOBILE_APP_VIP,
        IS_LEGGING_BAR_VIP,
        IS_KIOSK_VIP,
        IS_SCRUBS_VIP,
        ORDER_DISCOUNT_PERCENT,
        IS_CURRENT,
        CANCEL_TYPE,
        CANCEL_METHOD,
        CANCEL_REASON,
        ACTIVATION_SEQUENCE_NUMBER,
        META_CREATE_DATETIME,
        META_UPDATE_DATETIME
FROM EDW_PROD.NEW_STG.FACT_ACTIVATION_REPLACE 
) src ON tgt.CUSTOMER_ID = src.CUSTOMER_ID and tgt.ACTIVATION_LOCAL_DATETIME = src.ACTIVATION_LOCAL_DATETIME 
WHEN NOT MATCHED THEN                
    INSERT (
        MEMBERSHIP_EVENT_KEY,
        STORE_ID,
        CUSTOMER_ID,
        ACTIVATION_LOCAL_DATETIME,
        NEXT_ACTIVATION_LOCAL_DATETIME,
        SOURCE_ACTIVATION_LOCAL_DATETIME,
        SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME,
        CANCELLATION_LOCAL_DATETIME,
        IS_REACTIVATED_VIP,
        ORDER_ID,
        SESSION_ID,
        ACTIVATION_CHANNEL_TYPE,
        ACTIVATION_CHANNEL,
        ACTIVATION_SUBCHANNEL,
        VIP_COHORT_MONTH_DATE,
        SOURCE_VIP_COHORT_MONTH_DATE,
        MEMBERSHIP_EVENT_TYPE,
        MEMBERSHIP_TYPE,
        IS_VIP_ACTIVATION_FROM_REACTIVATED_LEAD,
        DM_GATEWAY_ID,
        DEVICE,
        BROWSER,
        OPERATING_SYSTEM,
        UTM_MEDIUM,
        UTM_SOURCE,
        UTM_CAMPAIGN,
        UTM_CONTENT,
        UTM_TERM,
        PCODE,
        CCODE,
        ACODE,
        SCODE,
        SUB_STORE_ID,
        STORE_TYPE,
        STORE_SUB_TYPE,
        IS_RETAIL_VIP,
        IS_RETAIL_VARSITY_VIP,
        IS_RETAIL_LEGGING_BAR_VIP,
        IS_MOBILE_APP_VIP,
        IS_LEGGING_BAR_VIP,
        IS_KIOSK_VIP,
        IS_SCRUBS_VIP,
        ORDER_DISCOUNT_PERCENT,
        IS_CURRENT,
        CANCEL_TYPE,
        CANCEL_METHOD,
        CANCEL_REASON,
        ACTIVATION_SEQUENCE_NUMBER,
        META_CREATE_DATETIME,
        META_UPDATE_DATETIME
        )
        VALUES(
        src.MEMBERSHIP_EVENT_KEY,
        src.STORE_ID,
        src.CUSTOMER_ID,
        src.ACTIVATION_LOCAL_DATETIME,
        src.NEXT_ACTIVATION_LOCAL_DATETIME,
        src.SOURCE_ACTIVATION_LOCAL_DATETIME,
        src.SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME,
        src.CANCELLATION_LOCAL_DATETIME,
        src.IS_REACTIVATED_VIP,
        src.ORDER_ID,
        src.SESSION_ID,
        src.ACTIVATION_CHANNEL_TYPE,
        src.ACTIVATION_CHANNEL,
        src.ACTIVATION_SUBCHANNEL,
        src.VIP_COHORT_MONTH_DATE,
        src.SOURCE_VIP_COHORT_MONTH_DATE,
        src.MEMBERSHIP_EVENT_TYPE,
        src.MEMBERSHIP_TYPE,
        src.IS_VIP_ACTIVATION_FROM_REACTIVATED_LEAD,
        src.DM_GATEWAY_ID,
        src.DEVICE,
        src.BROWSER,
        src.OPERATING_SYSTEM,
        src.UTM_MEDIUM,
        src.UTM_SOURCE,
        src.UTM_CAMPAIGN,
        src.UTM_CONTENT,
        src.UTM_TERM,
        src.PCODE,
        src.CCODE,
        src.ACODE,
        src.SCODE,
        src.SUB_STORE_ID,
        src.STORE_TYPE,
        src.STORE_SUB_TYPE,
        src.IS_RETAIL_VIP,
        src.IS_RETAIL_VARSITY_VIP,
        src.IS_RETAIL_LEGGING_BAR_VIP,
        src.IS_MOBILE_APP_VIP,
        src.IS_LEGGING_BAR_VIP,
        src.IS_KIOSK_VIP,
        src.IS_SCRUBS_VIP,
        src.ORDER_DISCOUNT_PERCENT,
        src.IS_CURRENT,
        src.CANCEL_TYPE,
        src.CANCEL_METHOD,
        src.CANCEL_REASON,
        src.ACTIVATION_SEQUENCE_NUMBER,
        src.META_CREATE_DATETIME,
        src.META_UPDATE_DATETIME
        )
;