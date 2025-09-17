set low_watermark_ltz = (
    select
        max(a.MONTH_DATE)
    from REPORTING_PROD.GFB.gfb_customer_dataset_base a
    );


CREATE OR REPLACE TEMP TABLE _email_interactions_base AS
SELECT
    cd.customer_id
    ,cd.month_date
    ,cd.activation_local_date
    ,cd.cancel_local_date
FROM REPORTING_PROD.GFB.gfb_customer_dataset_base cd
where
    cd.MONTH_DATE >= $low_watermark_ltz
    and cd.MONTH_DATE < date_trunc('MONTH', current_date());


CREATE OR REPLACE TEMP TABLE _customer_contact_map_base AS
SELECT
    sub.customer_id
    ,sub.emarsys_user_id AS contact_id
FROM lake_view.emarsys.email_subscribes AS sub
WHERE
    (
        sub.brand like '%justfab%'
        or
        sub.BRAND like '%shoedazzle%'
        or
        sub.BRAND like '%fabkids%'
    )
    AND sub.customer_id IN (SELECT DISTINCT customer_id FROM _email_interactions_base);


CREATE OR REPLACE TEMP TABLE _email_campaigns AS
SELECT
    ec.campaign_id
FROM lake.emarsys.email_campaigns_v2 AS ec
JOIN lake.emarsys.customer_store_mapping AS csm
    ON csm.customer_id = ec.customer_id
WHERE
    LOWER(csm.store_group) ILIKE ANY ('%justfab%', '%shoedazzle%', '%fabkids%')
    AND NOT CONTAINS(LOWER(ec.name), 'test')
    AND NOT CONTAINS(LOWER(ec.name), 'keya')
    AND NOT CONTAINS(LOWER(ec.name), 'copy')

UNION

SELECT
    es.campaign_id
FROM lake.emarsys.email_sends AS es
JOIN lake.emarsys.customer_store_mapping AS csm
    ON csm.customer_id = es.customer_id
JOIN lake.emarsys.email_campaigns_v2 AS ec
    ON es.campaign_id = ec.campaign_id
WHERE
    LOWER(csm.store_group) ILIKE ANY ('%justfab%', '%shoedazzle%', '%fabkids%')
    AND NOT CONTAINS(LOWER(ec.name), 'test')
    AND NOT CONTAINS(LOWER(ec.name), 'keya')
    AND NOT CONTAINS(LOWER(ec.name), 'copy')

UNION

SELECT
    es.campaign_id
FROM lake.emarsys.email_clicks AS es
JOIN lake.emarsys.customer_store_mapping AS csm
    ON csm.customer_id = es.customer_id
JOIN lake.emarsys.email_campaigns_v2 AS ec
    ON es.campaign_id = ec.campaign_id
WHERE
    LOWER(csm.store_group) ILIKE ANY ('%justfab%', '%shoedazzle%', '%fabkids%')
    AND NOT CONTAINS(LOWER(ec.name), 'test')
    AND NOT CONTAINS(LOWER(ec.name), 'keya')
    AND NOT CONTAINS(LOWER(ec.name), 'copy')

UNION

SELECT
    es.campaign_id
FROM lake.emarsys.email_opens AS es
JOIN lake.emarsys.customer_store_mapping AS csm
    ON csm.customer_id = es.customer_id
JOIN lake.emarsys.email_campaigns_v2 AS ec
    ON es.campaign_id = ec.campaign_id
WHERE
    LOWER(csm.store_group) ILIKE ANY ('%justfab%', '%shoedazzle%', '%fabkids%')
    AND NOT CONTAINS(LOWER(ec.name), 'test')
    AND NOT CONTAINS(LOWER(ec.name), 'keya')
    AND NOT CONTAINS(LOWER(ec.name), 'copy')

UNION

SELECT
    es.campaign_id
FROM lake.emarsys.email_unsubscribes AS es
JOIN lake.emarsys.customer_store_mapping AS csm
    ON csm.customer_id = es.customer_id
JOIN lake.emarsys.email_campaigns_v2 AS ec
    ON es.campaign_id = ec.campaign_id
WHERE
    LOWER(csm.store_group) ILIKE ANY ('%justfab%', '%shoedazzle%', '%fabkids%')
    AND NOT CONTAINS(LOWER(ec.name), 'test')
    AND NOT CONTAINS(LOWER(ec.name), 'keya')
    AND NOT CONTAINS(LOWER(ec.name), 'copy');


CREATE OR REPLACE TEMP TABLE _email_sends AS
SELECT
    customer_id,
    month_date,
    COUNT(*) as send_count
FROM
(
     SELECT
        eb.customer_id,
        eb.month_date,
        c.campaign_id,
        send.message_id
    FROM _email_interactions_base as eb
    JOIN _customer_contact_map_base as mb
        ON mb.customer_id = eb.customer_id
    JOIN lake.emarsys.email_sends AS send
        ON send.contact_id = mb.contact_id
    JOIN _email_campaigns as c
        ON c.campaign_id = send.campaign_id
    WHERE
        cast(send.event_time as DATE) >= eb.activation_local_date
        AND cast(send.event_time as DATE) <= COALESCE(eb.cancel_local_date, CURRENT_DATE())
        AND DATE_TRUNC('MONTH', cast(send.event_time as DATE)) = eb.month_date
    qualify
        row_number() over(PARTITION BY eb.customer_id,c.campaign_id,send.message_id ORDER BY send.event_time) = 1
) AS S
GROUP BY
    customer_id,
    month_date;


CREATE OR REPLACE TEMP TABLE _email_opens AS
select
    customer_id,
    month_date,
    count(*) as open_count
from
(
    SELECT
        eb.customer_id,
        eb.month_date,
        c.campaign_id,
        open.message_id
    FROM _email_interactions_base as eb
    JOIN _customer_contact_map_base as mb
        ON mb.customer_id = eb.customer_id
    JOIN lake.emarsys.email_opens AS OPEN
        ON OPEN.contact_id = mb.contact_id
    JOIN _email_campaigns AS C
        ON C.campaign_id = OPEN.campaign_id
    WHERE
        OPEN.event_time:: DATE >= eb.activation_local_date
        AND OPEN.event_time:: DATE <= COALESCE (eb.cancel_local_date, CURRENT_DATE ())
        AND DATE_TRUNC('MONTH', OPEN.event_time:: DATE) = eb.month_date
    qualify
        row_number() over(PARTITION BY eb.customer_id, C.campaign_id, OPEN.message_id ORDER BY OPEN.event_time) = 1
) AS s
GROUP BY
    customer_id,
    month_date;


CREATE OR REPLACE TEMP TABLE _email_clicks AS
select
    customer_id,
    month_date,
    count(*) as click_count
from
(
    SELECT
        eb.customer_id,
        eb.month_date,
        c.campaign_id,
        click.message_id
    FROM _email_interactions_base AS eb
    JOIN _customer_contact_map_base AS mb
        ON mb.customer_id = eb.customer_id
    JOIN lake.emarsys.email_clicks AS click
        ON click.contact_id = mb.contact_id
    JOIN _email_campaigns AS c
        ON c.campaign_id = click.campaign_id
    WHERE
        click.event_time::DATE >= eb.activation_local_date
        AND click.event_time::DATE <= COALESCE(eb.cancel_local_date, current_date())
        AND DATE_TRUNC('MONTH', click.event_time:: DATE) = eb.month_date
    qualify
        row_number() over(partition BY eb.customer_id,c.campaign_id,click.message_id ORDER BY click.event_time) = 1
) AS s
GROUP BY
    customer_id,
    month_date;


CREATE OR REPLACE TEMP TABLE _email_optout AS
SELECT
    eb.customer_id,
    eb.month_date,
    AVG(datediff(day,optout.event_time::DATE,cancel_local_date)) AS days_between_optout_and_cancel,
    COUNT(1) AS optout_count
FROM _email_interactions_base AS eb
JOIN _customer_contact_map_base AS mb
    ON mb.customer_id = eb.customer_id
JOIN lake.emarsys.email_unsubscribes AS optout
    ON optout.contact_id = mb.contact_id
    AND DATE_TRUNC('MONTH', optout.event_time::DATE) = eb.month_date
JOIN _email_campaigns AS c
    ON c.campaign_id = optout.campaign_id
WHERE
    optout.event_time::DATE >= eb.activation_local_date
    AND optout.event_time::DATE <= COALESCE(eb.cancel_local_date, current_date())
    AND DATE_TRUNC('MONTH', optout.event_time:: DATE) = eb.month_date
GROUP BY
    eb.customer_id,
    eb.month_date;


delete from REPORTING_PROD.GFB.gfb_customer_dataset_email_action a
where
    a.MONTH_DATE >= $low_watermark_ltz;


insert into REPORTING_PROD.GFB.gfb_customer_dataset_email_action
select
    es.customer_id
    ,es.month_date

    ,es.send_count
    ,coalesce(eo.open_count, 0) as open_count
    ,coalesce(ec.click_count, 0) as click_count
    ,coalesce(eop.optout_count, 0) as optout_count
    ,coalesce(eop.days_between_optout_and_cancel,0) AS days_between_optout_and_cancel
from _email_sends es
left join _email_opens eo
    on eo.customer_id = es.customer_id
    and eo.month_date = es.month_date
left join _email_clicks ec
    on ec.customer_id = es.customer_id
    and ec.month_date = es.month_date
left join _email_optout eop
    on eop.customer_id = es.customer_id
    and eop.month_date = es.month_date;
