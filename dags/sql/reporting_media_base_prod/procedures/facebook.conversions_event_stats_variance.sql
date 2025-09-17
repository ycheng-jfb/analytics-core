set event_check_date = CURRENT_DATE();


CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.facebook.conversions_event_stats_variance AS
with last_day_events as
(
SELECT pixel_id,
    event,
    CONVERT_TIMEZONE('America/Los_Angeles', start_time)::date as event_start_date,
    sum(coalesce(browser,0)) as last_day_browser_events_count,
    sum(coalesce(server,0)) as last_day_server_events_count
FROM lake.facebook.ads_pixel_stats
WHERE event_start_date = DATEADD(DAY, -1, $event_check_date)
GROUP BY 1, 2, 3
),
current_day_events as
(
SELECT pixel_id,
    event,
    -- start_time,
    CONVERT_TIMEZONE('America/Los_Angeles', start_time)::date as event_start_date,
    sum(coalesce(browser,0)) as curr_day_browser_events_count,
    sum(coalesce(server,0)) as curr_day_server_events_count
FROM lake.facebook.ads_pixel_stats
WHERE event_start_date = $event_check_date
GROUP BY 1, 2, 3
)
select
    coalesce(c.pixel_id, l.pixel_id) as pixel_id,
    coalesce(c.event, l.event) as event_id,
    last_day_browser_events_count,
    curr_day_browser_events_count,
    last_day_server_events_count,
    curr_day_server_events_count,
    abs(last_day_browser_events_count-curr_day_browser_events_count)/IFF(nvl(last_day_browser_events_count,0)=0,1,last_day_browser_events_count)*100 browser_events_var,
    abs(last_day_server_events_count-curr_day_server_events_count)/IFF(nvl(last_day_server_events_count,0)=0,1,last_day_server_events_count)*100 server_events_var,
    IFF(browser_events_var>70, 'Variance is >70% when comp to last day', NULL) browser_events_variance_details,
    IFF(server_events_var>70, 'Variance is >70% when comp to last day', NULL) server_events_variance_details,
from last_day_events l
join current_day_events c
on c.pixel_id = l.pixel_id
and c.event = l.event
and (browser_events_variance_details IS NOT NULL or server_events_variance_details IS NOT NULL)
order by pixel_id, event_id;
