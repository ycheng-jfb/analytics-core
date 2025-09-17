create or replace transient table reporting_prod.fabletics.pcvr_product_psource as
SELECT session_start_date,
       platform,
       psource,
       country,
       IFF(LOWER(country) IN ('us', 'ca'), 'North America', 'Europe') AS region,
       product_type,
       ses.product_id,
       is_activating,
       registration_session,
       registration_source,
       INITCAP(membership_status)                                          AS membership_status,
       SUM(ses.list_view_total)                                            AS list_view_total,
       SUM(ses.pdp_view_total)                                             AS pdp_view_total,
       SUM(ses.pa_total)                                                   AS pa_total,
       SUM(ses.oc_total)                                                   AS oc_total,
       SUM(ses.ss_pdp_view)                                                AS ss_pdp_view,
       SUM(ses.ss_product_added)                                           AS ss_product_added,
       SUM(ses.ss_product_ordered)                                         AS ss_product_ordered,
       current_timestamp::datetime                                         AS refresh_datetime,
       max(ses.MAX_RECEIVED)                                               AS max_received,
       max(ses.max_received_app) as max_received_app
FROM reporting_prod.fabletics.pcvr_session_historical AS ses
where ses.product_id is not null
  and ses.product_id <>''
GROUP BY session_start_date,
         platform,
         psource,
         country,
         product_type,
         ses.product_id,
         is_activating,
         registration_session,
         registration_source,
         INITCAP(membership_status);

