CREATE OR REPLACE TRANSIENT TABLE reporting_media_prod.dbo.ccpa_gdpr_requests AS
SELECT DISTINCT r.customer_id,
                lower(c.email) AS email1,
                CASE WHEN lower(r.email) IS DISTINCT FROM lower(c.email) THEN lower(r.email) END AS email2,
                a.phone AS phonenumber1,
                ds.store_id,
                ds.store_brand_abbr,
                ds.store_country,
                ds.store_region,
                r.date_requested::DATE date_requested,
                r.datetime_completed,
                CASE
                    WHEN r.request_source_id = 1 THEN 'Live Agent'
                    WHEN r.request_source_id = 2 THEN 'Live Chat'
                    WHEN r.request_source_id = 3 THEN 'Web Form'
                    WHEN r.request_source_id = 4 THEN 'Mobile App'
                    ELSE 'n/a' END AS request_source,
                rt.label request_type,
                sc.label status,
                r.region_id
FROM lake_consolidated_view.gdpr.request r
         LEFT JOIN lake_consolidated_view.gdpr.request_type rt ON rt.request_type_id = r.request_type_id
         LEFT JOIN lake_consolidated_view.gdpr.statuscode sc ON sc.statuscode = r.statuscode
         LEFT JOIN lake_consolidated_view.ultra_merchant.customer c ON c.customer_id = r.customer_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.address a ON a.address_id = c.default_address_id
         LEFT JOIN edw_prod.data_model.dim_store ds ON ds.store_id = c.store_id
WHERE r.date_requested >= '2018-05-01'
  --AND r.statuscode != 1009 --canceled by customer
  AND r.region_id IN (1, 2)
  AND r.request_type_id IN (1, 2, 3, 4)
  -- the below conditions are to exclude any testing data
  AND NOT (
            lower(coalesce(r.email, '')) LIKE ANY
            ('%@test.com', '%@fkqa.com', '%@testus.com', '%@sdtest.com', '%@example.com')
        OR lower(coalesce(c.email, '')) LIKE ANY
           ('%@test.com', '%@fkqa.com', '%@testus.com', '%@sdtest.com', '%@example.com')
        OR lower(coalesce(r.firstname, '')) = 'test'
        OR lower(coalesce(r.lastname, '')) = 'test'
        -- these customer ids use real names and real emails and cant be filtered using above criteria, so they are hardcoded.
        OR r.customer_id IN
           (502413742, '531255367', '516460522', '516579874', '583508026', '288108274',
            '288260659', '507407326', '583508026', '594349831', '497443942', '262538839',
            '257226748', '516622564', '516628483', '497443942', '285702967', '634903084',
            '634913542', '634766827', '589680268', '588229333', '634887427', '516629110',
            '569938609', '634932565', '634933039', '572705233', '508514845', '634938214',
            '573155344', '634937857', '577780990', '522734503', '634947037', '301101181',
            '635029471', '580650100', '634987156', '634984291', '634979728', '633544843',
            '634547311', '580782523', '634968400', '634541770', '397299988', '573116914',
            '571440013', '571438795', '634932931', '576389011', '634956550', '634957066',
            '579102046', '579370579', '579375025', '613586188', '620653471', '620653618',
            '566681530', '566057257', '635081515', '529364875', '516622819', '634349032',
            '634795396', '621768640', '620632900', '512859916', '186411037', '18641100',
            '634556764', '638207878')
    );
