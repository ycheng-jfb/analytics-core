 create or replace view lake.blisspoint.vw_hourly_spend as
   SELECT *,
        case
            when lower(a.business_unit) in ('savagex', 'savage x') then 'savage x'
            when lower(a.business_unit) = 'fableticsmen' then lower('fabletics' || ' ' || a.country)
            else lower(a.business_unit) || ' ' || lower(a.country)
        end as store_reporting_name
    FROM lake.blisspoint.hourly_spend a
    ;
