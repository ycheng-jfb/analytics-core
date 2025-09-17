-- Daily Click Attribution Incremental
-- output: click metrics by store, channel, subchannel, vendor, utm params, date grain
-- datasource: MEDDS16 - Daily Click Attribution
-- dashboard:  MED98 - Daily Click Attribution (MED98b - internal dashboard saved in Media Measurement Admin for QA)

------------------------------------------------------------------------------------
------------------------------------------------------------------------------------

set execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);
set target_table = 'reporting_media_prod.dbo.daily_click_attribution_incr' ;

    MERGE INTO reporting_media_prod.public.meta_table_dependency_watermark AS t
    USING (
        SELECT
            'reporting_media_prod.dbo.daily_click_attribution_incr' AS table_name,
            NULLIF(dependent_table_name, 'reporting_media_prod.dbo.daily_click_attribution_incr') AS dependent_table_name,
            high_watermark_datetime AS new_high_watermark_datetime
        FROM (
            SELECT
                'reporting_base_prod.shared.session' AS dependent_table_name,
                max(session_local_datetime)::timestamp_ltz(3) AS high_watermark_datetime
                FROM reporting_base_prod.shared.session
            UNION
            SELECT
                'edw_prod.data_model.fact_registration' AS dependent_table_name,
                max(registration_local_datetime)::timestamp_ltz(3) AS high_watermark_datetime
                FROM edw_prod.data_model.fact_registration
            UNION
            SELECT
                'edw_prod.data_model.fact_activation' AS dependent_table_name,
                max(activation_local_datetime)::timestamp_ltz(3) AS high_watermark_datetime
                FROM edw_prod.data_model.fact_activation
            ) AS h
        ) AS s
    ON t.table_name = s.table_name
        AND equal_null(t.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED
        AND NOT equal_null(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
    THEN UPDATE SET
        t.new_high_watermark_datetime = s.new_high_watermark_datetime,
        t.meta_update_datetime = current_timestamp::timestamp_ltz(3)
    WHEN NOT MATCHED THEN
    INSERT (
        table_name,
        dependent_table_name,
        high_watermark_datetime,
        new_high_watermark_datetime
    )
    VALUES (
        s.table_name,
        s.dependent_table_name,
        '2023-01-01'::timestamp_ltz,
        s.new_high_watermark_datetime
        )
;

set session_data_last_updated = reporting_media_prod.public.udf_get_watermark($target_table,'reporting_base_prod.shared.session');
set registration_data_last_updated = reporting_media_prod.public.udf_get_watermark($target_table,'edw_prod.data_model.fact_registration');
set activation_data_last_updated =  reporting_media_prod.public.udf_get_watermark($target_table,'edw_prod.data_model.fact_activation');


set session_metadata_last_updated = (select max(meta_update_datetime) from reporting_base_prod.shared.session);
set registration_metadata_last_updated = (select max(meta_update_datetime) from edw_prod.data_model.fact_registration);
set activation_metadata_last_updated = (select max(meta_update_datetime) from edw_prod.data_model.fact_activation);

create or replace temporary table _session_mapping as
select distinct
       s.store_id,
       session_id,
       s.utm_medium,
       s.utm_source,
       s.utm_campaign,
       s.utm_content,
       s.utm_term,
       channel,
       subchannel,
       vendor
from reporting_base_prod.shared.session s
join reporting_base_prod.shared.media_source_channel_mapping m on m.media_source_hash = s.media_source_hash
join edw_prod.data_model.dim_store ds on ds.store_id = s.store_id
where session_local_datetime >= $session_data_last_updated
   -- or (to_date(session_local_datetime) between dateadd(day,-2,$last_year_start_date) and $last_year_end_date)
    and session_id is not null;

update _session_mapping
set vendor = 'other'
where vendor is null or vendor = '' or vendor = 'none';

update _session_mapping
set vendor = 'unclassified'
where channel = 'unclassified' and subchannel = 'unclassified';

create or replace temporary table _sessions as
select distinct
       s.store_id,
       store_brand as store_brand_name,
       store_region,
       store_country,
       cast(case when is_male_session = true or is_male_gateway = true then true else false end as boolean) as is_male_customer,
       cast(case when is_scrubs_gateway = true then true else false end as boolean) as is_fl_scrubs_customer,
       to_date(session_local_datetime) as date,
       session_id,
       1 as sessions
from reporting_base_prod.shared.session s
join reporting_base_prod.shared.media_source_channel_mapping m on m.media_source_hash = s.media_source_hash
join edw_prod.data_model.dim_store ds on ds.store_id = s.store_id
where session_local_datetime >= $session_data_last_updated
    --or (to_date(session_local_datetime) between dateadd(day,-2,$last_year_start_date) and $last_year_end_date)
    and session_id is not null;

create or replace temporary table _leads as
select distinct
       l.store_id,
       store_brand as store_brand_name,
       store_region,
       store_country,
       cast(case when lower(gender) = 'm' and lower(store_brand_abbr) = 'fl' then true else false end as boolean) as is_male_customer,
       cast(is_scrubs_customer as boolean) as is_fl_scrubs_customer,
       to_date(l.registration_local_datetime) as date,
       l.session_id,
       1 as primary_leads
from edw_prod.data_model.fact_registration l
join edw_prod.data_model.dim_store ds on ds.store_id = l.store_id
join edw_prod.data_model.dim_customer c on c.customer_id = l.customer_id
where is_secondary_registration = false
    and is_retail_registration = false
    and is_fake_retail_registration = false
    and l.registration_local_datetime >= $registration_data_last_updated
   -- or (to_date(l.registration_local_datetime) between $last_year_start_date and $last_year_end_date))
    and l.session_id is not null;

create or replace temporary table _from_leads as
select distinct
        l.store_id,
        store_brand as store_brand_name,
        store_region,
        store_country,
        cast(case when lower(gender) = 'm' and lower(store_brand_abbr) = 'fl' then true else false end as boolean) as is_male_customer,
        cast(is_scrubs_customer as boolean) as is_fl_scrubs_customer,
        to_date(l.registration_local_datetime) as date,
        l.session_id,
        iff(activation_local_datetime is null,0,1) as vips_from_leads_7d,
        1 as orders,
        unit_count,
        reporting_usd_conversion_rate,
        product_gross_revenue_local_amount,
        product_subtotal_local_amount,
        product_discount_local_amount
from edw_prod.data_model.fact_registration l
join edw_prod.data_model.dim_store ds on ds.store_id = l.store_id
join edw_prod.data_model.dim_customer c on c.customer_id = l.customer_id
join edw_prod.data_model.fact_activation a on a.customer_id = l.customer_id
    and datediff(day,l.registration_local_datetime,activation_local_datetime) < 7
    and datediff(day,l.registration_local_datetime,activation_local_datetime) >= 0
    and is_retail_vip = false
join edw_prod.data_model.fact_order o on a.activation_key = o.activation_key
join edw_prod.data_model.dim_order_membership_classification mc on o.order_membership_classification_key = mc.order_membership_classification_key
    and lower(membership_order_type_l3) in ('activating vip')
where is_secondary_registration = false
    and is_retail_registration = false
    and is_fake_retail_registration = false
    and l.registration_local_datetime >= $registration_data_last_updated
  --  or (to_date(l.registration_local_datetime) between $last_year_start_date and $last_year_end_date))
    and l.session_id is not null;

create or replace temporary table _same_session_vips as
select distinct
        a.store_id,
        store_brand as store_brand_name,
        store_region,
        store_country,
        cast(case when lower(gender) = 'm' and lower(store_brand_abbr) = 'fl' then true else false end as boolean) as is_male_customer,
        cast(is_scrubs_customer as boolean) as is_fl_scrubs_customer,
        a.session_id,
        to_date(activation_local_datetime) as date,
        1 as same_session_vips,
        1 as orders,
        unit_count,
        reporting_usd_conversion_rate,
        product_gross_revenue_local_amount,
        product_subtotal_local_amount,
        product_discount_local_amount
from edw_prod.data_model.fact_activation a
join edw_prod.data_model.fact_order o on a.order_id = o.order_id and a.customer_id = o.customer_id
join edw_prod.data_model.dim_order_membership_classification mc on o.order_membership_classification_key = mc.order_membership_classification_key
    and lower(membership_order_type_l3) in ('activating vip')
join edw_prod.data_model.dim_store ds on ds.store_id = a.store_id
join edw_prod.data_model.dim_customer c on c.customer_id = a.customer_id
where activation_local_datetime >= $activation_data_last_updated
  --  or (to_date(activation_local_datetime) between $last_year_start_date and $last_year_end_date))
    and is_retail_vip = false
    and a.session_id is not null;

create or replace temporary table _combos as
select distinct store_id, store_brand_name, store_region, store_country, is_male_customer, is_fl_scrubs_customer, date, session_id from _sessions
union select distinct store_id, store_brand_name, store_region, store_country, is_male_customer, is_fl_scrubs_customer, date, session_id from _leads
union select distinct store_id, store_brand_name, store_region, store_country, is_male_customer, is_fl_scrubs_customer, date, session_id from _from_leads
union select distinct store_id, store_brand_name, store_region, store_country, is_male_customer, is_fl_scrubs_customer, date, session_id from _same_session_vips;

create or replace temporary table _conversions_agg as
select c.*,
        utm_medium,
        utm_source,
        utm_campaign,
        utm_content,
        utm_term,
        channel,
        subchannel,
        vendor,
        sessions as sessions,
        primary_leads as primary_leads,
        vips_from_leads_7d as vips_from_leads_7d,
        l.orders as orders_from_leads,
        l.unit_count as unit_count_from_leads,
        l.product_gross_revenue_local_amount * l.reporting_usd_conversion_rate as gross_revenue_from_leads,
        same_session_vips as same_session_vips,
        v.orders as orders,
        v.unit_count as unit_count,
        v.product_gross_revenue_local_amount * v.reporting_usd_conversion_rate as gross_revenue
from _combos c
join _session_mapping m on c.session_id = m.session_id
left join _sessions s on s.session_id = c.session_id
    and s.store_id = c.store_id
    and s.store_brand_name = c.store_brand_name
    and s.store_region = c.store_region
    and s.store_country = c.store_country
    and s.is_male_customer = c.is_male_customer
    and s.is_fl_scrubs_customer = c.is_fl_scrubs_customer
    and s.date = c.date
left join _leads r on r.session_id = c.session_id
    and r.store_id = c.store_id
    and r.store_brand_name = c.store_brand_name
    and r.store_region = c.store_region
    and r.store_country = c.store_country
    and r.is_male_customer = c.is_male_customer
    and r.is_fl_scrubs_customer = c.is_fl_scrubs_customer
    and r.date = c.date
left join _from_leads l on l.session_id = c.session_id
    and l.store_id = c.store_id
    and l.store_brand_name = c.store_brand_name
    and l.store_region = c.store_region
    and l.store_country = c.store_country
    and l.is_male_customer = c.is_male_customer
    and l.is_fl_scrubs_customer = c.is_fl_scrubs_customer
    and l.date = c.date
left join _same_session_vips v on v.session_id = c.session_id
    and v.store_id = c.store_id
    and v.store_brand_name = c.store_brand_name
    and v.store_region = c.store_region
    and v.store_country = c.store_country
    and v.is_male_customer = c.is_male_customer
    and v.is_fl_scrubs_customer = c.is_fl_scrubs_customer
    and v.date = c.date;

-- final output table
create or replace temporary table _daily_click_attribution_final as
select store_id,
        store_brand_name,
        store_region,
        store_country,
        is_male_customer,
        is_fl_scrubs_customer,
        utm_medium,
        utm_source,
        utm_campaign,
        utm_content,
        utm_term,
        channel,
        subchannel,
        vendor,
        date,
        sum(sessions) as sessions,
        sum(primary_leads) as primary_leads,
        sum(vips_from_leads_7d) as vips_from_leads_7d,
        sum(same_session_vips) as same_session_vips,
        sum(orders_from_leads) as orders_from_leads,
        sum(unit_count_from_leads) as unit_count_from_leads,
        sum(gross_revenue_from_leads) as gross_revenue_from_leads,
        sum(orders) as orders,
        sum(unit_count) as unit_count,
        sum(gross_revenue) as gross_revenue
from _conversions_agg
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;

MERGE INTO reporting_media_prod.dbo.daily_click_attribution_incr t
USING (SELECT *,
              HASH(*) AS meta_row_hash
       FROM _daily_click_attribution_final
      ) s
    ON equal_null(t.store_id, s.store_id) AND
       equal_null(t.store_brand_name, s.store_brand_name) AND
       equal_null(t.store_region, s.store_region) AND
       equal_null(t.store_country, s.store_country) AND
       equal_null(t.is_male_customer, s.is_male_customer) AND
       equal_null(t.is_fl_scrubs_customer, s.is_fl_scrubs_customer) AND
       equal_null(t.utm_medium, s.utm_medium) AND
       equal_null(t.utm_source, s.utm_source) AND
       equal_null(t.utm_campaign, s.utm_campaign) AND
       equal_null(t.utm_content, s.utm_content) AND
       equal_null(t.utm_term, s.utm_term) AND
       equal_null(t.channel, s.channel) AND
       equal_null(t.subchannel, s.subchannel) AND
       equal_null(t.vendor, s.vendor) AND
       equal_null(t.DATE, s.DATE)
WHEN NOT MATCHED THEN
  INSERT ( store_id,
           store_brand_name,
           store_region,
           store_country,
           is_male_customer,
           is_fl_scrubs_customer,
           utm_medium,
           utm_source,
           utm_campaign,
           utm_content,
           utm_term,
           channel,
           subchannel,
           vendor,
           DATE,
           sessions,
           primary_leads,
           vips_from_leads_7d,
           same_session_vips,
           orders_from_leads,
           unit_count_from_leads,
           gross_revenue_from_leads,
           orders,
           unit_count,
           gross_revenue,
           session_data_last_updated,
           registration_data_last_updated,
           activation_data_last_updated,
           meta_row_hash,
           meta_create_datetime,
           meta_update_datetime )
  VALUES ( store_id,
           store_brand_name,
           store_region,
           store_country,
           is_male_customer,
           is_fl_scrubs_customer,
           utm_medium,
           utm_source,
           utm_campaign,
           utm_content,
           utm_term,
           channel,
           subchannel,
           vendor,
           DATE,
           sessions,
           primary_leads,
           vips_from_leads_7d,
           same_session_vips,
           orders_from_leads,
           unit_count_from_leads,
           gross_revenue_from_leads,
           orders,
           unit_count,
           gross_revenue,
           $session_metadata_last_updated,
           $registration_metadata_last_updated,
           $activation_metadata_last_updated,
           meta_row_hash,
           $execution_start_time,
           $execution_start_time)
WHEN MATCHED AND NOT equal_null(t.meta_row_hash, s.meta_row_hash) THEN
  UPDATE SET t.sessions = s.sessions,
             t.primary_leads = s.primary_leads,
             t.vips_from_leads_7d = s.vips_from_leads_7d,
             t.same_session_vips = s.same_session_vips,
             t.orders_from_leads = s.orders_from_leads,
             t.unit_count_from_leads = s.unit_count_from_leads,
             t.gross_revenue_from_leads = s.gross_revenue_from_leads,
             t.orders = s.orders,
             t.unit_count = s.unit_count,
             t.gross_revenue = s.gross_revenue,
             t.session_data_last_updated = $session_metadata_last_updated,
             t.registration_data_last_updated = $registration_metadata_last_updated,
             t.activation_data_last_updated = $activation_metadata_last_updated,
             t.meta_row_hash = s.meta_row_hash,
             t.meta_update_datetime = $execution_start_time;

update reporting_media_prod.public.meta_table_dependency_watermark
set high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime = current_timestamp::timestamp_ltz(3)
where table_name=$target_table;
