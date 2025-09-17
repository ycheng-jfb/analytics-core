
-- Click Conversion Counts

set start_date = dateadd(year,-2,current_date());

--------------------------------------------------------------------
-- isolate all sessions with media partner ID (irmp) in the UTM string

create or replace temporary table _sessions as
select
    u.session_id, referer, uri, s.customer_id, u.datetime_added,
    -- adding mens flag to seperate fl and flm traffic (utilizes gateway name and utm parameters)
    case when (g.label ilike '%%flm%%'
                   or uri ilike '%%utm_term=flmen%%'
                   or uri ilike '%%gender=m%%'
                   or uri ilike '%%irmp=2240647%%'
                                      or uri ilike '%%irmp=flmen%%') then 1 else 0 end as mens_session_flag,
    case when uri ilike '%%utm_term=flscb%%' then 1 else 0 end as fl_scrubs_session_flag
from lake_consolidated_view.ultra_merchant.session_uri u
left join lake_consolidated_view.ultra_merchant.session s on s.session_id = u.session_id
left join lake_consolidated_view.ultra_merchant.dm_gateway g on g.dm_gateway_id = s.dm_gateway_id
where u.datetime_added::date >= $start_date
    and (lower(uri) like '%%irmp=%%' or lower(uri) like '%%&imp=%%'); -- have to add IMP here because of error

--------------------------------------------------------------------
-- get uri values parsed out

create or replace temporary table _uri as
    select a.session_id, customer_id, datetime_added, mens_session_flag, fl_scrubs_session_flag
      , right(lower(uri), length(lower(uri))-(charindex('?', lower(uri)))) as uri_paramters
      , row_number() over (order by a.session_id) as sequence
  from _sessions a;

update _uri
set uri_paramters = replace(lower(uri_paramters),'%%3a',':');

create or replace temporary table _uri_parameters as
(
    select session_id, customer_id, datetime_added, mens_session_flag, fl_scrubs_session_flag
    , substr(value,1 , charindex( '=', value, 1)-1) as key
    ,case
     when charindex('=', value) > 0 then left(substr(value, charindex( '=', value, 1)+1),45)
     when charindex('&', value) > 0 then left(substr(value, charindex( '&', value, 1)+1),45)
     when charindex('?', value) > 0 then left(substr(value, charindex( '?', value, 1)+1),45)
     else left(substr(value, charindex( '=', value, 1)+1),50)
     end as value
  from _uri, lateral split_to_table(_uri.uri_paramters, '&')
  where key in ('clickid','sharedid','irad','mpid','irmp','imp','utm_content','utm_campaign','utm_medium','utm_source','utm_term')
);

--imp should not be a key in above table (error in documentation)
--replacing imp with irmp moving forward
create or replace temporary table _uri_parameters_new as
select distinct * from (select distinct session_id, customer_id, datetime_added,
       mens_session_flag, fl_scrubs_session_flag,
       iff(key='imp','irmp',key) as key,
        value
from _uri_parameters);

create or replace temporary table _uri_pivoted as
select
    session_id,
    customer_id,
    datetime_added,
    mens_session_flag,
    fl_scrubs_session_flag,
    clickid,
    sharedid,
    irad,
    mpid,
    irmp,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    utm_term
from _uri_parameters_new
pivot(max(value) for key in ('clickid','sharedid','irad','mpid','irmp','utm_medium','utm_source','utm_campaign','utm_content','utm_term')) as p
        (session_id,customer_id,datetime_added,mens_session_flag,fl_scrubs_session_flag,clickid,sharedid,irad,mpid,irmp,utm_medium,utm_source,utm_campaign,utm_content,utm_term);

update _uri_pivoted
set irmp = left(irmp,charindex('?',irmp)-1)
where irmp like '%%?%%';

update _uri_pivoted
set irmp = left(irmp,charindex(char(37)||'3f',lower(irmp))-1)
where lower(irmp) like '%%3f%%';

update _uri_pivoted
set sharedid = left(sharedid,charindex('?',sharedid)-1)
where sharedid like '%%?%%';

delete from _uri_pivoted
where utm_medium ilike '%%podcast%%' or utm_medium ilike '%%affiliate%%';

--------------------------------------------------------------------
-- count all sessions from influencer links

create or replace temporary table _session as
select
    lower(cast(irmp as varchar(55))||'|'||coalesce(sharedid,'nosid')) as media_partner_id,
    cast(null as varchar(55)) as reporting_id,
    s.store_id as store_id,
    u.session_id,
    'Session' as event_type,
    'Session' as event_description,
    u.datetime_added as event_datetime,
    u.customer_id as customer_id,
    u.mens_session_flag,
    u.fl_scrubs_session_flag,
    null as order_id,
    null as revenue,
    null as margin,
    null as order_units
from _uri_pivoted u
left join lake_consolidated_view.ultra_merchant.session s on s.session_id = u.session_id;

create or replace temporary table _session_count as
select
    media_partner_id,
    reporting_id,
    cast(case when mens_session_flag=1 then store_id || '011'
        when fl_scrubs_session_flag=1 then store_id || '022'
    else store_id end as int) as media_store_id,
    session_id,
    event_type,
    event_description,
    event_datetime,
    customer_id,
    order_id,
    revenue,
    margin,
    order_units
from _session;

----------------------------------------------------------------------------
-- count all click through leads from influencer links

create or replace temporary table _lead_flag as
select
    lower(cast(irmp as varchar(55))||'|'||coalesce(sharedid,'nosid')) as media_partner_id,
    cast(null as varchar(55)) as reporting_id,
    ds.store_id,
    p.session_id,
    'Lead' as event_type,
    'Click' as event_description,
    c.registration_local_datetime::TIMESTAMP_NTZ(9) as event_datetime,
    c.customer_id,
    p.mens_session_flag,
    p.fl_scrubs_session_flag,
    null as order_id,
    null as revenue,
    null as margin,
    null as order_units
from _uri_pivoted p
join edw_prod.data_model.fact_registration c on c.session_id = p.session_id
join edw_prod.data_model.dim_store ds on ds.store_id = c.store_id;


create or replace temporary table _leads as
select
    media_partner_id,
    reporting_id,
    cast(case when mens_session_flag=1 then store_id || '011'
        when fl_scrubs_session_flag=1 then store_id || '022'
    else store_id end as int) as media_store_id,
    session_id,
    event_type,
    event_description,
    event_datetime,
    customer_id,
    order_id,
    revenue,
    margin,
    order_units
from _lead_flag;

----------------------------------------------------------------------------
-- count all vips from click leads within 30 days

create or replace temporary table _vips_from_leads30d_flag as
select
    lower(cast(irmp as varchar(55))||'|'||coalesce(sharedid,'nosid')) as media_partner_id,
    cast(null as varchar(55)) as reporting_id,
    ds.store_id,
    p.session_id,
    'VIP' as event_type,
    'Click-fromLead30D' as event_description,
    v.activation_local_datetime::TIMESTAMP_NTZ(9) as event_datetime,
    c.customer_id,
    p.mens_session_flag,
    p.fl_scrubs_session_flag,
    v.order_id as order_id,
    coalesce(fo.product_gross_revenue_local_amount * fo.order_date_usd_conversion_rate,0) as revenue,
    coalesce(fo.product_order_cash_margin_pre_return_local_amount * fo.order_date_usd_conversion_rate,0) as margin,
    fo.unit_count as order_units
from _uri_pivoted p
join edw_prod.data_model.fact_registration c on c.session_id = p.session_id
join edw_prod.data_model.fact_activation v on v.customer_id = c.customer_id
    and datediff(day,c.registration_local_datetime,v.activation_local_datetime) < 30
join edw_prod.data_model.fact_order fo on fo.order_id = v.order_id
    and fo.store_id = v.store_id
join edw_prod.data_model.dim_store ds on ds.store_id = c.store_id;

create or replace temporary table _vips_from_leads30d as
select
    media_partner_id,
    reporting_id,
    cast(case when mens_session_flag=1 then store_id || '011'
        when fl_scrubs_session_flag=1 then store_id || '022'
    else store_id end as int) as media_store_id,
    session_id,
    event_type,
    event_description,
    event_datetime,
    customer_id,
    order_id,
    revenue,
    margin,
    order_units
from _vips_from_leads30d_flag;

----------------------------------------------------------------------------
-- count all same session vips not from click lead pool

create or replace temporary table _vips_not_from_leads_flag as
select
    lower(cast(irmp as varchar(55))||'|'||coalesce(sharedid,'nosid')) as media_partner_id,
    cast(null as varchar(55)) as reporting_id,
    ds.store_id,
    p.session_id,
    'VIP' as event_type,
    'Click-notfromLead' as event_description,
    v.activation_local_datetime::TIMESTAMP_NTZ(9) as event_datetime,
    c2.customer_id,
    p.mens_session_flag,
    p.fl_scrubs_session_flag,
    v.order_id as order_id,
    coalesce(fo.product_gross_revenue_local_amount * fo.order_date_usd_conversion_rate,0) as revenue,
    coalesce(fo.product_order_cash_margin_pre_return_local_amount * fo.order_date_usd_conversion_rate,0) as margin,
    fo.unit_count as order_units
from _uri_pivoted p
join edw_prod.data_model.fact_activation v on v.session_id = p.session_id
join edw_prod.data_model.fact_registration c2 on c2.customer_id = v.customer_id
join edw_prod.data_model.fact_order fo on fo.order_id = v.order_id
    and fo.store_id = v.store_id
join edw_prod.data_model.dim_store ds on ds.store_id = c2.store_id
left join edw_prod.data_model.fact_registration c on c.session_id = p.session_id
where c.session_id is null;

create or replace temporary table _vips_not_from_leads as
select
    media_partner_id,
    reporting_id,
    cast(case when mens_session_flag=1 then store_id || '011'
        when fl_scrubs_session_flag=1 then store_id || '022'
    else store_id end as int) as media_store_id,
    session_id,
    event_type,
    event_description,
    event_datetime,
    customer_id,
    order_id,
    revenue,
    margin,
    order_units
from _vips_not_from_leads_flag;


-- HDYH Conversion Counts:
----------------------------------------------------------------------------
-- create table with mpid and hdyh value mapping from numerous sources

-- new hdyh/paid/organic influencer mapping gsheet (seperate tabs for each brand/region)
create or replace temporary table _NEW_organic_influencer_mapping as
select distinct lower(business_unit_abbr) as business_unit_abbr, hdyh, region,
    lower(media_partner_id) as media_partner_id
from lake_view.sharepoint.med_organic_influencer_mapping
where media_partner_id is not null
    and hdyh is not null
    and region is not null;

create or replace temporary table _dynamic_hdyh_mapping as
select distinct store_name_abbr as business_unit_abbr, influencer_cleaned_name as hdyh, 'NA' as region, --region alias
    lower(cast(media_partner_id as varchar)) as media_partner_id
from lake_view.sharepoint.med_influencer_mapping
where media_partner_id is not null
    and influencer_cleaned_name is not null;

create or replace temporary table _dynamic_hdyh_mapping_changelog1 as
select distinct store_name_abbr as business_unit_abbr, hdyh_answer_change_1 as hdyh, 'NA' as region, --region alias
    lower(cast(media_partner_id as varchar)) as media_partner_id
from lake_view.sharepoint.med_influencer_mapping
where media_partner_id is not null
    and hdyh_answer_change_1 is not null;

create or replace temporary table _dynamic_hdyh_mapping_changelog2 as
select distinct store_name_abbr as business_unit_abbr, hdyh_answer_change_2 as hdyh, 'NA' as region, --region alias
    lower(cast(media_partner_id as varchar)) as media_partner_id
from lake_view.sharepoint.med_influencer_mapping
where media_partner_id is not null
    and hdyh_answer_change_2 is not null;

create or replace temporary table _dynamic_hdyh_mapping_changelog3 as
select distinct store_name_abbr as business_unit_abbr, hdyh_answer_change_3 as hdyh, 'NA' as region, --region alias
    lower(cast(media_partner_id as varchar)) as media_partner_id
from lake_view.sharepoint.med_influencer_mapping
where media_partner_id is not null
    and hdyh_answer_change_3 is not null;

create or replace transient table reporting_media_base_prod.influencers.influencer_historical_mpid_hdyh_mapping as -- before you run, look at other procs
select * from _NEW_organic_influencer_mapping
union
select * from _dynamic_hdyh_mapping
union
select * from _dynamic_hdyh_mapping_changelog1
union
select * from _dynamic_hdyh_mapping_changelog2
union
select * from _dynamic_hdyh_mapping_changelog3;

update reporting_media_base_prod.influencers.influencer_historical_mpid_hdyh_mapping
    set media_partner_id = media_partner_id || '|nosid'
where media_partner_id not ilike '%%|%%';


----------------------------------------------------------------------------
-- count all leads with influencer hdyh answers

-- joining on region to avoid cross-business unit/region duplications
create or replace temporary table _hdyh_leads_flag as
select
    ip.media_partner_id as media_partner_id,
    cast(null as varchar(55)) as reporting_id,
    st.store_id,
    c.session_id as session_id,
    'Lead' as event_type,
    'HDYH' as event_description,
    c.registration_local_datetime::TIMESTAMP_NTZ(9) as event_datetime,
    c.customer_id,
    cast(iff(lower(store_brand) = 'fabletics' and lower(gender) = 'm', 1, 0) as boolean) as mens_session_flag,
    cast(iff(lower(store_brand) = 'fabletics' and lower(is_scrubs_customer) = true, 1, 0) as boolean) as fl_scrubs_session_flag,
    null as order_id,
    null as revenue,
    null as margin,
    null as order_units
from edw_prod.data_model.fact_registration c
join edw_prod.data_model.dim_customer dc on c.customer_id = dc.customer_id
join edw_prod.data_model.dim_store st on st.store_id = c.store_id
join reporting_media_base_prod.influencers.influencer_historical_mpid_hdyh_mapping ip on lower(ip.hdyh) = lower(c.how_did_you_hear)
    and ip.region = st.store_region
    and lower(st.store_brand_abbr) = case when lower(ip.business_unit_abbr) in ('flm','scb') then 'fl' else ip.business_unit_abbr end
where c.session_id is not null
    and ip.media_partner_id is not null
    and to_date(c.registration_local_datetime) >= '2019-01-01'
    and ifnull(c.is_fake_retail_registration, false) = false;

create or replace temporary table _hdyh_leads as
select
    media_partner_id,
    reporting_id,
    cast(case when mens_session_flag=1 then store_id || '011'
        when fl_scrubs_session_flag=1 then store_id || '022'
    else store_id end as int) as media_store_id,
    session_id,
    event_type,
    event_description,
    event_datetime,
    customer_id,
    order_id,
    revenue,
    margin,
    order_units
from _hdyh_leads_flag;

----------------------------------------------------------------------------
-- count all vips with influencer hdyh answers at lead registration

create or replace temporary table _hdyh_vips_flag as
select
    lower(ip.media_partner_id) as media_partner_id,
    cast(null as varchar(55)) as reporting_id,
    st.store_id,
    c.session_id as session_id, -- registration session_id
    'VIP' as event_type,
    'HDYH' as event_description,
    v.activation_local_datetime::TIMESTAMP_NTZ(9) as event_datetime,
    c.customer_id,
    cast(iff(lower(store_brand) = 'fabletics' and lower(gender) = 'm', 1, 0) as boolean) as mens_session_flag,
    cast(iff(lower(store_brand) = 'fabletics' and lower(is_scrubs_customer) = true, 1, 0) as boolean) as fl_scrubs_session_flag,
    v.order_id as order_id,
    coalesce(fo.product_gross_revenue_local_amount * fo.order_date_usd_conversion_rate,0) as revenue,
    coalesce(fo.product_order_cash_margin_pre_return_local_amount * fo.order_date_usd_conversion_rate,0) as margin,
    fo.unit_count as order_units
from edw_prod.data_model.fact_registration c
join edw_prod.data_model.dim_customer dc on c.customer_id = dc.customer_id
join edw_prod.data_model.fact_activation v on v.customer_id = c.customer_id
    and datediff(day, c.registration_local_datetime, v.activation_local_datetime) < 30
join edw_prod.data_model.fact_order fo on fo.order_id = v.order_id
    and fo.store_id = v.store_id
join edw_prod.data_model.dim_store st on st.store_id = c.store_id
join reporting_media_base_prod.influencers.influencer_historical_mpid_hdyh_mapping ip on lower(ip.hdyh) = lower(c.how_did_you_hear)
    and ip.region = st.store_region
    and lower(st.store_brand_abbr) = case when lower(ip.business_unit_abbr) in ('flm','scb') then 'fl' else ip.business_unit_abbr end
where ip.media_partner_id is not null
    and to_date(c.registration_local_datetime) >= '2019-01-01'
    and ifnull(c.is_fake_retail_registration, false) = false;

create or replace temporary table _hdyh_vips as
select
    media_partner_id,
    reporting_id,
    cast(case when mens_session_flag=1 then store_id || '011'
        when fl_scrubs_session_flag=1 then store_id || '022'
    else store_id end as int) as media_store_id,
    session_id,
    event_type,
    event_description,
    event_datetime,
    customer_id,
    order_id,
    revenue,
    margin,
    order_units
from _hdyh_vips_flag;

----------------------------------------------------------------------------
-- count all repeat VIP orders from influencer posts

create or replace temporary table _repeat_vip_orders as
select
    lower(cast(irmp as varchar(55))||'|'||coalesce(sharedid,'nosid')) as media_partner_id,
    cast(null as varchar(55)) as reporting_id,
    ds.store_id,
    p.session_id,
    'Repeat' as event_type,
    'Click Purchase' as event_description,
    fo.order_local_datetime::TIMESTAMP_NTZ(9) as event_datetime,
    fo.customer_id,
    p.mens_session_flag,
    p.fl_scrubs_session_flag,
    fo.order_id as order_id,

    coalesce(case when lower(ds.store_brand_abbr) in ('jf', 'sd', 'fk')
                  then (fo.subtotal_excl_tariff_local_amount - fo.product_discount_local_amount) * fo.order_date_usd_conversion_rate
                  else fo.product_gross_revenue_local_amount * fo.order_date_usd_conversion_rate
                  end, 0) as revenue,

    coalesce(case when lower(ds.store_brand_abbr) in ('jf', 'sd', 'fk')
                  then revenue - (fo.reporting_landed_cost_local_amount * fo.order_date_usd_conversion_rate)
                  else fo.product_margin_pre_return_local_amount * fo.order_date_usd_conversion_rate
                  end, 0) as margin,

    fo.unit_count as order_units
from _uri_pivoted p
join edw_prod.data_model.fact_order fo on fo.session_id = p.session_id
join edw_prod.data_model.dim_store ds on ds.store_id = fo.store_id
join edw_prod.data_model.dim_order_membership_classification m on m.order_membership_classification_key = fo.order_membership_classification_key
join edw_prod.data_model.dim_order_status os on os.order_status_key = fo.order_status_key
join edw_prod.data_model.dim_order_sales_channel osc on osc.order_sales_channel_key = fo.order_sales_channel_key
where lower(m.membership_order_type_l2) = 'repeat vip'
    and lower(os.order_status) in ('success','pending')
    and lower(osc.order_classification_l1) = 'product order';

create or replace temporary table _repeat_vip_orders_final as
select
    media_partner_id,
    reporting_id,
    cast(case when mens_session_flag=1 then store_id || '011'
        when fl_scrubs_session_flag=1 then store_id || '022'
    else store_id end as int) as media_store_id,
    session_id,
    event_type,
    event_description,
    event_datetime,
    customer_id,
    order_id,
    revenue,
    margin,
    order_units
from _repeat_vip_orders;

--------------------------------------------------------------------

create or replace transient table reporting_media_base_prod.influencers.click_conversion_counts
(
    media_partner_id VARCHAR ,
    reporting_id VARCHAR,
    media_store_id INT,
    session_id number(38,0),
    event_type VARCHAR,
    event_description VARCHAR,
    event_datetime DATETIME,
    customer_id NUMBER(38,0),
    order_id NUMBER(38,0),
    revenue DECIMAL(20,4),
    margin DECIMAL(20,4),
    order_units INT
);

insert into  reporting_media_base_prod.influencers.click_conversion_counts
    select l.*
    from _leads l
    left join  reporting_media_base_prod.influencers.click_conversion_counts c on c.customer_id = l.customer_id
        and c.event_type = l.event_type
    where c.media_partner_id is null;

insert into  reporting_media_base_prod.influencers.click_conversion_counts
    select l.*
    from _vips_from_leads30d l
    left join  reporting_media_base_prod.influencers.click_conversion_counts c on c.customer_id = l.customer_id
        and c.event_type = l.event_type
    where c.media_partner_id is null;

insert into  reporting_media_base_prod.influencers.click_conversion_counts
    select l.*
    from _vips_not_from_leads l
    left join  reporting_media_base_prod.influencers.click_conversion_counts c on c.customer_id = l.customer_id
        and c.event_type = l.event_type
    where c.media_partner_id is null;

insert into  reporting_media_base_prod.influencers.click_conversion_counts
    select l.*
    from _hdyh_leads l
    left join  reporting_media_base_prod.influencers.click_conversion_counts c on c.customer_id = l.customer_id
        and c.event_type = l.event_type
    where c.media_partner_id is null;

insert into  reporting_media_base_prod.influencers.click_conversion_counts
    select distinct l.*
    from _hdyh_vips l
    left join  reporting_media_base_prod.influencers.click_conversion_counts c on c.customer_id = l.customer_id
        and c.event_type = l.event_type
    where c.media_partner_id is null;

insert into reporting_media_base_prod.influencers.click_conversion_counts
    select *
    from _repeat_vip_orders_final;

insert into reporting_media_base_prod.influencers.click_conversion_counts
    select *
    from _session_count;


------------------------------------------------------------------------
-- cleanup historical utm values

create or replace temporary table _fix_conversions
(new_unique_id varchar(55),
old_unique_id varchar(55));

insert into _fix_conversions
    values
('1457730|nosid','1457730|karissaduncan'),
('1242537|loeylane','1242537|loeylaneyt'),
('1242537|loeylane','1242537|loeylaney'),
('1787016|taramichelle','1787016|nosid'),
('1790399|sierraschultzzie','1790399|schultzzie'),
('1810178|madisonpettis','1810178|madisonpettis?'),
('1809592|jalisavaughn','1809592|jalisaevaughn'),
('1242610|rachelmartino','1242610|nosid'),
('xx1804284|makaylalondon','1804284|makaylalondon'),
('2209184|nosid','1939855|snitchery'),
('2209179|nosid','1939855|loeylane'),
('2210595|vivianvo','1304717|vivianvofarmer'),
('2210595|vivianvo','1304717|vivianvo'),
('2210595|vivianvoyt','1304717|vivianvoyt'),
('2210595|vivianvo','1304717|vivianvo?'),
('2210584|ashleynichole','1304717|ashleynichole'),
('2210584|ashleynicholeyt','1304717|ashleynicholeyt'),
('2210584|ashleynichole','1304717|ashleynichole?'),
('2216316|danielleebrownn','1810916|danielleebrownn'),
('2216316|danielleebrownn','1810916|danielleebrownn?'),
('2216302|michelleinfusino','1810916|michelleinfusino'),
('2216302|michelleinfusino','1810916|michelleinfusino?'),
('2214856|whitneyfransway','1810916|whitneyfransway'),
('2214856|whitneyfransway','1810916|whitneyfransway?'),
('2209463|uchenwosu','1810916|uchenwosu'),
('2209463|uchenwosu','1810916|uchenwosu?'),
('2210524|sierranielsen','1458838|sierranielsen'),
('2210524|sierranielsen','1458838|sierranielsen?'),
('2454658|nosid','1723547|nosid'),
('2549373|nosid','2504979|nosid');

update reporting_media_base_prod.influencers.click_conversion_counts
    set media_partner_id = new_unique_id
from _fix_conversions where lower(_fix_conversions.old_unique_id) = lower(media_partner_id);
