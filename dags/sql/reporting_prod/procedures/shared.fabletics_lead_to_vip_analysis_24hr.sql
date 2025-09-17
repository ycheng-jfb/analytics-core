
create or replace transient table reporting_prod.shared.fabletics_lead_to_vip_analysis_24hr as
select
    case when lower(st.store_brand) = 'fabletics' and lower(dc.gender) = 'm' and is_scrubs_customer = true then 'Fabletics Scrubs Men'
         when lower(st.store_brand) = 'fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs Women'
         when lower(st.store_brand) = 'fabletics' and lower(dc.gender) = 'm' then 'Fabletics Men'
         when lower(st.store_brand) = 'fabletics' then 'Fabletics Women'
    else st.store_brand end as store_brand,
    ifnull(svm.channel,'unclassified') as channel,

    is_reactivated_lead,
    case when datediff(day,m.datetime_added,cl.datetime_added) < 90 then '0-3M'
            when datediff(day,m.datetime_added,cl.datetime_added) < 180 then '3M-6M'
            when datediff(day,m.datetime_added,cl.datetime_added) < 360 then '6M-1Y'
            when datediff(day,m.datetime_added,cl.datetime_added) < 720 then '1Y-2Y'
            when datediff(day,m.datetime_added,cl.datetime_added) < 1080 then '2Y-3Y'
            else '3+Y' end as reactivated_lead_tenure,

    ifnull(case when --lower(st2.store_type) = 'retail' or
              fr.is_retail_registration = true then 'Retail'
        else svm.platform end, 'Unknown') as platform,
    ifnull(svm.operatingsystem,'Unknown') as operatingsystem,

    bottom_size as raw_bottom_size,
    top_size as raw_top_size,
    bra_size as raw_bra_size,

    bottom_size_group as bottom_size,
    top_size_group as top_size,
    bra_size_group as bra_size,

    age_estimate_group,

    st.store_region,
    st.store_country,
    fr.registration_local_datetime::date as registration_date,

    count(*) as primary_leads,

    count(iff(datediff(hour,fr.registration_local_datetime,fa.activation_local_datetime) < 2,fa.customer_id,null)) as vips_2hr,
    count(fa.customer_id) as vips_24hr,

    count(iff(lower(st2.store_type) != 'retail'
                  and fr.is_retail_registration = false
                  and datediff(hour,fr.registration_local_datetime,fa.activation_local_datetime) < 2, fa.customer_id, null)) as online_vips_2hr,

    count(iff(lower(st2.store_type) != 'retail'
                  and fr.is_retail_registration = false, fa.customer_id, null)) as online_vips_24hr,

    count(iff(lower(st2.store_type) != 'retail'
                  and fr.is_retail_registration = false
                  and datediff(hour,fr.registration_local_datetime,fa.activation_local_datetime) < 2
                  and is_reactivated_vip = true, fa.customer_id, null)) as online_reactivated_vips_2hr

from edw_prod.data_model.fact_registration fr
join reporting_base_prod.fabletics.vw_dim_customer_ext dc on dc.customer_id = edw_prod.stg.udf_unconcat_brand(fr.customer_id)
join edw_prod.data_model.dim_store st on st.store_id = fr.store_id
left join lake_consolidated_view.ultra_merchant.customer_link cl on cl.current_customer_id = fr.customer_id
left join lake_consolidated_view.ultra_merchant.membership m on m.customer_id = cl.original_customer_id
left join reporting_base_prod.shared.session_single_view_media svm on svm.session_id = fr.session_id
left join edw_prod.data_model.fact_activation fa on fa.customer_id = fr.customer_id
    and datediff(hour,fr.registration_local_datetime,fa.activation_local_datetime) < 24
left join  edw_prod.data_model.dim_store st2 on st2.store_id = fa.sub_store_id

where lower(st.store_brand) in ('fabletics','yitty')
    and is_fake_retail_registration = false
    and is_secondary_registration = false
    and fr.registration_local_datetime >= '2021-01-01'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16;
