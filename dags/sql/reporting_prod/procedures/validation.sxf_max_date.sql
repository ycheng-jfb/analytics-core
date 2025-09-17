select * from (
select 'reporting_prod.sxf.style_master'as Table_name ,coalesce(max((load_datetime::date)),'1900-01-01')  as max_date,current_date()-max_date  as diff_date from reporting_prod.sxf.style_master
union all
select  'reporting_prod.sxf.prebreak_size_scales'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01')  as max_date,current_date()-max_date as diff_date  from reporting_prod.sxf.prebreak_size_scales
union all
select  'reporting_prod.sxf.cohort_waterfall'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01')  as max_date,current_date()-max_date as diff_date  from reporting_prod.sxf.cohort_waterfall
union all
select  'reporting_prod.sxf.cohort_waterfall_customer'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01')  as max_date,current_date()-max_date as diff_date  from reporting_prod.sxf.cohort_waterfall_customer
union all
select  'reporting_prod.sxf.crm_contribution'as Table_name ,coalesce(max((date_day::date)),'1900-01-01')  as max_date,current_date()-max_date as diff_date  from reporting_prod.sxf.crm_contribution
union all
select  'reporting_prod.sxf.crm_optimization_dataset'as Table_name ,coalesce(max((send_date::date)),'1900-01-01') as max_date,current_date()-max_date as diff_date  from reporting_prod.sxf.crm_optimization_dataset
union all
select  'reporting_prod.sxf.custom_segment_waterfall_growth'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01')  as max_date,current_date()-max_date as diff_date  from reporting_prod.sxf.custom_segment_waterfall_growth
union all
select  'reporting_prod.sxf.order_line_dataset'as Table_name ,coalesce(max((order_local_datetime::date)),'1900-01-01') as max_date,current_date()-max_date as diff_date  from reporting_prod.sxf.order_line_dataset
union all
select  'reporting_prod.sxf.retail_attribution'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01') as max_date,current_date()-max_date as diff_date  from reporting_prod.sxf.retail_attribution
union all
select  'reporting_prod.sxf.retail_traffic'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01') as max_date,current_date()-max_date as diff_date  from reporting_prod.sxf.retail_traffic
union all
select  'reporting_prod.sxf.custom_segment_waterfall'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01') as max_date,current_date()-max_date as diff_date  from reporting_prod.sxf.custom_segment_waterfall
union all
select  'reporting_prod.data_science.export_postreg_segment_spi_ranking'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01') as max_date,current_date()-max_date as diff_date  from reporting_prod.data_science.export_postreg_segment_spi_ranking
WHERE META_COMPANY_ID = 30
union all
select  'reporting_prod.sxf.daily_bop_eop_counts'as Table_name ,coalesce(max((date::date)),'1900-01-01')  as max_date,current_date()-max_date as diff_date  from reporting_prod.sxf.daily_bop_eop_counts
union all
select  'lake_view.sharepoint.med_sxf_core_basic_key'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01')  as max_date,current_date()-max_date as diff_date  from lake_view.sharepoint.med_sxf_core_basic_key
union all
select  'lake_view.sharepoint.med_sxf_savage_showroom'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01')   as max_date,current_date()-max_date as diff_date  from lake_view.sharepoint.med_sxf_savage_showroom
union all
select  'lake_view.sharepoint.med_sxf_crm_dimensions'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01')  as max_date,current_date()-max_date as diff_date  from lake_view.sharepoint.med_sxf_crm_dimensions
union all
select  'lake_view.sharepoint.med_sxf_fabric_key'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01')  as max_date,current_date()-max_date as diff_date  from lake_view.sharepoint.med_sxf_fabric_key
union all
select  'lake_view.sharepoint.med_sxf_merch_stylemaster'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01')   as max_date,current_date()-max_date as diff_date  from lake_view.sharepoint.med_sxf_merch_stylemaster
union all
select  'lake_view.sharepoint.med_sxf_pricing_key'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01')  as max_date,current_date()-max_date as diff_date  from lake_view.sharepoint.med_sxf_pricing_key
union all
select  'lake_view.sharepoint.med_sxf_category_type_key'as Table_name ,coalesce(max((meta_update_datetime::date)),'1900-01-01')  as max_date,current_date()-max_date as diff_date  from lake_view.sharepoint.med_sxf_category_type_key
union all
select  'reporting_base_prod.data_science.product_recommendation_tags'as Table_name ,coalesce(max((meta_create_datetime::date)),'1900-01-01') as max_date,current_date()-max_date as diff_date  from reporting_base_prod.data_science.product_recommendation_tags
)
where diff_date >1
