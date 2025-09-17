CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_CJ_PLACEMENT_COSTS as
select date::date as date,
    business_unit,
    region,
    country,
    store_id,
    id,
    placement_cost,
    is_mens_flag,
    vendor_name,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.cj_placement_costs_flna_f
union
select date::date as date,
    business_unit,
    region,
    country,
    store_id,
    id,
    placement_cost,
    is_mens_flag,
    vendor_name,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.cj_placement_costs_flna_m
union
select date::date as date,
    business_unit,
    region,
    country,
    store_id,
    id,
    placement_cost,
    is_mens_flag,
    vendor_name,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.cj_placement_costs_ytna
union
select date::date as date,
    business_unit,
    region,
    country,
    store_id,
    id,
    placement_cost,
    is_mens_flag,
    vendor_name,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.cj_placement_costs_sxna
union
select date::date as date,
    business_unit,
    region,
    country,
    store_id,
    id,
    placement_cost,
    is_mens_flag,
    vendor_name,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.cj_placement_costs_jfna
union
select date::date as date,
    business_unit,
    region,
    country,
    store_id,
    id,
    placement_cost,
    is_mens_flag,
    vendor_name,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.cj_placement_costs_sdna
union
select date::date as date,
    business_unit,
    region,
    country,
    store_id,
    id,
    placement_cost,
    is_mens_flag,
    vendor_name,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.cj_placement_costs_fkna;
