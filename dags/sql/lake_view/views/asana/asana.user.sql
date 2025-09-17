create or replace view LAKE_VIEW.ASANA.USER as (
    select
        ID,
        NAME,
        EMAIL,
        _FIVETRAN_DELETED,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.USER
);
