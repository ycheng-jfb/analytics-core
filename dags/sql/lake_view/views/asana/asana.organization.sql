create or replace view LAKE_VIEW.ASANA.ORGANIZATION as (
    select
        ID,
        NAME,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.ORGANIZATION
);
