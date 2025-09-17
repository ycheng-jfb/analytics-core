create or replace view LAKE_VIEW.ASANA.SECTION as (
    select
        ID,
        NAME,
        PROJECT_ID,
        CREATED_AT,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.SECTION
);
