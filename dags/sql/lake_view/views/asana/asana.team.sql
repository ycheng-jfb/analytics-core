create or replace view LAKE_VIEW.ASANA.TEAM as (
    select
        ID,
        NAME,
        ORGANIZATION_ID,
        _FIVETRAN_DELETED,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.TEAM
);
