create or replace view LAKE_VIEW.ASANA.WORKSPACE as (
    select
        ID,
        NAME,
        IS_ORGANIZATION,
        _FIVETRAN_DELETED,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.WORKSPACE
);
