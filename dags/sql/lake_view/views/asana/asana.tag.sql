create or replace view LAKE_VIEW.ASANA.TAG as (
    select
        ID,
        CREATED_AT,
        NAME,
        COLOR,
        NOTES,
        MESSAGE,
        WORKSPACE_ID,
        _FIVETRAN_DELETED,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.TAG
);
