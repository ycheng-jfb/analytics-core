create or replace view LAKE_VIEW.ASANA.CUSTOM_FIELD as (
    select
        ID,
        NAME,
        TYPE,
        DESCRIPTION,
        PORTFOLIO_ID,
        PROJECT_ID,
        WORKSPACE_ID,
        _FIVETRAN_DELETED,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.CUSTOM_FIELD
);
