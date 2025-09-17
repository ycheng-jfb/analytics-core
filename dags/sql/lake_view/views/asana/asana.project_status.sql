create or replace view LAKE_VIEW.ASANA.PROJECT_STATUS as (
    select
        ID,
        PROJECT_ID,
        CREATED_AT,
        MODIFIED_AT,
        COLOR,
        TEXT,
        TITLE,
        CREATED_BY_ID,
        AUTHOR_ID,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.PROJECT_STATUS
);
