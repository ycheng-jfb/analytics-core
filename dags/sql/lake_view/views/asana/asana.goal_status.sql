create or replace view LAKE_VIEW.ASANA.GOAL_STATUS as (
    select
        ID,
        GOAL_ID,
        CREATED_AT,
        MODIFIED_AT,
        STATUS_TYPE,
        TEXT,
        TITLE,
        CREATED_BY_ID,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.GOAL_STATUS
);
