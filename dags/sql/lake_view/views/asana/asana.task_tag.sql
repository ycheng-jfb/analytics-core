create or replace view LAKE_VIEW.ASANA.TASK_TAG as (
    select
        TASK_ID,
        TAG_ID,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.TASK_TAG
);
