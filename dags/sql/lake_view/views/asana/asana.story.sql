create or replace view LAKE_VIEW.ASANA.STORY as (
    select
        ID,
        TYPE,
        TEXT,
        SOURCE,
        CREATED_AT,
        HEARTED,
        NUM_HEARTS,
        CREATED_BY_ID,
        TARGET_ID,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.STORY
);
