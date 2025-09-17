create or replace view LAKE_VIEW.ASANA.TAG_FOLLOWER as (
    select
        TAG_ID,
        USER_ID,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.TAG_FOLLOWER
);
