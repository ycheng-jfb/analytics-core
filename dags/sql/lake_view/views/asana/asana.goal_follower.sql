create or replace view LAKE_VIEW.ASANA.GOAL_FOLLOWER as (
    select
        GOAL_ID,
        USER_ID,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.GOAL_FOLLOWER
);
