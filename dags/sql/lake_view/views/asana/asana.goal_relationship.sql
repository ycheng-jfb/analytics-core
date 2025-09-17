create or replace view LAKE_VIEW.ASANA.GOAL_RELATIONSHIP as (
    select
        GOAL_ID,
        ID,
        CONTRIBUTION_WEIGHT,
        RESOURCE_SUBTYPE,
        SUPPORTING_RESOURCE_GID,
        SUPPORTING_RESOURCE_NAME,
        SUPPORTING_RESOURCE_RESOURCE_TYPE,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.GOAL_RELATIONSHIP
);
