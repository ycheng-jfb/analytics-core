create or replace view LAKE_VIEW.ASANA.ATTACHMENT as (
    select
        ID,
        HOST,
        NAME,
        VIEW_URL,
        CREATED_AT,
        DOWNLOAD_URL,
        PARENT_ID,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.ASANA_ALL_PROJECTS.ATTACHMENT
);
