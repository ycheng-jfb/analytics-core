create or replace view LAKE_VIEW.GOOGLE_ANALYTICS.USER_PERMISSIONS(
	ID,
	USER_ID,
	USER_EMAIL,
	ENTITY_TYPE,
	ENTITY_ID,
	ENTITY_NAME,
	entity_property_id,
	PERMISSIONS_EFFECTIVE,
	PERMISSIONS_LOCAL,
	META_CREATE_DATETIME,
	META_UPDATE_DATETIME
) as
SELECT
    id,
    user_id,
    user_email,
    CASE
        WHEN account_id IS NOT NULL
        THEN 'account'
        WHEN property_id IS NOT NULL
        THEN 'property'
        ELSE 'view'
    END AS entity_type,
    CASE
        WHEN account_id IS NOT NULL
        THEN account_id::VARCHAR
        WHEN property_id IS NOT NULL
        THEN property_id
        ELSE view_id::VARCHAR
    END AS entity_id,
    CASE
        WHEN account_id IS NOT NULL
        THEN account_name
        WHEN property_id IS NOT NULL
        THEN property_name
        ELSE view_name
    END AS entity_name,
    CASE
        WHEN account_id IS NOT NULL
        THEN NULL
        WHEN property_id IS NOT NULL
        THEN property_id
        ELSE view_webPropertyId
    END AS entity_property_id,
    permissions_effective,
    permissions_local,
    meta_create_datetime,
    meta_update_datetime
    FROM lake.google_analytics.user_permissions;
