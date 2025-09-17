CREATE VIEW IF NOT EXISTS lake_view.bambuser.user_roles AS
SELECT
    u.store_group,
    u.id AS USER_ID,
    r.value::VARCHAR AS ROLE
FROM lake.bambuser.users u,
lateral flatten(input => u.ROLES) r;
