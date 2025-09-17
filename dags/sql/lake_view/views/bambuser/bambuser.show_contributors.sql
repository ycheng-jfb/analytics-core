CREATE VIEW IF NOT EXISTS lake_view.bambuser.show_contributors AS
SELECT
    s.store_group,
    s.show_id,
    c.value::VARCHAR AS CONTRIBUTOR_ID
FROM lake.bambuser.shows s,
lateral flatten(input => s.CONTRIBUTORS) c;
