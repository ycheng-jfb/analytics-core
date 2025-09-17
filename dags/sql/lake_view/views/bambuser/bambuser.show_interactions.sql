CREATE VIEW IF NOT EXISTS lake_view.bambuser.show_interactions AS
SELECT
    s.store_group,
    s.show_id,
    t.key AS INTERACTION_TYPE,
    i.key AS INTERACTION,
    i.value:total::NUMBER AS TOTAL_COUNT,
    i.value:unique::NUMBER AS UNIQUE_COUNT
FROM lake.bambuser.shows s,
lateral flatten(input => s.INTERACTIONS) t,
lateral flatten(input => t.value) i;
