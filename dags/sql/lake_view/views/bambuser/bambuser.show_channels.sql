CREATE VIEW IF NOT EXISTS lake_view.bambuser.show_channels AS
SELECT
    s.store_group,
    s.show_id,
    c.value::VARCHAR AS CHANNEL_ID
FROM lake.bambuser.shows s,
lateral flatten(input => s.CHANNELS) c;
