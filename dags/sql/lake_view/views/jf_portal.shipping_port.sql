CREATE VIEW IF NOT EXISTS lake_view.jf_portal.shipping_port AS
SELECT
   shipping_port_id,
   country_origin,
   city_origin,
   date_create,
   date_update,
   preferred_day_of_week,
   preferred_dow_origin_country_id,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.shipping_port;
