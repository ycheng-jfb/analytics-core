TRUNCATE TABLE validation.utm_medium_and_utm_source_duplicates;

SET execution_start_datetime = CURRENT_TIMESTAMP;

INSERT INTO validation.utm_medium_and_utm_source_duplicates

SELECT utm_medium, utm_source, brand, utm_campaign, referrer, COUNT(1) AS duplicate_count
FROM lake_view.sharepoint.media_source_channel_mapping_adj
GROUP BY utm_medium, utm_source, brand, utm_campaign, referrer
HAVING COUNT(1) > 1;
