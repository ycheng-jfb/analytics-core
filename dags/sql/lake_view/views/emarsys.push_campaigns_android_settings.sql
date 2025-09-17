CREATE VIEW IF NOT EXISTS LAKE_VIEW.EMARSYS.PUSH_CAMPAIGNS_ANDROID_SETTINGS AS
SELECT
  CAMPAIGN_ID,
  CUSTOMER_ID,
  NAME,
  EVENT_TIME,
  LOADED_AT,
  TRIM(a_v.value[0]:v, '"') as android_settings_key,
  TRIM(a_v.value[1]:v, '"') as android_settings_value
from lake.emarsys.push_campaigns p
, lateral flatten(input => p.android_settings) a
, lateral flatten(input => a.value:v) a_v;
