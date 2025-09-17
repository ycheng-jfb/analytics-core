CREATE VIEW IF NOT EXISTS LAKE_VIEW.EMARSYS.PUSH_CAMPAIGNS_IOS_SETTINGS AS
SELECT
  CAMPAIGN_ID,
  CUSTOMER_ID,
  NAME,
  EVENT_TIME,
  LOADED_AT,
  TRIM(ios_v.value[0]:v, '"') as ios_settings_key,
  TRIM(ios_v.value[1]:v, '"') as ios_settings_value
from lake.emarsys.push_campaigns p
, lateral flatten(input => p.ios_settings) ios
, lateral flatten(input => ios.value:v) ios_v;
