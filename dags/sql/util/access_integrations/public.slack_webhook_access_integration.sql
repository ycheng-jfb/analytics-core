CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION slack_webhook_access_integration
    ALLOWED_NETWORK_RULES = (slack_webhook_network_rule)
    ALLOWED_AUTHENTICATION_SECRETS = (slack_app_webhook_url)
    ENABLED = true;
