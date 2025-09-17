USE DATABASE UTIL;
USE SCHEMA PUBLIC;


CREATE OR REPLACE SECRET slack_app_webhook_url
    type = GENERIC_STRING
    secret_string = '<slack_app_webhook_url>';
