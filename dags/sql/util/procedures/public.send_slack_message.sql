USE DATABASE UTIL;
USE SCHEMA PUBLIC;

CREATE OR REPLACE PROCEDURE util.public.send_slack_message(MSG string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (slack_webhook_access_integration)
SECRETS = ('slack_url' = slack_app_webhook_url)
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark
import json
import requests
import _snowflake
from datetime import date

def main(session, msg):
    # Retrieve the Webhook URL from the SECRET object
    webhook_url = _snowflake.get_generic_secret_string('slack_url')
    slack_data = {
     "text": f"Snowflake says: {msg}"
    }
    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )

    return "SUCCESS"
$$;
