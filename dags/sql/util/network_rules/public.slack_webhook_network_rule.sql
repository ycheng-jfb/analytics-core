CREATE OR REPLACE NETWORK RULE slack_webhook_network_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('hooks.slack.com');
