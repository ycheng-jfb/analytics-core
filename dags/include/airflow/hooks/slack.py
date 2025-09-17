# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import json

import pendulum
import requests
from airflow.hooks.base import BaseHook
from functools import cached_property

from include.utils.context_managers import TempLoggingWarning


class SlackHook(BaseHook):
    """
    This hook allows you to post messages to Slack using incoming webhooks.
    Takes both Slack webhook token directly and connection that has Slack webhook token.
    If both supplied, http_conn_id will be used as base_url,
    and webhook_token will be taken as endpoint, the relative path of the url.

    Each Slack webhook token can be pre-configured to use a specific channel, username and
    icon. You can override these defaults in this hook.

    :param http_conn_id: connection that has Slack webhook token in the extra field
    :type http_conn_id: str
    :param webhook_token: Slack webhook token
    :type webhook_token: str
    :param message: The message you want to send on Slack
    :type message: str
    :param attachments: The attachments to send on Slack. Should be a list of
                        dictionaries representing Slack attachments.
    :type attachments: list
    :param channel: The channel the message should be posted to
    :type channel: str
    :param username: The username to post to slack with
    :type username: str
    :param icon_emoji: The emoji to use as icon for the user posting to Slack
    :type icon_emoji: str
    :param link_names: Whether or not to find and link channel and usernames in your
                       message
    :type link_names: bool
    :param proxy: Proxy to use to make the Slack webhook call
    :type proxy: str
    """

    def __init__(self, slack_conn_id=None, *args, **kwargs):
        self.slack_conn_id = slack_conn_id

    @cached_property
    def oauth_token(self):
        conn = self.get_connection("slack_app")
        return conn.password

    def get_token(self):
        """
        Given either a manually set token or a conn_id, return the webhook_token to use
        :param token: The manually provided token
        :type token: str
        :param http_conn_id: The conn_id provided
        :type http_conn_id: str
        :return: webhook_token (str) to use
        """
        with TempLoggingWarning():
            conn = self.get_connection(self.slack_conn_id)
        extra = conn.extra_dejson
        return extra.get("webhook_token")

    def send_message(
        self,
        message,
        attachments=None,
        channel=None,
        username=None,
        icon_emoji=None,
        link_names=False,
        thread_ts=None,
    ):
        """
        Construct the Slack message. All relevant parameters are combined here to a valid
        Slack json message
        :return: Slack message (str) to send
        """
        cmd = {}

        if channel:
            cmd["channel"] = channel
        if username:
            cmd["username"] = username
        if icon_emoji:
            cmd["icon_emoji"] = icon_emoji
        if link_names:
            cmd["link_names"] = 1
        if attachments:
            cmd["attachments"] = attachments
        if thread_ts:
            cmd["thread_ts"] = thread_ts

        cmd["text"] = message

        base_url = "https://hooks.slack.com/services"
        webhook_token = self.get_token()
        requests.post(
            url=f"{base_url}/{webhook_token}",
            data=json.dumps(cmd),
            headers={"Content-type": "application/json"},
        )

    def get_channel_id(self, channel_name):
        url = "https://slack.com/api/conversations.list"
        params = {"token": self.oauth_token}
        while True:
            r = requests.get(url, params)
            for channel in r.json()["channels"]:
                if channel["name"] == channel_name:
                    return channel["id"]
            params["cursor"] = r.json()["response_metadata"]["next_cursor"]
            if not params[
                "cursor"
            ]:  # Cursor will return an empty string once it reaches end
                raise ValueError(f"Could not find channel {channel_name}")

    def get_message(self, channel_id, dag_id, data_interval_start):
        url = "https://slack.com/api/conversations.history"
        params = {
            "token": self.oauth_token,
            "channel": channel_id,
            "oldest": pendulum.DateTime.utcnow().add(days=-1).timestamp(),
        }
        while True:
            r = requests.get(url, params)
            for message in r.json()["messages"]:
                if (
                    dag_id in message["text"]
                    and str(data_interval_start) in message["text"]
                ):
                    return message
            if not r.json()["has_more"]:
                break
            params["cursor"] = r.json()["response_metadata"]["next_cursor"]

    def send_alert(self, message, channel_name, dag_id, data_interval_start):
        """
        This will check if a message already exists for a dag_id and execution date. If it does,
        it will reply to that thread. If it doesn't, it will send a new message.

        https://api.slack.com/messaging/retrieving#finding_threads
        """
        channel_id = self.get_channel_id(channel_name)
        existing_message = self.get_message(channel_id, dag_id, data_interval_start)
        if existing_message:
            self.send_message(
                message.replace("<!here>", ""), thread_ts=existing_message["ts"]
            )
        else:
            self.send_message(message)
