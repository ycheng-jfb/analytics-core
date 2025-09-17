import base64
import time

import requests
from airflow.hooks.base import BaseHook
from functools import cached_property
from urllib.parse import unquote
from include.config import conn_ids


class ZendeskHook(BaseHook):
    def __init__(
        self,
        base_url="https://techstyletap.zendesk.com/api",
        version="v2",
        zendesk_conn_id=conn_ids.Zendesk.default,
    ):
        self.base_url = base_url
        self.version = version

        self.zendesk_conn_id = zendesk_conn_id
        self.conn = self.get_connection(zendesk_conn_id)
        self.client_id = self.conn.login
        self.client_secret = self.conn.password

    @cached_property
    def session(self):
        access_token = base64.b64encode(
            bytes(self.client_id + "/token:" + self.client_secret, "ISO-8859-1")
        ).decode("ascii")
        token_header = {"Authorization": f"Basic {access_token}"}
        session = requests.session()
        session.headers = token_header
        return session

    @staticmethod
    def raise_for_status(response):
        if response.status_code not in (200, 429):
            print(f"headers: {response.headers}")
            print(f"content: {response.content}")
            print(f"status_code: {response.status_code}")
            response.raise_for_status()
            raise Exception(response.content)

    def make_request(self, url, params=None):
        response = self.session.get(url, params=params)
        self.raise_for_status(response)
        if response.status_code == 429:
            retry_after = response.headers["Retry-After"]
            print(
                f"No. of hits exceeded for the endpoint: {url}. "
                f"Wait for {retry_after} seconds..."
            )
            time.sleep(int(retry_after) + 1)
            response = self.session.get(url)
        return response

    def get_response_all_pages(self, endpoint, params=None):
        endpoint = endpoint[1:] if endpoint[0] == "/" else endpoint
        url = f"{self.base_url}/{self.version}/{endpoint}.json"
        while url:
            wait_time = 1
            max_attempts = 5
            for attempt in range(max_attempts):
                try:
                    response = self.make_request(url, params)
                    break
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 503:  # Database timeout error
                        print(
                            f"Database timeout error, retrying in {wait_time} seconds..."
                        )
                        time.sleep(wait_time)
                        wait_time *= 2  # Double the wait time for the next attempt
                    else:
                        raise  # Some other error occurred, re-raise the exception
            data = response.json()
            for dict in data[endpoint]:
                yield dict
            if data["meta"]["has_more"]:
                url = data["links"]["next"]
                params = {}
            else:
                url = ""
