from functools import cached_property

import requests
from airflow.hooks.base import BaseHook
from include.config import conn_ids


class CallMinerBulkHook(BaseHook):
    def __init__(
        self,
        conn_id="callminer_bulk_default",
        base_url="https://api.callminer.net/bulkexport/api",
    ):
        self.conn_id = conn_id
        self.conn = self.get_connection(self.conn_id)
        self.extras = self.conn.extra_dejson

        self.client_id = self.conn.login
        self.client_secret = self.conn.password
        self.job_id = self.extras.get("job_id")
        self.base_url = base_url or self.conn.base_url or self.extras.get("host")

    @cached_property
    def session(self):
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": "https://callminer.net/auth/platform-bulkexport https://callminer.net/auth/"
            "platform-webhook",
        }
        print("Generating an Access Token")

        response = requests.post("https://idp.callminer.net/connect/token", data=data)
        session = requests.session()
        response.raise_for_status()
        session.headers = {
            "Accept": "application/json",
            "Content-type": "application/json",
        }
        rdict = response.json()
        access_token = rdict["access_token"]
        session.headers.update({"Authorization": f"Bearer {access_token}"})
        return session

    def make_request(
        self,
        method,
        endpoint,
        headers=None,
        params=None,
        json=None,
        data=None,
        stream=None,
    ):
        endpoint = endpoint[1:] if endpoint[0] == "/" else endpoint
        base_url = self.base_url[:-1] if self.base_url[-1] == "/" else self.base_url
        url = f"{base_url}/{endpoint}"

        r = self.session.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=json,
            data=data,
            stream=stream,
        )
        r.raise_for_status()
        return r
