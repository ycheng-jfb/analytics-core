import base64

import requests
from airflow.hooks.base import BaseHook
from functools import cached_property
from include.config import conn_ids


class LithiumHook(BaseHook):
    def __init__(self, lithium_conn_id=conn_ids.Lithium.default):
        self.lithium_conn_id = lithium_conn_id

    @cached_property
    def session(self):
        conn = self.get_connection(self.lithium_conn_id)
        login = conn.login
        password = conn.password
        creds_bytes = f"{login}:{password}".encode("utf-8")
        encoded = base64.b64encode(creds_bytes).decode("utf-8")
        headers = {"Accept": "application/json", "Authorization": f"Basic {encoded}"}

        s = requests.session()
        s.headers.update(headers)

        return s

    def make_request(self, url: str, params: dict = None):
        try:
            r = self.session.request(method="GET", url=url, params=params)
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError as e:
            print(f"headers: {r.headers}")
            print(f"content: {r.text}")
            print(f"status_code: {r.status_code}")
            raise e
