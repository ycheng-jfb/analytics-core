import requests
from airflow.hooks.base import BaseHook
from functools import cached_property
from include.config import conn_ids

DEFAULT_API_VERSION = "v1"


class NorthbeamHook(BaseHook):
    def __init__(self, northbeam_conn_id=conn_ids.Northbeam.default):
        conn = self.get_connection(northbeam_conn_id)
        self.client_id = conn.login
        self.api_key = conn.password
        self.base_url = conn.host

    @cached_property
    def session(self):
        headers = {
            "Authorization": f"Basic {self.api_key}",
            "Data-Client-ID": self.client_id,
            'Content-Type': 'application/json',
        }
        session = requests.session()
        session.headers = headers
        return session

    def make_request(self, endpoint: str, payload: list = None):
        response = self.session.post(
            url=f'https://{self.base_url}/{DEFAULT_API_VERSION}/{endpoint}', json=payload
        )
        response.raise_for_status()
        return response.text
