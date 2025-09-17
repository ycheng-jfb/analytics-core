from functools import cached_property

import requests
from airflow.hooks.base import BaseHook


class BuilderHook(BaseHook):
    def __init__(self, builder_conn_id=None, api_key=None):
        conn = self.get_connection(builder_conn_id)
        self.api_key = api_key or conn.password
        self.url = conn.host

    @cached_property
    def session(self):
        headers = {"Content-Type": "application/json"}
        session = requests.session()
        session.params = {"apiKey": self.api_key}
        session.headers = headers
        return session

    def make_request(
        self,
        path: str,
        method: str = "GET",
        params: dict = None,
        data: dict = None,
        files: dict = None,
    ):
        response = self.session.request(
            method=method,
            url=self.url + "/" + path,
            params=params,
            data=data,
            files=files,
        )
        response.raise_for_status()
        return response
