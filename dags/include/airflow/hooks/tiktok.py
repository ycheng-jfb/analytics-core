import requests
from airflow.hooks.base import BaseHook
from functools import cached_property
from include.config import conn_ids


class TiktokHook(BaseHook):
    def __init__(self, tiktok_conn_id=conn_ids.Tiktok.default, api_key=None):
        conn = self.get_connection(tiktok_conn_id)
        self.api_key = api_key or conn.password
        self.url = conn.host

    @cached_property
    def session(self):
        headers = {"Access-Token": self.api_key, 'Content-Type': 'application/json'}
        session = requests.session()
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
        if path == 'open_api/v1.2/dmp/custom_audience/file/upload/':
            self.session.headers.pop('Content-Type', 'not_found')
        response = self.session.request(
            method=method, url=self.url + '/' + path, params=params, data=data, files=files
        )
        response.raise_for_status()
        return response
