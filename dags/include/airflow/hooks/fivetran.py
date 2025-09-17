import requests
from airflow.hooks.base import BaseHook
from functools import cached_property
from include.config import conn_ids


class FivetranHook(BaseHook):
    def __init__(self, fivetran_conn_id=conn_ids.Fivetran.default, api_key=None):
        conn = self.get_connection(fivetran_conn_id)
        self.api_key = api_key or conn.password
        self.url = conn.host

    @cached_property
    def session(self):
        headers = {"Authorization": f'Basic {self.api_key}', 'Content-Type': 'application/json'}
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
        response = self.session.request(
            method=method, url=self.url + '/' + path, params=params, data=data, files=files
        )
        response.raise_for_status()
        return response
