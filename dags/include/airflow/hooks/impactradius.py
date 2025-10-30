import requests
from airflow.hooks.base import BaseHook
from functools import cached_property


class ImpactRadiusHook(BaseHook):
    def __init__(
        self, impactradius_conn_id="impactradius_conversions", api_key=None, username=None
    ):
        conn = self.get_connection(impactradius_conn_id)
        self.password = api_key or conn.password
        self.username = username or conn.login
        self.url = conn.host

    @cached_property
    def session(self):
        session = requests.Session()
        session.headers = {
            "Content-type": "application/json",
            'Authorization': f'Basic {self.password}',
        }
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
            url='https://' + self.url + '/Advertisers/' + self.username + path,
            params=params,
            data=data,
            files=files,
        )
        response.raise_for_status()
        return response
