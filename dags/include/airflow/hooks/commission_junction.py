import requests
from airflow.hooks.base import BaseHook
from functools import cached_property


class CommissionJunctionHook(BaseHook):
    def __init__(self, commissionjunction_conn_id, access_token=None):
        conn = self.get_connection(commissionjunction_conn_id)
        self.access_token = access_token or conn.password
        self.url = conn.host

    @cached_property
    def session(self):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "text/plain",
        }
        session = requests.session()
        session.headers = headers
        return session

    def make_request(
        self,
        path: str,
        payload: dict = None,
    ):
        response = self.session.request(
            method="POST", url=self.url + "/" + path, data=payload
        )
        response.raise_for_status()
        return response
