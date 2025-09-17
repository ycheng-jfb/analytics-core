import requests
from airflow.hooks.base import BaseHook
from functools import cached_property
from include.config import conn_ids


class AmazonHook(BaseHook):
    def __init__(
        self,
        amazon_conn_id=conn_ids.AWS.amazon_default,
    ):
        self.amazon_conn_id = amazon_conn_id
        self.conn = self.get_connection(self.amazon_conn_id)
        self.extras = self.conn.extra_dejson

        self.client_id = self.conn.login
        self.client_secret = self.conn.password
        self.host = self.conn.host
        self.refresh_token = self.extras.get("refresh_token")

    @cached_property
    def session(self):
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }
        auth_url = f"https://{self.host}/auth/o2/token"
        session = requests.session()
        session.headers = {"Accept": "application/json", "Content-type": "application/json"}
        print("Generating an Access Token")
        response = session.post(url=auth_url, json=payload)
        response.raise_for_status()

        rdict = response.json()
        access_token = rdict['access_token']
        session.headers.update({"x-amz-access-token": access_token})
        return session
