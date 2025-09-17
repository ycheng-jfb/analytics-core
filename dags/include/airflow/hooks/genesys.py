import base64

import requests
from airflow.hooks.base import BaseHook
from functools import cached_property
from include.config import conn_ids


class GenesysPureCloudHook(BaseHook):
    def __init__(
        self,
        genesys_conn_id=conn_ids.Genesys.default,
    ):
        self.genesys_conn_id = genesys_conn_id
        self.conn = self.get_connection(genesys_conn_id)
        self.client_id = self.conn.login
        self.client_secret = self.conn.password

    @cached_property
    def session(self):
        authorization = base64.b64encode(
            bytes(self.client_id + ":" + self.client_secret, "ISO-8859-1")
        ).decode("ascii")
        request_headers = {
            "Authorization": f"Basic {authorization}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        request_body = {"grant_type": "client_credentials"}
        response = requests.post(
            "https://login.mypurecloud.com/oauth/token",
            data=request_body,
            headers=request_headers,
        )
        if response.status_code != 200:
            print("Failed with Status Code:")
            raise Exception(f"Failed to get token: {str(response.status_code)} - {response.reason}")
        print("Got token")
        token_response = response.json()
        token_header = {
            "Authorization": f"{token_response['token_type']} {token_response['access_token']}"
        }
        session = requests.session()
        session.headers = token_header
        return session
