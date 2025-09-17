import requests

from include.airflow.hooks.base_web_api_hook import BaseWebApiHook


class HermesHook(BaseWebApiHook):
    def __init__(
        self,
        conn_id,
        host="https://api.hermesworld.co.uk/client-tracking-api",
        host_api_token="https://hermes-client-integration-prod.eu.auth0.com/oauth/token",
        version="v1",
    ):
        super().__init__(
            conn_id=conn_id,
            host=host,
            version=version,
        )
        self.host_api_token = host_api_token

    def get_base_url(self):
        return f"{self.host}/{self.version}"

    def get_base_headers(self):
        body = {
            "grant_type": "client_credentials",
            "client_id": self.extras.get("client_id"),
            "client_secret": self.extras.get("client_secret"),
            "audience": "client-tracking-api",
        }
        r = requests.post(self.host_api_token, data=body)
        token = r.json().get("access_token")

        return {
            "apikey": self.extras.get("api_key"),
            "Authorization": "Bearer " + token,
        }
