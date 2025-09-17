import hashlib
import hmac
import json
import time
from functools import cached_property

import requests
from airflow.hooks.base import BaseHook
from include.config import conn_ids


class TiktokShopHook(BaseHook):
    def __init__(self, tiktok_conn_id=conn_ids.Tiktok.shop_default):
        self.conn = self.get_connection(tiktok_conn_id)
        self.extras = self.conn.extra_dejson

        self.host = self.conn.host
        self.app_key = self.conn.login
        self.app_secret = self.conn.password
        self.refresh_token = self.extras.get("refresh_token")
        # self.version = self.extras.get('version', '202309')

    @cached_property
    def session(self):
        params = {
            "app_key": self.app_key,
            "app_secret": self.app_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }
        auth_url = f"https://{self.host}/api/v2/token/refresh"
        session = requests.session()
        session.headers = {
            "Accept": "application/json",
            "Content-type": "application/json",
        }
        print("Generating an Access Token")
        response = session.get(url=auth_url, params=params)
        response.raise_for_status()

        rdict = response.json()
        access_token = rdict["data"]["access_token"]
        session.headers.update({"x-tts-access-token": access_token})
        return session

    def _generate_signature(self, path, params, body=None):
        # Reorder the params based on alphabetical order
        secret = self.app_secret
        keys = sorted(params.keys())

        # Concat all the params in the format of {key}{value} and append the request path to the
        # beginning
        input_string = path
        for key in keys:
            input_string += key + str(params[key])

        # Concat body
        if body:
            input_string += json.dumps(body)

        # Wrap string generated in up with app_secret
        input_string = secret + input_string + secret
        # Encode the digest byte stream in hexadecimal and use SHA256 to generate sign with salt
        # (secret)
        h = hmac.new(secret.encode(), input_string.encode(), hashlib.sha256)

        # Return the hexadecimal digest
        return h.hexdigest()

    def make_request(
        self,
        url: str,
        path: str,
        method: str = "GET",
        params: dict = None,
        data: dict = None,
        files: dict = None,
    ):
        params = dict() if params is None else params
        params["app_key"] = self.app_key
        params["timestamp"] = int(time.time())

        sign = self._generate_signature(path, params, data)
        request_params = {"sign": sign, **params}

        request_body = json.dumps(data) if data else None

        response = self.session.request(
            method=method,
            url=url + path,
            params=request_params,
            data=request_body,
            files=files,
        )
        response.raise_for_status()
        return response.json()["data"]
