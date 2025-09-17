from airflow.hooks.base import BaseHook
from google.ads.googleads.client import GoogleAdsClient
from include.config import conn_ids


class GoogleAdsHook(BaseHook):
    def __init__(self, google_ads_conn_id=conn_ids.Google.ads_default):
        self.google_ads_conn_id = google_ads_conn_id

    def get_conn(self):
        conn = self.get_connection(self.google_ads_conn_id)
        yaml_str = conn.extra_dejson["yaml"]
        client = GoogleAdsClient.load_from_string(yaml_str)
        return client
