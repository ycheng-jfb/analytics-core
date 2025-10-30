from airflow.hooks.base import BaseHook
from google.ads.googleads.client import GoogleAdsClient


class GoogleExportHook(BaseHook):
    def __init__(self, google_export_conn_id='google_offline_export_default'):
        self.google_export_conn_id = google_export_conn_id

    def get_conn(self):
        conn = self.get_connection(self.google_export_conn_id)
        yaml_str = conn.extra_dejson["yaml"]
        client = GoogleAdsClient.load_from_string(yaml_str, version='v18')
        return client
