import datetime
import logging

import pandas as pd
import pendulum
import requests
from airflow.models import BaseOperator
from airflow.utils.email import send_email_smtp
from requests.auth import HTTPBasicAuth
from tabulate import tabulate

from include.airflow.hooks.elasticsearch import ESHook
from include.config.email_lists import anomaly_detection_support
from include.utils.data_structures import chunk_df


class BaseAnomalyDetectionOperator(BaseOperator):
    def __init__(
        self,
        elasticsearch_conn_id,
        kibana_conn_id,
        job_name,
        index_name,
        id_name,
        *args,
        **kwargs,
    ):
        self.elasticsearch_conn_id = elasticsearch_conn_id
        self.kibana_conn_id = kibana_conn_id
        self.job_name = job_name
        self.index_name = index_name
        self.id_name = id_name
        self.datafeed_name = f"datafeed-{job_name}"
        self._es_hook = None
        self._kibana_hook = None
        super().__init__(*args, **kwargs)

    @property
    def es_hook(self):
        if self._es_hook:
            return self._es_hook
        self._es_hook = ESHook(self.elasticsearch_conn_id, protocol="https")
        return self._es_hook

    @property
    def kibana_hook(self):
        if self._kibana_hook:
            return self._kibana_hook
        self._kibana_hook = ESHook(self.kibana_conn_id, protocol="https")
        return self._kibana_hook

    def get_job_last_timestamp(self):
        post_url = f"https://{self.es_hook.conn.host}/_ml/anomaly_detectors/{self.job_name}/_stats"
        es_call = requests.get(
            post_url,
            auth=HTTPBasicAuth(self.es_hook.conn.login, self.es_hook.conn.password),
        )
        results_json = es_call.json()
        print(results_json)
        last_timestamp = results_json["jobs"][0]["data_counts"][
            "latest_record_timestamp"
        ]
        left_off_date = str(
            datetime.datetime.utcfromtimestamp(last_timestamp / 1000).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )[:-3]
        )
        return left_off_date

    def start_datafeed(self, start_time, end_time):
        """
        starts the datafeed

        :start_time: str, format of start time in UTC (example: "2019-09-10T23:00:00Z"). Equal to first time of new data load
        :end_time: str, format of end time in UTC (example: "2019-09-11T23:00:00Z"). Equal to last time of new data load
        """  # noqa:E501
        start_time = str(start_time)
        end_time = str(end_time)
        post_ml_start_datafeed = f"https://{self.es_hook.conn.host}/_ml/datafeeds/{self.datafeed_name}/_start"

        start_data = {
            "start": start_time,  # this is in UTC, 7 hours earlier than PST
            "end": end_time,
        }

        es_call = requests.post(
            post_ml_start_datafeed,
            json=start_data,
            auth=HTTPBasicAuth(self.es_hook.conn.login, self.es_hook.conn.password),
        )
        start_result = es_call.json()

        return start_result

    def open_ml_job(self):
        """
        opens ML job. needed before datafeed is started.
        :job_name: str, name of job
        """

        post_ml_open_ml_job = f"https://{self.es_hook.conn.host}/_ml/anomaly_detectors/{self.job_name}/_open"

        timeout = {"timeout": "35m"}

        es_call = requests.post(
            post_ml_open_ml_job,
            json=timeout,
            auth=HTTPBasicAuth(self.es_hook.conn.login, self.es_hook.conn.password),
        )
        open_result = es_call.json()
        return open_result

    def stop_datafeed(self):
        """
        closes ml datafeed

        :datafeed_name: str, name of datafeed
        """

        post_ml_stop_datafeed = (
            f"https://{self.es_hook.conn.host}/_ml/datafeeds/{self.datafeed_name}/_stop"
        )

        start_data = {"force": "true"}

        es_call = requests.post(
            post_ml_stop_datafeed,
            json=start_data,
            auth=HTTPBasicAuth(self.es_hook.conn.login, self.es_hook.conn.password),
        )
        stop_result = es_call.json()
        return stop_result

    def get_records_dataframe(self, start_time):
        """
        gets anomaly results.

        :start_time: str, format of start time in UTC (example: "2019-09-10T23:00:00Z"). Equal to first time of new data load
        """  # noqa:E501
        start_time = str(start_time)

        post_ml_get_records = f"https://{self.es_hook.conn.host}/_ml/anomaly_detectors/{self.job_name}/results/records"

        records_body = {"sort": "record_score", "desc": "true", "start": start_time}

        es_call = requests.post(
            post_ml_get_records,
            json=records_body,
            auth=HTTPBasicAuth(self.es_hook.conn.login, self.es_hook.conn.password),
        )
        records_result = es_call.json()
        if records_result["count"] > 0:
            anomaly_df = pd.DataFrame(records_result["records"])
            anomaly_df["timestamp"] = pd.to_datetime(anomaly_df["timestamp"], unit="ms")
            anomaly_df = anomaly_df[
                ["timestamp", "field_name", "record_score", "actual", "typical"]
            ]
        else:
            anomaly_df = pd.DataFrame()
            print("No Anomalies Recorded")

        return anomaly_df

    def get_first_timestamp_formatted(self, data):
        timestamp_original = str(data.timestamp_hourly.min())
        first_timestamp_formatted = str(
            pendulum.parse(timestamp_original)
            .in_timezone("America/Los_Angeles")
            .strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        return first_timestamp_formatted

    def get_last_timestamp_formatted(self, data):
        timestamp_original = str(data.timestamp_hourly.max())
        last_timestamp_formatted = str(
            pendulum.parse(timestamp_original)
            .in_timezone("America/Los_Angeles")
            .strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        return last_timestamp_formatted

    def get_high_anomalies(self, anomaly_dataframe, anomaly_threshold=50):
        high_anomalies_df = anomaly_dataframe[
            anomaly_dataframe["record_score"] >= anomaly_threshold
        ].reset_index(drop=True)
        return high_anomalies_df

    def send_anomaly_email(
        self,
        anomalies_df,
        first_timestamp,
        last_timestamp,
        recipients=["snelson@techstyle.com"],
    ):
        link_to_kibana_with_anomalies = f"https://{self.kibana_hook.conn.host}/app/ml#/explorer/?_g=(ml:(jobIds:!('{self.job_name}')),refreshInterval:(display:Off,pause:!f,value:0),time:(from:'{first_timestamp}',mode:absolute,to:'{last_timestamp}'))&_a=(filters:!(),mlAnomaliesTable:(intervalValue:auto,thresholdValue:0),mlExplorerFilter:(),mlExplorerSwimlane:(selectedLane:Overall,selectedTime:1567767600),mlSelectInterval:(display:Auto,val:auto),mlSelectLimit:(display:'10',val:10),mlSelectSeverity:(color:%23d2e9f7,display:warning,val:0),mlShowCharts:!t,query:(query_string:(analyze_wildcard:!t,query:'*')))"  # noqa:E501

        subject = f"Anomalies detected for job: {self.job_name}"

        html = """
        <html>
        <head>
        <style>
         table, th, td {{ border: 1px solid black; border-collapse: collapse; }}
          th, td {{ padding: 5px; }}
        </style>
        </head>
        <body><p>Anomalies Detected: </p>
        {table}
        <br>
        <body><p>Link to Kibana Dashboard with potential influencers: </p>
        <br>
        {kibana_link}
        </body></html>
        """

        col_list = list(anomalies_df.columns.values)
        html = html.format(
            table=tabulate(anomalies_df, headers=col_list, tablefmt="html"),
            kibana_link=link_to_kibana_with_anomalies,
        )
        send_email_smtp(to=recipients, subject=subject, html_content=html)

    def execute(self, context):
        log = logging.getLogger()
        log.setLevel(logging.WARNING)

        last_timestamp_loaded = self.get_job_last_timestamp()
        print("last_timestamp_loaded: ", last_timestamp_loaded)

        data = self.get_data(timestamp=last_timestamp_loaded)

        first_timestamp = self.get_first_timestamp_formatted(data=data)
        print("first_timestamp: ", first_timestamp)
        last_timestamp = self.get_last_timestamp_formatted(data=data)

        for df_to_chunk in chunk_df(df=data, chunk_size=10000):
            print("Chunk shape: ", df_to_chunk.shape)
            self.es_hook.helpers_api_put_dataframe_to_data_science_cluster(
                data=df_to_chunk, index_name=self.index_name, id_name=self.id_name
            )
        # start job, datafeed, and collect anomaly records
        self.open_ml_job()
        self.start_datafeed(start_time=first_timestamp, end_time=last_timestamp)

        anomaly_records_df = self.get_records_dataframe(start_time=first_timestamp)
        if len(anomaly_records_df) > 0:
            high_anomalies_df = self.get_high_anomalies(
                anomaly_records_df, anomaly_threshold=50
            )
            print("Anomalies: ", "\n", high_anomalies_df)
            if len(high_anomalies_df) > 0:
                self.send_anomaly_email(
                    anomalies_df=high_anomalies_df,
                    first_timestamp=first_timestamp,
                    last_timestamp=last_timestamp,
                    recipients=anomaly_detection_support,
                )
        if len(anomaly_records_df) == 0:
            print("No Anomalies")
