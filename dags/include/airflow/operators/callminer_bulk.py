import time
from functools import cached_property
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from zipfile import BadZipFile, ZipFile

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.airflow.hooks.callminer_bulk import CallMinerBulkHook
from include.airflow.operators.watermark import BaseProcessWatermarkOperator
from include.utils.exponential_wait import Waiter


class CallMinerBulkConversation(BaseProcessWatermarkOperator):

    template_fields = ("s3_key",)

    def __init__(
        self,
        s3_key,
        s3_bucket,
        namespace,
        process_name,
        s3_conn_id,
        callminer_bulk_conn_id,
        initial_load_value='1900-01-01 00:00:00+00:00',
        *args,
        **kwargs,
    ):
        super().__init__(
            initial_load_value=initial_load_value,
            namespace=namespace,
            process_name=process_name,
            *args,
            **kwargs,
        )
        self.callminer_bulk_conn_id = callminer_bulk_conn_id
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.s3_conn_id = s3_conn_id

    @cached_property
    def callminer_hook(self):
        return (
            CallMinerBulkHook(conn_id=self.callminer_bulk_conn_id)
            if self.callminer_bulk_conn_id
            else CallMinerBulkHook()
        )

    def get_high_watermark(self):
        return self.new_high_watermark if self.new_high_watermark else self.low_watermark

    def upload_to_s3(self, filename: str, bucket, key, s3_replace=True):
        s3_hook = S3Hook(self.s3_conn_id)
        s3_hook.load_file(
            filename=filename,
            key=key,
            bucket_name=bucket,
            replace=s3_replace,
        )
        print(f"uploaded to {key}")

    @staticmethod
    def check_job_status(conn_id):
        callminer_hook = CallMinerBulkHook(conn_id=conn_id) if conn_id else CallMinerBulkHook()
        params = {'id': callminer_hook.job_id}
        return callminer_hook.make_request('GET', 'export/history', params=params)

    def check_job(self):
        params = {'id': self.callminer_hook.job_id}
        return self.callminer_hook.make_request('GET', 'export/history', params=params)

    def watermark_execute(self, context=None):
        data = self.check_job()
        new_data = list(filter(lambda x: x["JobCompletionTime"] > self.low_watermark, data.json()))

        if not new_data:
            print("No new bulk job available")
            return

        # retrieve url
        download_url = new_data[0]["DownloadEndpoint"]
        download_endpoint = "/".join(download_url.split("/")[-2:])

        # update high watermark
        self.new_high_watermark = new_data[0]["JobCompletionTime"]

        response = self.callminer_hook.make_request('GET', download_endpoint, stream=True)

        with (
            NamedTemporaryFile(mode='wb', delete=True) as temp_zip_file,
            TemporaryDirectory() as zip_temp_dir,
        ):
            # Set the path for the temporary file
            temp_zip_path = temp_zip_file.name
            total_size = int(response.headers.get('content-length', 0))
            print(f"Temporary file for downloading: {temp_zip_path} with size: {total_size}")

            for chunk in response.iter_content(chunk_size=1024 * 1024):
                temp_zip_file.write(chunk)

            print(f"File downloaded successfully to {temp_zip_path}")

            try:
                # Open and extract the .zip file into the temporary directory
                with ZipFile(temp_zip_path, 'r') as zip_ref:
                    zip_ref.extractall(zip_temp_dir)
                    print(f"File uncompressed successfully into {zip_temp_dir}")
            except BadZipFile:
                print(f"Error: The file {temp_zip_path} is not a valid .zip file.")

            for file in Path(zip_temp_dir).iterdir():
                if file.as_posix().endswith("_Transcripts.csv.gz"):
                    self.upload_to_s3(file.as_posix(), bucket=self.s3_bucket, key=self.s3_key)


class CallMinerBulkBackfillConversation(BaseOperator):

    template_fields = ("s3_key",)

    def __init__(
        self,
        s3_key,
        s3_bucket,
        s3_conn_id,
        callminer_bulk_conn_id,
        start_datetime,
        end_datetime,
        *args,
        **kwargs,
    ):
        super().__init__(
            *args,
            **kwargs,
        )
        self.callminer_bulk_conn_id = callminer_bulk_conn_id
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.s3_conn_id = s3_conn_id
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

    @cached_property
    def callminer_hook(self):
        return (
            CallMinerBulkHook(conn_id=self.callminer_bulk_conn_id)
            if self.callminer_bulk_conn_id
            else CallMinerBulkHook()
        )

    def upload_to_s3(self, filename: str, bucket, key, s3_replace=True):
        s3_hook = S3Hook(self.s3_conn_id)
        s3_hook.load_file(
            filename=filename,
            key=key,
            bucket_name=bucket,
            replace=s3_replace,
        )
        print(f"uploaded to {key}")

    def create_job(self):
        job_body = {
            "Name": f"Transcripts {self.start_datetime} to {self.end_datetime}",
            "TenantApiKey": "TechStyle",
            "Duration": {
                "TimeFrame": "Custom",
                "StartDate": self.start_datetime,
                "EndDate": self.end_datetime,
            },
            "DataTypes": ["Transcripts"],
            "NotificationMethod": "Email",
            "EmailRecipients": ["psureshbhai@fabletics.com"],
            "IncludeUpdatedContacts": True,
        }

        return self.callminer_hook.make_request('POST', 'export/job', json=job_body)

    def remove_job(self, job_id):
        return self.callminer_hook.make_request('DELETE', f"export/job/{job_id}")

    def check_job_status(self, job_id):
        params = {'id': job_id}
        return self.callminer_hook.make_request('GET', 'export/history', params=params)

    def execute(self, context=None):

        job_resp = self.create_job()
        job_id = job_resp.json()["Id"]
        print(f"export job created with job_id:{job_id}")

        waiter = Waiter(
            growth_param=1.2,
            initial_wait=30,
            max_wait_time_seconds=60 * 30,
        )
        while True:
            wait_time = waiter.next()
            time.sleep(wait_time)
            exported_job = self.check_job_status(job_id)

            if bool(exported_job.json()) & (exported_job.json()[0]["Status"] != "Completed"):
                wait_time = waiter.next()
                time.sleep(wait_time)

            else:
                print(f"export job {job_id} completed successfully")
                break

        # retrieve url
        download_url = exported_job.json()[0]["DownloadEndpoint"]
        download_endpoint = "/".join(download_url.split("/")[-2:])

        response = self.callminer_hook.make_request('GET', download_endpoint, stream=True)

        with (
            NamedTemporaryFile(mode='wb', delete=True) as temp_zip_file,
            TemporaryDirectory() as zip_temp_dir,
        ):
            temp_zip_path = temp_zip_file.name
            total_size = int(response.headers.get('content-length', 0))
            print(f"Temporary file for downloading: {temp_zip_path} with size: {total_size}")

            for chunk in response.iter_content(chunk_size=1024 * 1024):
                temp_zip_file.write(chunk)

            print(f"File downloaded successfully to {temp_zip_path}")

            try:
                # Open and extract the .zip file into the temporary directory
                with ZipFile(temp_zip_path, 'r') as zip_ref:
                    zip_ref.extractall(zip_temp_dir)
                    print(f"File uncompressed successfully into {zip_temp_dir}")
            except BadZipFile:
                print(f"Error: The file {temp_zip_path} is not a valid .zip file.")

            for file in Path(zip_temp_dir).iterdir():
                if file.as_posix().endswith("_Transcripts.csv.gz"):
                    self.upload_to_s3(file.as_posix(), bucket=self.s3_bucket, key=self.s3_key)

        self.remove_job(job_id)
        print(f"export job {job_id} removed successfully")
