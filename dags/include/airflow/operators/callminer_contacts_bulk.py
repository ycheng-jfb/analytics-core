from functools import cached_property
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from zipfile import BadZipFile, ZipFile

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.airflow.hooks.callminer_contacts_bulk import CallMinerBulkContactsHook
from include.airflow.operators.watermark import BaseProcessWatermarkOperator


class CallMinerBulkContacts(BaseProcessWatermarkOperator):
    template_fields = ("s3_key",)

    def __init__(
        self,
        s3_key,
        s3_bucket,
        namespace,
        process_name,
        s3_conn_id,
        callminer_bulk_conn_id,
        initial_load_value="1900-01-01 00:00:00+00:00",
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
            CallMinerBulkContactsHook(conn_id=self.callminer_bulk_conn_id)
            if self.callminer_bulk_conn_id
            else CallMinerBulkContactsHook()
        )

    def get_high_watermark(self):
        return (
            self.new_high_watermark if self.new_high_watermark else self.low_watermark
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

    @staticmethod
    def check_job_status(table_name, conn_id):
        callminer_hook = (
            CallMinerBulkContactsHook(conn_id=conn_id)
            if conn_id
            else CallMinerBulkContactsHook()
        )
        job_mapping_dict = {
            "contacts_new": callminer_hook.contacts_job_id,
            "categories_new": callminer_hook.categories_job_id,
            "categories_components_new": callminer_hook.categories_components_job_id,
            "scores_new": callminer_hook.scores_job_id,
            "score_indicators_new": callminer_hook.score_indicators_job_id,
        }
        params = {"id": job_mapping_dict.get(table_name)}
        return callminer_hook.make_request("GET", "export/history", params=params)

    def check_job(self):
        job_mapping_dict = {
            "contacts_new": self.callminer_hook.contacts_job_id,
            "categories_new": self.callminer_hook.categories_job_id,
            "categories_components_new": self.callminer_hook.categories_components_job_id,
            "scores_new": self.callminer_hook.scores_job_id,
            "score_indicators_new": self.callminer_hook.score_indicators_job_id,
        }
        params = {"id": job_mapping_dict.get(self.process_name)}
        return self.callminer_hook.make_request("GET", "export/history", params=params)

    def watermark_execute(self, context=None):
        filename_mapping_dict = {
            "contacts_new": "Contacts",
            "categories_new": "Categories",
            "categories_components_new": "Category_Components",
            "scores_new": "Scores",
            "score_indicators_new": "Score_Indicators",
        }
        filename_postfix = filename_mapping_dict.get(self.process_name)

        data = self.check_job()
        new_data = list(
            filter(
                lambda x: x["Status"] == "Completed"
                and x["JobCompletionTime"] > self.low_watermark,
                data.json(),
            )
        )

        if not new_data:
            print("No new bulk job available")
            return

        # retrieve url
        download_url = new_data[0]["DownloadEndpoint"]
        download_endpoint = "/".join(download_url.split("/")[-2:])

        # update high watermark
        self.new_high_watermark = new_data[0]["JobCompletionTime"]

        response = self.callminer_hook.make_request(
            "GET", download_endpoint, stream=True
        )

        with (
            NamedTemporaryFile(mode="wb", delete=True) as temp_zip_file,
            TemporaryDirectory() as zip_temp_dir,
        ):
            # Set the path for the temporary file
            temp_zip_path = temp_zip_file.name
            total_size = int(response.headers.get("content-length", 0))
            print(
                f"Temporary file for downloading: {temp_zip_path} with size: {total_size}"
            )

            for chunk in response.iter_content(chunk_size=1024 * 1024):
                temp_zip_file.write(chunk)

            print(f"File downloaded successfully to {temp_zip_path}")

            try:
                # Open and extract the .zip file into the temporary directory
                with ZipFile(temp_zip_path, "r") as zip_ref:
                    zip_ref.extractall(zip_temp_dir)
                    print(f"File uncompressed successfully into {zip_temp_dir}")
            except BadZipFile:
                print(f"Error: The file {temp_zip_path} is not a valid .zip file.")

            for file in Path(zip_temp_dir).iterdir():
                if file.as_posix().endswith(f"_{filename_postfix}.csv.gz"):
                    self.upload_to_s3(
                        file.as_posix(), bucket=self.s3_bucket, key=self.s3_key
                    )
