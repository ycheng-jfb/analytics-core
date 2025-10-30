from __future__ import annotations
import time
from functools import partial
from typing import Callable
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GCSHook(GoogleBaseHook):
    """Use the Google Cloud connection to interact with Google Cloud Storage."""

    _conn: storage.Client | None = None

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

    def get_conn(self):
        if not self._conn:
            self._conn = storage.Client(
                credentials=self.get_credentials(), client_info=CLIENT_INFO, project=self.project_id
            )

        return self._conn

    def upload(
        self,
        bucket_name: str,
        object_name: str,
        filename: str | None = None,
        timeout: int | None = 60,
        num_max_attempts: int = 1,
    ):
        """
        Upload a local file to Google Cloud Storage.

        :param bucket_name: The bucket to upload to.
        :param object_name: The object name to set when uploading the file.
        :param filename: The local file path to the file to be uploaded.
        :param timeout: Request timeout in seconds.
        :param num_max_attempts: Number of attempts to try to upload the file.
        """

        def _call_with_retry(f: Callable[[], None]) -> None:
            """
            Upload a file with a retry mechanism and exponential back-off.

            :param f: Callable that should be retried.
            """
            for attempt in range(1, 1 + num_max_attempts):
                try:
                    f()
                except GoogleCloudError as e:
                    if attempt == num_max_attempts:
                        self.log.error(
                            "Upload attempt of object: %s from %s has failed. Attempt: %s, max %s.",
                            object_name,
                            object_name,
                            attempt,
                            num_max_attempts,
                        )
                        raise e

                    # Wait with exponential backoff scheme before retrying.
                    timeout_seconds = 2 ** (attempt - 1)
                    time.sleep(timeout_seconds)

        client = self.get_conn()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name=object_name)

        if filename:
            _call_with_retry(
                partial(
                    blob.upload_from_filename,
                    filename=filename,
                    content_type="application/octet-stream",
                    timeout=timeout,
                )
            )
            self.log.info("File %s uploaded to %s in %s bucket", filename, object_name, bucket_name)
        else:
            raise ValueError("'filename' parameter missing. It is required to upload to gcs.")
