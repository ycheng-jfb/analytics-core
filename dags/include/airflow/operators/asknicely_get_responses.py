from datetime import datetime

import pendulum
import requests

from include.airflow.hooks.asknicely import AsknicelyHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvOperator
from include.utils.decorators import exponential_backoff
from include.config import conn_ids


class AsknicelyResponsesToS3Operator(BaseRowsToS3CsvOperator):
    """
    :param asknicely_conn_id: name of Sailthru credentials to use
    :param process_start_date: start date for API call
    :param s3_bucket: s3 bucket to send the data
    :param s3_key: s3 key to use.  in order to use splitting, put ``*`` where you want part id
    :param file_column_list: list of key values to write to file, and values used for csv header
    :param column_list: list of columns to select from table, if omitted defaults to all columns
    :param aws_conn_id: name of AWS connection to use
    :param kwargs:
    """

    template_fields = ["key", "process_end_date", "process_start_date"]

    def __init__(
        self,
        asknicely_conn_id,
        process_start_date,
        process_end_date,
        s3_bucket,
        s3_key,
        file_column_list,
        write_header,
        aws_conn_id=conn_ids.AWS.tfg_default,
        **kwargs,
    ):
        self.asknicely_conn_id = asknicely_conn_id
        self.process_start_date = process_start_date
        self.process_end_date = process_end_date
        super().__init__(
            s3_conn_id=aws_conn_id,
            bucket=s3_bucket,
            key=s3_key,
            column_list=file_column_list,
            write_header=write_header,
            **kwargs,
        )

    def xstr(s):
        return "" if s is None else str(s)

    @exponential_backoff(10)
    def get_rows(self):
        self.process_start_date = pendulum.parse(self.process_start_date).date()
        self.process_end_date = pendulum.parse(self.process_end_date).date()

        asknicely_hook = AsknicelyHook(self.asknicely_conn_id)
        request_uri = asknicely_hook.get_request_uri(
            self.process_start_date, self.process_end_date
        )
        response_data = requests.get(
            request_uri, headers={"X-apikey": asknicely_hook.api_key}
        ).json()
        updated_at = pendulum.DateTime.utcnow().isoformat()

        for data in response_data["data"]:
            business_unit = asknicely_hook.domain_key.upper()

            if data["country_c"] is None:
                country = ""
            elif data["country_c"].upper() == "GB":
                country = "UK"
            else:
                country = data["country_c"].upper()

            response_id = int(data["response_id"])

            response_date = datetime.fromtimestamp(int(data["responded"])).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            sent_date = datetime.fromtimestamp(int(data["sent"])).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            comment = data["comment"]

            del data["comment"]
            asknicely_responses_data = str(data)

            yield {
                **data,
                "business_unit": business_unit,
                "country": country,
                "response_id": response_id,
                "response_date": response_date,
                "sent_date": sent_date,
                "comment": comment,
                "asknicely_responses_data": asknicely_responses_data,
                "updated_at": updated_at,
            }
