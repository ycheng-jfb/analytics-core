import pendulum
from functools import cached_property

from include.airflow.hooks.aem import AemHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvWatermarkOperator


class AemMetadata(BaseRowsToS3CsvWatermarkOperator):
    @cached_property
    def aem_hook(self):
        return AemHook(conn_id=self.hook_conn_id) if self.hook_conn_id else AemHook()

    def get_high_watermark(self):
        return pendulum.now().strftime("%Y-%m-%d")

    def get_rows(self):
        updated_at = pendulum.DateTime.utcnow()
        data = self.aem_hook.make_request(
            "GET",
            f"getimagemetadata?sDate={self.low_watermark}&eDate={self.new_high_watermark}",
        ).json()
        for asset in data:
            yield {**asset, "updated_at": updated_at}
