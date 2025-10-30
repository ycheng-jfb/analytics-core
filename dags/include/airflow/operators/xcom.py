from datetime import timedelta

import pendulum
from airflow.models import BaseOperator, DagRun


class IncrementalXcomUnixTimestampOperator(BaseOperator):
    WATERMARK_XCOM_KEY = "high_watermark"

    def __init__(self, lookback_interval=timedelta(days=7), *args, **kwargs):
        self.lookback_interval = lookback_interval
        self.low_watermark = None

        super().__init__(*args, **kwargs)

    def get_low_watermark(self, context):
        lwm_override = None
        try:
            lwm_override = context["dag_run"].conf.get("low_watermark")
        except Exception:
            raise

        if lwm_override:
            lwm = int(lwm_override)
            self.log.info(f"low_watermark override given in dag run conf: using {lwm}")
        else:
            lwm = (
                self.xcom_pull(
                    context=context,
                    task_ids=self.task_id,
                    dag_id=self.dag_id,
                    key=self.WATERMARK_XCOM_KEY,
                    include_prior_dates=True,
                )
                or 0
            )
            self.log.info(f"pulled last_high_watermark using xcom: {lwm}")

            if lwm > 0:
                lwm_dt = pendulum.from_timestamp(lwm) - self.lookback_interval
                lwm = int(lwm_dt.timestamp())
                self.log.info(f"setting lwm back '{self.lookback_interval}': {lwm}")

        self.low_watermark = lwm

    def set_high_watermark(self, context):
        dr = context["dag_run"]  # type: DagRun
        new_high_watermark = int(dr.start_date.timestamp())
        self.xcom_push(context=context, key=self.WATERMARK_XCOM_KEY, value=new_high_watermark)

    def execute(self, context):
        """
        Call self.get_low_watermark(context=context) before your execute method in order to pull low_watermark from
        xcom.
        Use self.low_watermark in your operation.
        After successful completion of your work, call self.set_high_watermark(context=context) to set xcom
        ``high_watermark`` value.
        """
        raise NotImplementedError
