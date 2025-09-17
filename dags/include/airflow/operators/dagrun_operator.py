from airflow import DAG
from airflow.models import SkipMixin, TaskInstance
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.email import send_email


class TFGTriggerDagRunOperator(TriggerDagRunOperator, SkipMixin):
    """
    Same as airflow TriggerDagRunOperator but with option to skip downstream if the target dag is paused.

    Args:
        skip_downstream_if_paused: If True, will check if target dag_id is paused.  if so, will skip.
    """

    def __init__(self, skip_downstream_if_paused=True, **kwargs):
        super().__init__(**kwargs)
        self.skip_downstream_if_paused = skip_downstream_if_paused

    @property
    def target_is_paused(self):
        return DAG(dag_id=self.trigger_dag_id).is_paused

    def execute(self, context):
        skip = self.skip_downstream_if_paused and self.target_is_paused
        if skip:
            ti: TaskInstance = context["ti"]
            message = f"Warning: target dag_id {self.trigger_dag_id} is disabled; skipping trigger;"
            send_email(
                to=self.email,
                subject=message,
                html_content=message,
            )
            self.skip_all_except(ti, [])
        else:
            super().execute(context=context)
