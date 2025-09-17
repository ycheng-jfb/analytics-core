from airflow.www import utils as wwwutils
from airflow.www.widgets import AirflowModelListWidget
from flask import url_for
from flask_appbuilder import ModelView
from markupsafe import Markup


class AirflowModelView(ModelView):
    list_widget = AirflowModelListWidget
    page_size = 25

    CustomSQLAInterface = wwwutils.CustomSQLAInterface


def task_instance_link(attr):
    dag_id = attr.get("dag_id")
    task_id = attr.get("task_id")
    if not (dag_id and task_id):
        return None
    kwargs = dict(dag_id=dag_id, task_id=task_id)
    data_interval_start = attr.get("data_interval_start")
    if data_interval_start:
        kwargs.update(data_interval_start=data_interval_start.isoformat())
    url = url_for("Airflow.task", **kwargs)
    url_root = url_for("Airflow.graph", **kwargs)
    return Markup(
        """
        <span style="white-space: nowrap;">
        <a href="{url}">{task_id}</a>
        <a href="{url_root}" title="Filter on this task and upstream">
        <span class="glyphicon glyphicon-filter" style="margin-left: 0px;"
            aria-hidden="true"></span>
        </a>
        </span>
        """
    ).format(url=url, task_id=task_id, url_root=url_root)


def dag_link(attr):
    kwargs = {}
    dag_id = attr.get("dag_id")
    if dag_id:
        kwargs.update(dag_id=dag_id)
    else:
        return None
    data_interval_start = attr.get("data_interval_start")
    if data_interval_start:
        kwargs.update(data_interval_start=data_interval_start)
    url = url_for("Airflow.graph", **kwargs)
    return Markup('<a href="{}">{}</a>').format(url, dag_id)
