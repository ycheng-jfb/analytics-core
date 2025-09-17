from flask import redirect
from flask_appbuilder.actions import action

from include.airflow.models.task_state import TaskState
from include.airflow.views.base import AirflowModelView, dag_link, task_instance_link


class TaskStateModelView(AirflowModelView):
    route_base = "/tfg_task_state"

    datamodel = AirflowModelView.CustomSQLAInterface(TaskState)

    base_permissions = ["can_add", "can_list", "can_edit", "can_delete"]

    search_columns = ["dag_id", "task_id", "value", "timestamp"]
    list_columns = ["dag_id", "task_id", "value", "timestamp"]
    add_columns = ["dag_id", "task_id", "value"]
    edit_columns = ["dag_id", "task_id", "value"]
    base_order = ("timestamp", "desc")

    formatters_columns = {"task_id": task_instance_link, "dag_id": dag_link}

    @action(
        "muldelete",
        "Delete",
        "Are you sure you want to delete selected records?",
        single=False,
    )
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())
