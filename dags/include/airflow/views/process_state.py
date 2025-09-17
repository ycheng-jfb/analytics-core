from flask import redirect
from flask_appbuilder.actions import action

from include.airflow.models.process_state import ProcessState
from include.airflow.views.base import AirflowModelView


class ProcessStateModelView(AirflowModelView):
    route_base = "/tfg_process_state"

    datamodel = AirflowModelView.CustomSQLAInterface(ProcessState)

    base_permissions = ["can_add", "can_list", "can_edit", "can_delete"]

    search_columns = ["namespace", "process_name", "value", "timestamp"]
    list_columns = ["namespace", "process_name", "value", "timestamp"]
    add_columns = ["namespace", "process_name", "value"]
    edit_columns = ["namespace", "process_name", "value"]
    base_order = ("timestamp", "desc")

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
