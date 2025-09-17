import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import pendulum
from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.email import send_email
from functools import cached_property
from snowflake.connector import DictCursor

from include import SQL_DIR
from include.airflow.callbacks.slack import send_slack_message
from include.airflow.callbacks.slack import SlackFailureCallback
from include.airflow.hooks.snowflake import SnowflakeHook
from include.config import owners, conn_ids
from include.config.email_lists import data_integration_support, edw_engineering
from include.utils import snowflake

default_args = {
    'start_date': pendulum.datetime(2020, 1, 1, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.data_integrations,
    'email': data_integration_support,
    "on_failure_callback": SlackFailureCallback('slack_alert_edm'),
}

dag = DAG(
    dag_id='edm_database_view_reconcile',
    default_args=default_args,
    schedule='0 13 * * 3',
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)


@dataclass
class ViewConfig:
    view_db_list: list
    email_to_list: list
    email_cc_list: Optional[list] = None


view_config_list = [
    ViewConfig(
        view_db_list=['edw.data_model'], email_to_list=data_integration_support + edw_engineering
    ),
    ViewConfig(
        view_db_list=['edw.reporting'], email_to_list=data_integration_support + edw_engineering
    ),
]


class ReconcileViewSnowflake(BaseOperator):
    """
    This operator will compare snowflake view definition available in source control with
    snowflake and sends email alert with list of mismatch views.  If the view doesnt exist in repo
    code will raise an exception.

    Args:
        snowflake_conn_id: snowflake connection
        view_database: list of view databases that needs to be compared
        email_to_list: list of email_ids to whom the report needs to be sent to
        email_cc_list: list of email_ids to whom the report needs to be sent to in cc

    """

    def __init__(
        self,
        snowflake_conn_id,
        view_database,
        email_to_list,
        email_cc_list,
        **kwargs,
    ):
        self.snowflake_conn_id = snowflake_conn_id
        self.view_database = view_database
        self.email_to_list = email_to_list
        self.email_cc_list = email_cc_list
        super().__init__(**kwargs)

    @cached_property
    def snowflake_hook(self):
        return SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

    def get_view_names(self, database_name, schema_name, sf_con):
        query = f"""SELECT TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME as VIEW_NAME
                    FROM {database_name}.INFORMATION_SCHEMA.VIEWS
                    WHERE table_schema"""
        query += "<>'INFORMATION_SCHEMA';" if schema_name is None else f"=UPPER('{schema_name}');"
        cur = sf_con.cursor()
        cur.execute(command=query)
        view_list = [rows[0] for rows in cur.fetchall()]
        return view_list

    @staticmethod
    def parse_view_definition(data):
        """
        This method parses view definition,strips spaces and extracts the column/base table information

        Args:
            data: view definition

        """
        index = data.lower().index("select")
        view_data = data[index:].strip(" ")
        view_def = ''.join(e for e in view_data if e.isalnum())
        return view_def

    def read_view_definition_from_snowflake(self, view_name, sf_con):
        cur = sf_con.cursor()
        updated_view_name = '.'.join(['"' + i + '"' for i in view_name.split('.', 2)])
        cur.execute(f"select get_ddl('view', '{updated_view_name}')")
        rows = cur.fetchall()
        data = rows[0][0]
        parsed_data = self.parse_view_definition(data=data)
        return parsed_data

    def read_view_definition_from_repository(self, database_directory, view_name):
        view_script = view_name.replace(f"{database_directory.upper()}.", "") + ".sql"
        view_path = Path(SQL_DIR, database_directory, "views", view_script.lower())
        if not os.path.exists(view_path):
            return
        with open(view_path, 'r') as file:
            data = file.read().replace('\n', '')
            parsed_data = self.parse_view_definition(data=data)
            return parsed_data

    @staticmethod
    def send_mail_alert(views_not_matching, views_not_present, email_to_list, email_cc_list):
        mismatch_df = pd.DataFrame(data=views_not_matching, columns=['mismatch_view_list'])
        missing_df = pd.DataFrame(data=views_not_present, columns=['missing_view_list'])
        html_content = f"{mismatch_df.to_html(index=False)}" if len(mismatch_df) != 0 else ''
        html_content += f"<br><br>{missing_df.to_html(index=False)}" if len(missing_df) != 0 else ''
        send_email(
            to=email_to_list,
            cc=email_cc_list,
            subject="Alert: View Mismatch list",
            html_content=html_content,
        )

    @staticmethod
    def send_mail_alert(views_not_matching, views_not_present, email_to_list, email_cc_list):
        mismatch_df = pd.DataFrame(data=views_not_matching, columns=['mismatch_view_list'])
        missing_df = pd.DataFrame(data=views_not_present, columns=['missing_view_list'])
        html_content = f"{mismatch_df.to_html(index=False)}" if len(mismatch_df) != 0 else ''
        html_content += f"<br><br>{missing_df.to_html(index=False)}" if len(missing_df) != 0 else ''
        send_email(
            to=email_to_list,
            cc=email_cc_list,
            subject="Alert: View Mismatch list",
            html_content=html_content,
        )

    @staticmethod
    def send_slack_alert(views_not_matching, views_not_present):
        slack_message = ReconcileViewSnowflake.format_slack_message(
            views_not_matching, views_not_present
        )
        # sends the slack notification to data_integrations channel
        send_slack_message(message=slack_message, conn_id='slack_data_integrations')

    @staticmethod
    def format_slack_message(views_not_matching, views_not_present):
        message = "*🔴 Alert: View Mismatch List 🔴*\n"

        if views_not_matching:
            message += "*❌ Views Not Matching:*\n"
            for view in views_not_matching:
                message += f"• `{view}`\n"

        if views_not_present:
            message += "\n*⚠️ Views Not Present:*\n"
            for view in views_not_present:
                message += f"• `{view}`\n"

        return message

    def execute(self, context=None):
        view_database_list = self.view_database
        views_not_matching = []
        views_not_present = []
        with self.snowflake_hook.get_conn() as cnx:
            query_tag = snowflake.generate_query_tag_cmd(self.dag_id, self.task_id)
            cur = cnx.cursor(cursor_class=DictCursor)
            cur.execute(query_tag)
            for view_db in view_database_list:
                view_database = view_db.split('.')[0]
                view_schema = view_db.split('.')[1] if '.' in view_db else None
                view_list = self.get_view_names(view_database, view_schema, cnx)
                for view in view_list:
                    snowflake_view_ddl = self.read_view_definition_from_snowflake(
                        view_name=view, sf_con=cnx
                    )
                    repo_view_ddl = self.read_view_definition_from_repository(
                        database_directory=view_database, view_name=view
                    )
                    if not repo_view_ddl:
                        views_not_present.append(f'{view}')
                    elif repo_view_ddl == snowflake_view_ddl:
                        pass
                    elif repo_view_ddl.lower() != snowflake_view_ddl.lower():
                        views_not_matching.append(f'{view}')
                    else:
                        pass
        if not views_not_matching and not views_not_present:
            print('All good')
        else:
            self.send_mail_alert(
                views_not_matching=views_not_matching,
                views_not_present=views_not_present,
                email_to_list=self.email_to_list,
                email_cc_list=self.email_cc_list,
            )
            self.send_slack_alert(
                views_not_matching=views_not_matching,
                views_not_present=views_not_present,
            )


with dag:
    for view in view_config_list:
        check_view_definition = ReconcileViewSnowflake(
            task_id=f'check_view_definition_repo_database_{view.view_db_list[0]}',
            snowflake_conn_id=conn_ids.Snowflake.default,
            view_database=view.view_db_list,
            email_to_list=view.email_to_list,
            email_cc_list=view.email_cc_list,
        )
