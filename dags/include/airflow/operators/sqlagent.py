from airflow.models import BaseOperator

from include.airflow.operators.mssql import MsSqlOdbcHook
from include.utils.context_managers import ConnClosing
from include.config import conn_ids


class RunSqlAgent(BaseOperator):
    """
    This operator class runs the given SQL agent job to run

    Args:
        mssql_job_name: the name of the SQL agent job
        mssql_conn_id: the connection id for the MSSQL Server
        wrapper_proc: the wrapper proc name which triggers the SQL agent job

    Examples:
        >>> task = RunSqlAgent(
        >>>     mssql_job_name='na-weekly',
        >>>     mssql_conn_id='mssql_edw01_app_airflow',
        >>>     task_id='test',
        >>>     wrapper_proc='sp_start_job',
        >>> )
        >>> task.mssql_job_name
        >>> task.execute(context=None)

    """

    def __init__(
        self,
        mssql_job_name,
        mssql_conn_id=conn_ids.MsSql.default,
        wrapper_proc="pr_start_job_wrapper",
        *args,
        **kwargs,
    ):
        self.mssql_conn_id = mssql_conn_id
        self._mssql_job_name = mssql_job_name
        self.wrapper_proc = wrapper_proc
        super().__init__(*args, **kwargs)

    @property
    def mssql_job_name(self):
        return self._mssql_job_name

    def execute(self, context):
        cmd = f"execute msdb.dbo.{self.wrapper_proc} '{self.mssql_job_name}'"
        mssql_hook = MsSqlOdbcHook(mssql_conn_id=self.mssql_conn_id)
        with ConnClosing(mssql_hook.get_conn()) as conn:
            cur = conn.cursor()
            cur.execute(cmd)
