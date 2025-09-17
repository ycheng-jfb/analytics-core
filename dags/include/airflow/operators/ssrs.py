from include.airflow.operators.mssql import MsSqlOdbcHook
from include.airflow.operators.sqlagent import RunSqlAgent
from include.config import conn_ids


class SsrsScheduleOperator(RunSqlAgent):
    """
    This operator class allows an SSRS schedule to run, given a schedule name

    Args:
        ssrs_schedule_name: the name of an SSRS schedule
        mssql_conn_id: the connection id for the server hosting SSRS
        wrapper_proc: the wrapper proc name which triggers the subscription in SSRS

    Examples:
        >>> task = SsrsScheduleOperator(
        >>>     ssrs_schedule_name='Finance Month End',
        >>>     mssql_conn_id='mssql_edw01_app_airflow',
        >>>     task_id='test',
        >>>     wrapper_proc='sp_start_job',
        >>> )
        >>> task.mssql_job_name
        >>> task.execute(context=None)

    """

    def __init__(
        self,
        ssrs_schedule_name,
        mssql_conn_id=conn_ids.MsSql.default,
        wrapper_proc="pr_start_job_wrapper",
        **kwargs,
    ):
        self.ssrs_schedule_name = ssrs_schedule_name
        super().__init__(
            mssql_job_name=None,
            mssql_conn_id=mssql_conn_id,
            wrapper_proc=wrapper_proc,
            **kwargs,
        )

    @property
    def mssql_job_name(self):
        cmd = f"""SELECT scheduleid
                FROM reportserver.dbo.schedule WITH (NOLOCK)
                WHERE name = '{self.ssrs_schedule_name}'
                """
        mssql_hook = MsSqlOdbcHook(mssql_conn_id=self.mssql_conn_id)
        rows = mssql_hook.get_records(cmd)
        job_name = str(rows[0][0])
        return job_name
