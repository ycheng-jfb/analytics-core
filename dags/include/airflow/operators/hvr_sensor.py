import pendulum
from airflow.sensors.base import BaseSensorOperator

from include.airflow.hooks.snowflake import SnowflakeHook
from include.utils.context_managers import ConnClosing
from include.utils import snowflake
from include.config import conn_ids
from airflow.utils import timezone


class hvr_sensor(BaseSensorOperator):

    def __init__(
        self,
        schema,
        execution_date=None,
        lookback_minutes=30,
        snowflake_conn_id=conn_ids.Snowflake.default,
        database='all',
        table_group='all',
        poke_interval=60 * 1,
        mode='poke',
        timeout=60 * 360,
        **kwargs,
    ):
        self.snowflake_conn_id = snowflake_conn_id
        self.lookback_minutes = lookback_minutes
        self.database = database.upper()
        self.schema = schema.upper()
        self.table_group = table_group.upper()
        super().__init__(
            **kwargs,
            poke_interval=poke_interval,
            mode=mode,
            timeout=timeout,
            retries=2,
            email_on_retry=False,
        )

    def calculate_time_cutoff(self, context):
        current_time = context["macros"].datetime.today()
        current_time_pst = timezone.convert_to_utc(current_time).in_timezone("America/Los_Angeles")

        self.time_cutoff = current_time_pst.subtract(minutes=self.lookback_minutes)

    def snowflake_hook(self):
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
        )

    def hvr_query(self, database, schema, table_group):
        cmd = (
            f"SELECT * \n"
            f"FROM LAKE_CONSOLIDATED.REFERENCE.HVR_LAKE_STATUS \n"
            f"WHERE UPDATED_TO < '{self.time_cutoff}' \n"
        )

        """
            Define what LAKE Brand database needs to be monitored.
            Default to "all LAKE Brand DBs".
        """
        if database != 'ALL':
            cmd += f"    AND DATABASE = '{database}' \n"

        cmd += f"    AND SCHEMA ILIKE '{schema}' \n"

        """
            Define what Table_Group needs to be monitored.
            Default to "all Table Groups in the Schema will be monitored".
        """
        if table_group != 'ALL':
            cmd += f"    AND TABLE_GROUP = '{table_group}' \n"
        cmd += "; "
        return cmd

    def poke(self, context):
        self.calculate_time_cutoff(context)

        cmd = self.hvr_query(self.database, self.schema, self.table_group)
        query_tag_cmd = snowflake.generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        with ConnClosing(snowflake_hook.get_conn()) as conn:
            cur = conn.cursor()
            cur.execute(query_tag_cmd)
            cur.execute(cmd)
            result = cur.fetchone()

        if result is None or result[0] == 0:
            print(f"HVR Data populated as of {self.time_cutoff}.")
            return True

        print(f"HVR Data not updated to {self.time_cutoff}, retrying.")
        return False
