import pendulum
from functools import cached_property

from edm.acquisition.configs import get_table_config
from include.airflow.hooks.bash import BashHook
from include.airflow.hooks.mssql import MsSqlOdbcHook, get_column_list_definition
from include.airflow.hooks.snowflake import SnowflakeHook
from include.config import stages
from include.utils.acquisition.table_config import database_schema_mapping, get_conn_id
from include.utils.context_managers import ConnClosing, Timer
from include.utils.string import unindent_auto


class LargeTableInitialLoad:
    """
    This is a helper class to facilitate getting large tables from MS Sql Server into snowflake.

    We use bcp and some linux magic to stream data from sql server to s3, splitting every 10,000,000
    rows, and escaping delimiters and special characters as necessary.

    The file format produced with bcp is slightly different than that used by our normal python
    process.  So we have to use snowflake to convert the format.  So we create a temporary table
    in snowflake, load the data, then export to the normal production inbound path.

    Args:
        database: source database
        schema: source schema
        table: source table
        server_name: source server host name e.g. ``tfgelsvm16dbp70``
        friendly_server_name: friendly name for source server, used in s3 prefix, e.g. ``dbp70``

    Examples:
        >>> table_list = [
        >>>     'ultramerchant.dbo.ui_promo_management_userlist_users',
        >>>     'ultramerchant.dbo.visitor',
        >>>     'ultramerchant.dbo.session_log',
        >>>     'ultramerchant.dbo.session_tracking_detail',
        >>> ]
        >>>
        >>> for full_table_name in table_list:
        >>>     database, schema, table = full_table_name.split('.')
        >>>     table_job = LargeTableInitialLoad(
        >>>         database=database,
        >>>         schema=schema,
        >>>         table=table,
        >>>         server_name='tfgelsvm16dbp70',
        >>>         friendly_server_name='dbp70',
        >>>     )
        >>>     table_job.run_backfill_to_s3(dry_run=True)
        >>>     table_job.run_snowflake_load(dry_run=True)

    """

    def __init__(self, database, schema, table, server_name, friendly_server_name=None):
        self.server_name = server_name
        self.database, self.schema, self.table = database, schema, table
        self.friendly_server_name = friendly_server_name or server_name
        self.prefix = f"archive/{self.friendly_server_name}_bcp_tsv/{self.database}/{self.schema}.{self.table}"
        self.bucket = 'tsos-da-int-backup-archive'
        self.stage = stages.tsos_da_int_backup_archive
        self.tmp_table = f"lake_stg.tmp.{self.schema}_{self.table}"
        self.target_database = 'lake'
        self.target_schema = database_schema_mapping.get(self.database, self.database)
        self.full_target_table_name = f"{self.target_database}.{self.target_schema}.{self.table}"

    @property
    def bash_cmd(self):
        return self._get_bash_cmd(
            server=self.server_name,
            database=self.database,
            schema=self.schema,
            table=self.table,
            prefix=self.prefix,
            bucket=self.bucket,
        )

    @staticmethod
    def _get_bash_cmd(
        server,
        database,
        schema,
        table,
        prefix,
        bucket,
    ):
        cmd = rf"""
        #!/bin/bash

        a=$'\001'
        b=$'\002'
        c=$'\003'
        ft="$a$b$b$a"
        rt="$a$c$c$a"
        awk_command_elements=(
            'BEGIN {{RS=rt;FS=ft;OFS="\t";ORS="\0"}}'
            '{{gsub(/\0/,"");'
            'gsub(/\\/,"\\\\");'
            'gsub(/\n/,"\\\n");'
            'gsub(/\t/,"\\\t");'
            '$1=$1}}1'
        )
        full_awk_command=$(IFS=; echo "${{awk_command_elements[*]}}")
        tr_replace_nul='tr "\0" "\n"'

        s3_bucket='{bucket}'
        s3_key='{prefix}/{schema}.{table}_$FILE.csv.gz'

        tfifo=$(mktemp -d)/fifo
        mkfifo $tfifo
        bcp "
        SELECT *
        FROM {database}.{schema}.[{table}] s (nolock)
            " \
            queryout $tfifo  \
            -S {server} \
            -U app_airflow \
            -P "$BCP_PASS" \
            -a 65535 \
            -c \
            -K ReadOnly \
            -r $rt \
            -t $ft &
        pid=$! # get PID of backgrounded bcp process
        count=$(ps -p $pid -o pid= |wc -l) # check whether process is still running
        if [[ $count -eq 0 ]] # if process is already terminated, something went wrong
        then
            echo "something went wrong with bcp command"
            rm $tfifo
            wait $pid
            exit $?
        else
            echo "bcp command still running"
            cat $tfifo | \
                awk \
                    -v rt=$rt \
                    -v ft=$ft \
                    "$full_awk_command" | \
                split \
                    -l 10000000 \
                    -t "\0" \
                    --filter="${{tr_replace_nul}} | gzip | aws s3 cp - s3://${{s3_bucket}}/${{s3_key}}" \
                && rm $tfifo
            exit $?
        fi

        """
        return cmd

    @cached_property
    def mssql_hook(self):
        return MsSqlOdbcHook(get_conn_id(self.database))

    @cached_property
    def source_column_list(self):
        with ConnClosing(self.mssql_hook.get_conn()) as cnx:
            cur = cnx.cursor()
            column_list = list(
                get_column_list_definition(
                    cur=cur, database=self.database, schema=self.schema, table=self.table
                )
            )
        return column_list

    @cached_property
    def table_config(self):
        return get_table_config(table_name=self.full_target_table_name)

    @property
    def ddl_create_tmp_table(self):
        col_ddl = ',\n    '.join([f"{x.name} {x.type}" for x in self.source_column_list])
        cmd = f"""
        CREATE TEMP TABLE {self.tmp_table} (
            {col_ddl}
        );
        """
        return unindent_auto(cmd)

    @property
    def dml_copy_into_tmp_table(self):
        params = {
            'TYPE': 'CSV',
            'FIELD_DELIMITER': r"'\t'",
            'ESCAPE': r"'\\'",
            'NULL_IF': "('')",
        }
        params_str = ',\n                '.join([f"{k} = {v}" for k, v in params.items()])
        cmd = f"""
        COPY INTO {self.tmp_table}
            FROM '{self.stage}/{self.prefix}/'
            FILE_FORMAT = (
                {params_str}
            );
        """
        return unindent_auto(cmd)

    @property
    def dml_copy_into_s3_inbound_path(self):
        params = {
            'TYPE': 'CSV',
            'FIELD_DELIMITER': r"'\t'",
            'RECORD_DELIMITER': r"'\n'",
            'FIELD_OPTIONALLY_ENCLOSED_BY': "'\"'",
            'ESCAPE_UNENCLOSED_FIELD': "NONE",
            'NULL_IF': "('')",
            "COMPRESSION": "gzip",
        }
        params_str = ',\n                '.join([f"{k} = {v}" for k, v in params.items()])
        today = pendulum.today().to_date_string().replace('-', '')
        s3_prefix = self.table_config.s3_prefix
        max_size = int(500e6)
        cmd = f"""
        COPY INTO '{stages.tsos_da_int_inbound}/{s3_prefix}/bcp_tsv_load_{today}/'
            FROM {self.tmp_table}
            FILE_FORMAT = (
                {params_str}
            )
            HEADER = FALSE
            MAX_FILE_SIZE={max_size};"""
        return unindent_auto(cmd)

    def run_snowflake_load(self, dry_run=False):
        """
        Run snowflake etl to convert file format and dump to s3 prod inbound path.

        Only run this once, and only after running the bcp part.

        1. create temp table
        2. load to temp table
        3. export to the production s3 inbound path for ecom acquisition in the right file format

        """
        cmd = ''.join(
            (
                self.ddl_create_tmp_table,
                self.dml_copy_into_tmp_table,
                self.dml_copy_into_s3_inbound_path,
            )
        )
        if dry_run:
            print(cmd)
        else:
            hook = SnowflakeHook()
            hook.execute_multiple(cmd)

    def run_backfill_to_s3(self, dry_run=False):
        """
        Will actually copy the data from sql server to

        """
        if dry_run:
            print(f"dryrun for table {self.table}: \n\n{self.bash_cmd}")
        else:
            hook = BashHook()
            with Timer(f"table {self.table}"):
                try:
                    hook.run_command(bash_command=self.bash_cmd)
                except Exception:
                    hook.send_sigterm()
