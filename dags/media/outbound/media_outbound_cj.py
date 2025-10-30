import csv
import gzip
from collections import namedtuple

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_export import SnowflakeToSFTPOperator
from include.config import owners, s3_buckets, conn_ids
from include.config.conn_ids import SFTP
from include.config.email_lists import airflow_media_support
from include.utils.snowflake import generate_query_tag_cmd, split_statements

data_interval_start = "{{ data_interval_end.strftime('%Y%m%d') }}"

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2021, 9, 13, tz="America/Los_Angeles"),
    'owner': owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
}


dag = DAG(
    dag_id="media_outbound_cj",
    default_args=default_args,
    schedule="0 20 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)

SftpPath = namedtuple('SftpPath', ['store', 'region', 'conn_id', 'cid', 'subid', 'actiontrackerid'])


items = [
    SftpPath('SX', 'NA', f"{SFTP.sftp_cj_sxna}", 5844330, 256340, [448334]),
    SftpPath('FL', 'NA', f"{SFTP.sftp_cj_flna}", 5885341, 257710, [445552]),
    SftpPath('FK', 'NA', f"{SFTP.sftp_cj_fkna}", 5886277, 257713, [448314]),
    SftpPath('JF', 'NA', f"{SFTP.sftp_cj_jfna}", 5886275, 257711, [447612]),
    SftpPath('SD', 'NA', f"{SFTP.sftp_cj_sdna}", 5886276, 257712, [448332]),
    SftpPath('YTY', 'NA', f"{SFTP.sftp_cj_flna}", 5885341, 257710, [445554]),
    SftpPath('SX', 'EU', f"{SFTP.sftp_cj_sxeu}", 5963319, 260704, [430735, 436913, 436915, 436914]),
    SftpPath('SX', 'UK', f"{SFTP.sftp_cj_sxeu}", 5889552, 257920, [429724]),
    SftpPath(
        'FL',
        'EU',
        f"{SFTP.sftp_cj_sxeu}",
        6323253,
        287822,
        [440020, 440023, 440026, 440029, 440032, 440035, 440038],
    ),
]


class CJCorrectionsToSFTP(SnowflakeToSFTPOperator):
    def __init__(self, cid, subid, *args, **kwargs):
        self.cid = cid
        self.subid = subid
        super().__init__(*args, **kwargs)

    def export_to_file(self, sql, local_path) -> int:
        batch_count = 1
        rows_exported = 0
        with self.snowflake_hook.get_conn() as cnx:
            cur = cnx.cursor()
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur.execute(query_tag)
            for statement in split_statements(sql):
                self.log.info("Executing: %s", statement)
                cur.execute(statement, params=self.parameters)
            open_func = gzip.open if self.compression == "gzip" else open
            with open_func(local_path, "wt") as f:  # type: ignore
                writer = csv.writer(
                    f,
                    dialect=self.dialect,
                    delimiter=self.field_delimiter,
                    quoting=csv.QUOTE_MINIMAL,
                    escapechar='\\',
                )
                writer.writerow([f'&CID={self.cid}'])
                writer.writerow([f'&SUBID={self.subid}'])
                rows = cur.fetchmany(self.batch_size)
                while rows:
                    writer.writerows(rows)
                    rows_exported += len(rows)
                    batch_count += 1
                    self.log.info(f"fetching more rows: batch {batch_count}")
                    rows = cur.fetchmany(self.batch_size)
        return rows_exported


class CJOrdersToSFTP(SnowflakeToSFTPOperator):

    def export_to_file(self, sql, local_path) -> int:
        batch_count = 1
        rows_exported = 0
        with self.snowflake_hook.get_conn() as cnx:
            cur = cnx.cursor()
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur.execute(query_tag)
            for statement in split_statements(sql):
                self.log.info("Executing: %s", statement)
                cur.execute(statement, params=self.parameters)
            open_func = gzip.open if self.compression == "gzip" else open
            with open_func(local_path, "wt") as f:  # type: ignore
                writer = csv.writer(
                    f,
                    dialect=self.dialect,
                    delimiter=self.field_delimiter,
                    quoting=self.quoting,
                    escapechar='\\',
                )
                rows = cur.fetchmany(self.batch_size)
                if rows and self.header:
                    column_list = [
                        'companyId',
                        'enterpriseId',
                        'subscriptionId',
                        'orderId',
                        'actionTrackerId',
                        'eventTime',
                        'cjEvent',
                        'amount',
                        'discount',
                        'currency',
                        'duration',
                        'customerStatus',
                    ]
                    writer.writerow(column_list)
                while rows:
                    writer.writerows(rows)
                    rows_exported += len(rows)
                    batch_count += 1
                    self.log.info(f"fetching more rows: batch {batch_count}")
                    rows = cur.fetchmany(self.batch_size)
                s3_hook = S3Hook(conn_ids.S3.tsos_da_int_prod)
                s3_bucket = s3_buckets.tsos_da_int_inbound
                s3_key = f'media/cj.orders/{self.filename}'
                print(f"uploading to s3 - {s3_bucket}/{s3_key}")
                s3_hook.load_file(
                    filename=local_path,
                    key=s3_key,
                    bucket_name=s3_bucket,
                    replace=True,
                )
        return rows_exported


with dag:
    cj_all_events = SnowflakeProcedureOperator(
        procedure='dbo.cj_all_events.sql',
        database='reporting_media_base_prod',
        autocommit=False,
    )

    for item in items:
        corrections_sql = f"""
            SELECT 'UQL' AS reason,
                   '' tid,
                   cj.ORDERID oid
            FROM lake.cj.advertiser_spend cj
            LEFT JOIN edw_prod.data_model.fact_activation fa
                ON cj.orderid = edw_prod.stg.udf_unconcat_brand(fa.order_id)::varchar
            WHERE actiontrackerid IN ({','.join(str(id) for id in item.actiontrackerid)})
            -- WHERE actiontrackerid = {item.actiontrackerid}
                AND ((cancellation_local_datetime < current_date()
                        AND activation_local_datetime >= dateadd(DAY, -60, current_date()))
                        OR (fa.order_id is null AND actiontrackerid != 429614))
            ;
        """
        post_corrections_to_sftp = CJCorrectionsToSFTP(
            task_id=f"{item.store}{item.region}_corrections_to_sftp",
            sql_or_path=corrections_sql,
            sftp_conn_id=item.conn_id,
            filename=f"cj_corrections_{item.store}{item.region}_{data_interval_start}.csv",
            sftp_dir="",
            field_delimiter=',',
            cid=item.cid,
            subid=item.subid,
        )

        if item.store != 'YTY':
            orders_sql = f"""
                SELECT companyid,
                       enterpriseid,
                       subscriptionid,
                       orderid,
                       actiontrackerid,
                       eventtime,
                       cjevent,
                       amount,
                       discount,
                       currency,
                       round(duration) as duration,
                       customerstatus
                FROM reporting_media_base_prod.dbo.cj_all_events
                WHERE companyid = {item.cid}
                ORDER BY 1,2,3,5,6;
            """
            post_orders_to_sftp = CJOrdersToSFTP(
                task_id=f"{item.store}{item.region}_orders_to_sftp",
                sql_or_path=orders_sql,
                sftp_conn_id=item.conn_id,
                filename=f"cj_orders_{item.store}{item.region}_{data_interval_start}.csv",
                sftp_dir="",
                field_delimiter=',',
                header=True,
            )
            cj_all_events >> post_orders_to_sftp
