from datetime import timedelta

import pendulum
from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from include.airflow.callbacks.slack import slack_failure_edm, slack_sla_miss_edm
from include.airflow.operators.mssql import MsSqlOperator, MsSqlToS3Operator
from include.airflow.operators.mssql_to_sftp import MssqlToSFTPOperator
from include.config import conn_ids, owners, s3_buckets
from include.config.email_lists import data_integration_support
from include.airflow.operators.snowflake import SnowflakeSqlOperator

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2021, 7, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": data_integration_support,
    "on_failure_callback": slack_failure_edm,
}


def from_ds(data_interval_start):
    exec_date = (
        pendulum.instance(data_interval_start).replace(tzinfo=None).format("YYYYMMDD")
    )
    return str(exec_date)


dag = DAG(
    dag_id="edm_outbound_mssql_sftp_storeforce_realtime",
    default_args=default_args,
    schedule="2/30 * * * *",
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
    sla_miss_callback=slack_sla_miss_edm,
    user_defined_macros={"from_ds": from_ds},
)

fl_store_level_grain_source_sql = """
    select
    STORE_CODE,
    FORMAT(DATE,'dd/MM/yyyy') AS DATE,
    FORMAT(SLOT,'HH:mm') AS SLOT,
    CAST(SUM(ORDER_COUNT) as int) AS ORDER_COUNT,
    CAST(SUM(PRODUCT_GROSS_REVENUE) AS Decimal(19,2)) AS PRODUCT_GROSS_REVENUE,
    CAST(SUM(UNIT_COUNT) as int) AS UNIT_COUNT,
    CAST(SUM(REFUND_ORDERS) as int) AS REFUND_ORDERS,
    CAST(SUM(PRODUCT_REFUND)AS Decimal(19,2)) AS PRODUCT_REFUND,
    CAST(SUM(REFUND_UNITS) as int) AS REFUND_UNITS,
    cast(SUM(ACTIVATING_ORDER_COUNT) as int) AS ACTIVATING_VIP_ORDER_COUNT,
    cast(SUM(GUEST_ORDER_COUNT) as int) AS GUEST_ORDER_COUNT,
    cast(SUM(REPEAT_VIP_ORDER_COUNT)as int) AS REPEAT_VIP_ORDER_COUNT,
    0 AS GROSS_MARGIN
from analytic.dbo.realtime_from_source
WHERE EVENT_STORE_BRAND='Fabletics' AND EVENT_STORE_LOCATION='Retail'
group by STORE_CODE,
    DATE,
    SLOT
ORDER BY STORE_CODE,
    DATE,
    SLOT;
"""

fl_associate_level_grain_source_sql = """
    select
    STORE_CODE,
    EMPLOYEE_NUMBER,
    FORMAT(DATE,'dd/MM/yyyy') AS DATE,
    cast(SUM(ORDER_COUNT) as int) AS ORDER_COUNT,
    CAST(SUM(PRODUCT_GROSS_REVENUE)AS DECIMAL(19,2)) AS PRODUCT_GROSS_REVENUE,
    cast(SUM(UNIT_COUNT) as int) AS UNIT_COUNT,
    cast( SUM(REFUND_ORDERS) as int) AS REFUND_ORDERS,
    CAST(SUM(PRODUCT_REFUND)AS DECIMAL(19,2)) AS PRODUCT_REFUND,
    cast(SUM(REFUND_UNITS) as int) AS REFUND_UNITS,
    cast(SUM(ACTIVATING_ORDER_COUNT)as int) AS ACTIVATING_VIP_ORDER_COUNT,
    cast(SUM(GUEST_ORDER_COUNT) as int) AS GUEST_ORDER_COUNT,
    cast(SUM(REPEAT_VIP_ORDER_COUNT) as int) AS REPEAT_VIP_ORDER_COUNT,
    0 AS GROSS_MARGIN
from analytic.dbo.realtime_from_source
WHERE EVENT_STORE_BRAND='Fabletics' AND EVENT_STORE_LOCATION='Retail'
    and EMPLOYEE_NUMBER <> '-1'
group by STORE_CODE,
    EMPLOYEE_NUMBER,
    DATE
ORDER BY STORE_CODE,
    EMPLOYEE_NUMBER,
    DATE;
"""

sxf_store_level_grain_source_sql = """select
    CONCAT('SXF-',STORE_CODE),
    FORMAT(DATE,'dd/MM/yyyy') AS DATE,
    FORMAT(SLOT,'HH:mm') AS SLOT,
    CAST(SUM(ORDER_COUNT) as int) AS ORDER_COUNT,
    CAST(SUM(PRODUCT_GROSS_REVENUE) AS Decimal(19,2)) AS PRODUCT_GROSS_REVENUE,
    CAST(SUM(UNIT_COUNT) as int) AS UNIT_COUNT,
    CAST(SUM(REFUND_ORDERS) as int) AS REFUND_ORDERS,
    CAST(SUM(PRODUCT_REFUND)AS Decimal(19,2)) AS PRODUCT_REFUND,
    CAST(SUM(REFUND_UNITS) as int) AS REFUND_UNITS,
    cast(SUM(ACTIVATING_ORDER_COUNT) as int) AS ACTIVATING_VIP_ORDER_COUNT,
    cast(SUM(GUEST_ORDER_COUNT) as int) AS GUEST_ORDER_COUNT,
    cast(SUM(REPEAT_VIP_ORDER_COUNT)as int) AS REPEAT_VIP_ORDER_COUNT
from analytic.dbo.realtime_from_source
WHERE EVENT_STORE_BRAND='Savage X' AND EVENT_STORE_LOCATION='Retail'
 and EMPLOYEE_NUMBER <> '-1'
group by CONCAT('SXF-',STORE_CODE),
    DATE,
    SLOT
ORDER BY CONCAT('SXF-',STORE_CODE),
    DATE,
    SLOT;
"""

sxf_associate_level_grain_source_sql = """
    select
    CONCAT('SXF-',STORE_CODE),
    EMPLOYEE_NUMBER,
    FORMAT(DATE,'dd/MM/yyyy') AS DATE,
    cast(SUM(ORDER_COUNT) as int) AS ORDER_COUNT,
    CAST(SUM(PRODUCT_GROSS_REVENUE)AS DECIMAL(19,2)) AS PRODUCT_GROSS_REVENUE,
    cast(SUM(UNIT_COUNT) as int) AS UNIT_COUNT,
    cast( SUM(REFUND_ORDERS) as int) AS REFUND_ORDERS,
    CAST(SUM(PRODUCT_REFUND)AS DECIMAL(19,2)) AS PRODUCT_REFUND,
    cast(SUM(REFUND_UNITS) as int) AS REFUND_UNITS,
    cast(SUM(ACTIVATING_ORDER_COUNT)as int) AS ACTIVATING_VIP_ORDER_COUNT,
    cast(SUM(GUEST_ORDER_COUNT) as int) AS GUEST_ORDER_COUNT,
    cast(SUM(REPEAT_VIP_ORDER_COUNT) as int) AS REPEAT_VIP_ORDER_COUNT
from analytic.dbo.realtime_from_source
WHERE EVENT_STORE_BRAND='Savage X' AND EVENT_STORE_LOCATION='Retail'
 and EMPLOYEE_NUMBER <> '-1'
group by CONCAT('SXF-',STORE_CODE),
    EMPLOYEE_NUMBER,
    DATE
ORDER BY CONCAT('SXF-',STORE_CODE),
    EMPLOYEE_NUMBER,
    DATE;
"""

fl_s3_key = "lake/storeforce/fabletics"
sxf_s3_key = "lake/storeforce/savagex"

date_param = "{{ from_ds(data_interval_start) }}"

export_sql_cmd = r"""
DELETE FROM lake_stg.storeforce.{store_brand_abbr}_store_level_intraday_feed_stg;

COPY INTO lake_stg.storeforce.{store_brand_abbr}_store_level_intraday_feed_stg (
    store_code,
    date,
    slot,
    order_count,
    product_gross_revenue,
    unit_count,
    refund_orders,
    product_refund,
    refund_units,
    activating_vip_order_count,
    guest_order_count,
    repeat_vip_order_count,
    metadata_filename,
    snapshot_datetime
)
    from (select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, metadata$filename,
          TO_TIMESTAMP(SPLIT_PART(SPLIT_PART(metadata$filename,'_', 3), '.', 1), 'YYYYMMDDTHH24MISS')
    FROM '@lake_stg.public.tsos_da_int_inbound/{s3_key}/daily_pos_{date_param}')
    FILE_FORMAT=(
        TYPE = CSV,
        FIELD_DELIMITER = '\t',
        RECORD_DELIMITER = '\n',
        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
        SKIP_HEADER = 0,
        ESCAPE_UNENCLOSED_FIELD = NONE,
        NULL_IF = ('')
    )
    ON_ERROR = 'SKIP_FILE';


MERGE INTO storeforce.{store_brand_abbr}_store_level_intraday_feed t
USING (
    SELECT
        store_code,
        date,
        slot,
        order_count,
        product_gross_revenue,
        unit_count,
        refund_orders,
        product_refund,
        refund_units,
        activating_vip_order_count,
        guest_order_count,
        repeat_vip_order_count,
        metadata_filename,
        snapshot_datetime,
        hash(*) AS meta_row_hash,
        current_timestamp AS meta_create_datetime,
        current_timestamp AS meta_update_datetime
    FROM lake_stg.storeforce.{store_brand_abbr}_store_level_intraday_feed_stg
    WHERE snapshot_datetime = (SELECT MAX(snapshot_datetime)
        FROM lake_stg.storeforce.{store_brand_abbr}_store_level_intraday_feed_stg
        WHERE TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', snapshot_datetime)) < current_date)
) s ON equal_null(t.store_code, s.store_code)
    AND equal_null(t.date, s.date)
    AND equal_null(t.slot, s.slot)
    AND equal_null(t.snapshot_datetime, s.snapshot_datetime)
WHEN NOT MATCHED THEN INSERT (
    store_code,
    date,
    slot,
    order_count,
    product_gross_revenue,
    unit_count,
    refund_orders,
    product_refund,
    refund_units,
    activating_vip_order_count,
    guest_order_count,
    repeat_vip_order_count,
    metadata_filename,
    snapshot_datetime,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
)
VALUES (
    store_code,
    date,
    slot,
    order_count,
    product_gross_revenue,
    unit_count,
    refund_orders,
    product_refund,
    refund_units,
    activating_vip_order_count,
    guest_order_count,
    repeat_vip_order_count,
    metadata_filename,
    snapshot_datetime,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
THEN UPDATE
SET
    t.store_code = s.store_code,
    t.date = s.date,
    t.slot = s.slot,
    t.order_count = s.order_count,
    t.product_gross_revenue = s.product_gross_revenue,
    t.unit_count = s.unit_count,
    t.refund_orders = s.refund_orders,
    t.product_refund = s.product_refund,
    t.refund_units = s.refund_units,
    t.activating_vip_order_count = s.activating_vip_order_count,
    t.guest_order_count = s.guest_order_count,
    t.repeat_vip_order_count = s.repeat_vip_order_count,
    t.metadata_filename = s.metadata_filename,
    t.snapshot_datetime = s.snapshot_datetime,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime;
"""


def check_time(**context):
    execution_time = context["data_interval_end"].in_timezone("America/Los_Angeles")
    if execution_time.hour == 3 and execution_time.minute < 30:
        return [to_fl_snowflake_export.task_id, to_sxf_snowflake_export.task_id]
    return []


with dag:
    with TaskGroup(group_id="fl-storeforce") as tg1:
        fl_storeforce_mssql = MsSqlOperator(
            sql="[analytic].[dbo].[usp_store_force]",
            task_id="fl_mssql_procedure_store_force",
            mssql_conn_id=conn_ids.MsSql.fabletics_app_airflow,
            priority_weight=990,
            sla=timedelta(minutes=25),
        )
        fl_store_level_sftp = MssqlToSFTPOperator(
            task_id="fl_store_level_grain_source_sftp_export",
            sql_or_path=fl_store_level_grain_source_sql,
            mssql_conn_id=conn_ids.MsSql.fabletics_app_airflow,
            sftp_conn_id=conn_ids.SFTP.sftp_storeforce,
            remote_dir="/POS",
            filename="daily_pos.csv",
            header=False,
            priority_weight=990,
            sla=timedelta(minutes=25),
        )

        fl_associate_level_sftp = MssqlToSFTPOperator(
            task_id="fl_associate_level_grain_source_sftp_export",
            sql_or_path=fl_associate_level_grain_source_sql,
            mssql_conn_id=conn_ids.MsSql.fabletics_app_airflow,
            sftp_conn_id=conn_ids.SFTP.sftp_storeforce,
            remote_dir="/EmployeeSales",
            filename="daily_employeesales.csv",
            header=False,
            priority_weight=990,
            sla=timedelta(minutes=25),
        )

        # fl_storeforce_mssql >> fl_store_level_sftp >> fl_associate_level_sftp

        fl_store_level_to_s3 = MsSqlToS3Operator(
            task_id="fl_store_level_grain_source_s3_export",
            sql=fl_store_level_grain_source_sql,
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f"{fl_s3_key}/daily_pos_{{{{ ts_nodash }}}}.csv.gz",
            mssql_conn_id=conn_ids.MsSql.fabletics_app_airflow,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            s3_replace=False,
        )

        fl_associate_level_to_s3 = MsSqlToS3Operator(
            task_id="fl_associate_level_grain_source_s3_export",
            sql=fl_associate_level_grain_source_sql,
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f"{fl_s3_key}/daily_employeesales_{{{{ ts_nodash }}}}.csv.gz",
            mssql_conn_id=conn_ids.MsSql.fabletics_app_airflow,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            s3_replace=False,
        )

        (
            fl_storeforce_mssql
            >> fl_store_level_sftp
            >> fl_associate_level_sftp
            >> fl_store_level_to_s3
            >> fl_associate_level_to_s3
        )

    with TaskGroup(group_id="sxf-storeforce") as tg2:
        sxf_storeforce_mssql = MsSqlOperator(
            sql="[analytic].[dbo].[usp_store_force]",
            task_id="sxf_mssql_procedure_store_force",
            mssql_conn_id=conn_ids.MsSql.savagex_app_airflow,
            priority_weight=990,
        )
        sxf_store_level_sftp = MssqlToSFTPOperator(
            task_id="sxf_store_level_grain_source_sftp_export",
            sql_or_path=sxf_store_level_grain_source_sql,
            mssql_conn_id=conn_ids.MsSql.savagex_app_airflow,
            sftp_conn_id=conn_ids.SFTP.sftp_storeforce_sxf,
            remote_dir="/POS",
            filename="daily_pos.csv",
            header=False,
            priority_weight=990,
        )

        sxf_associate_level_sftp = MssqlToSFTPOperator(
            task_id="sxf_associate_level_grain_source_sftp_export",
            sql_or_path=sxf_associate_level_grain_source_sql,
            mssql_conn_id=conn_ids.MsSql.savagex_app_airflow,
            sftp_conn_id=conn_ids.SFTP.sftp_storeforce_sxf,
            remote_dir="/EmployeeSales",
            filename="daily_employeesales.csv",
            header=False,
            priority_weight=990,
        )

        sxf_store_level_to_s3 = MsSqlToS3Operator(
            task_id="sxf_store_level_grain_source_s3_export",
            sql=sxf_store_level_grain_source_sql,
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f"{sxf_s3_key}/daily_pos_{{{{ ts_nodash }}}}.csv.gz",
            mssql_conn_id=conn_ids.MsSql.savagex_app_airflow,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            s3_replace=False,
        )

        sxf_associate_level_to_s3 = MsSqlToS3Operator(
            task_id="sxf_associate_level_grain_source_s3_export",
            sql=sxf_associate_level_grain_source_sql,
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f"{sxf_s3_key}/daily_employeesales_{{{{ ts_nodash }}}}.csv.gz",
            mssql_conn_id=conn_ids.MsSql.savagex_app_airflow,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            s3_replace=False,
        )

        (
            sxf_storeforce_mssql
            >> sxf_store_level_sftp
            >> sxf_associate_level_sftp
            >> sxf_store_level_to_s3
            >> sxf_associate_level_to_s3
        )

    check_eod = BranchPythonOperator(task_id="check_time", python_callable=check_time)

    to_fl_snowflake_export = SnowflakeSqlOperator(
        task_id="to_fl_snowflake_export",
        sql_or_path=export_sql_cmd.format(
            store_brand_abbr="fl", s3_key=fl_s3_key, date_param=date_param
        ),
        database="lake",
        warehouse="DA_WH_ETL_LIGHT",
    )

    to_sxf_snowflake_export = SnowflakeSqlOperator(
        task_id="to_sxf_snowflake_export",
        sql_or_path=export_sql_cmd.format(
            store_brand_abbr="sxf", s3_key=sxf_s3_key, date_param=date_param
        ),
        database="lake",
        warehouse="DA_WH_ETL_LIGHT",
    )

    (
        [fl_associate_level_to_s3, sxf_associate_level_to_s3]
        >> check_eod
        >> [to_fl_snowflake_export, to_sxf_snowflake_export]
    )
