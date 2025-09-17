from pathlib import Path
import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from include.airflow.operators.snowflake import SnowflakeAlertOperator
from include.utils import blueyonder
from include.airflow.operators.blueyonder import BlueYonderExportFeed
from include.airflow.operators.sftp import SFTPPutTouchFileOperator
from include.config import email_lists, owners, conn_ids
from include.airflow.callbacks.slack import slack_failure_edm
from include import SQL_DIR

default_args = {
    "start_date": pendulum.datetime(2024, 8, 1, 7, tz="America/Los_Angeles"),
    "retries": 2,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_outbound_sftp_blue_yonder_data_feed_daily",
    default_args=default_args,
    schedule="45 6 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=6,
)

incorrect_hierarchy_sql = """
select distinct product_sku, concat(product_segment_desc,sub_dept_desc,class_desc,sub_class_desc)
    as Incorrect_Hierarchy
from REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM
where Incorrect_Hierarchy not in(select distinct (concat(product_segment, category,class,subclass))
from LAKE_VIEW.SHAREPOINT.DREAMSTATE_UBT_HIERARCHY);
"""

multiple_categories_sql = """
select item.product_segment_desc as product_segments,
    item.style,
    count(distinct item.class) as classes,
    count(distinct item.sub_class) as sub_classes,
    count(distinct item.division) as divisions,
    count(distinct item.sub_dept) as sub_depts
from REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM item
join REPORTING_BASE_PROD.FABLETICS.UBT_MAX_SHOWROOM_NO_DATE_DH ubt on ubt.SKU = item.PRODUCT_SKU
group by item.product_segment_desc,item.style
having greatest(count(distinct item.class),
                count(distinct item.sub_class),
                count(distinct item.division),
                count(distinct item.sub_dept)
                ) > 1
order by greatest(classes, sub_classes, divisions, sub_depts) desc;
"""

alert_email_list = [
    "AGibson@fabletics.com",
    "CLiu@fabletics.com",
    "ChLambiase@fabletics.com",
    "MCagney@fabletics.com",
    "aflores@Fabletics.com",
    "MArabe@fabletics.com",
    "JFloresta@Fabletics.com",
    "bpatterson@fabletics.com",
    "JAikens@Fabletics.com",
    "JDeNoble@fabletics.com",
    "dklamerus@fabletics.com",
    "DLongsworth@fabletics.com",
    "LDeveau@Fabletics.com",
    "VaRodriguez@fabletics.com",
    "BPalazzo@fabletics.com",
    "MaMitchell@fabletics.com",
    "TFarrell@TechStyle.com",
    "koreilly@fabletics.com",
    "savangala@fabletics.com",
    "yyoruk@techstyle.com",
    "kogata@techstyle.com",
    "srapolu@techstyle.com",
    "lasplund@techstyle.com",
    "kraja@techstyle.com",
    "iquinto@techstyle.com",
]
with dag:
    incorrect_hierarchy = SnowflakeAlertOperator(
        task_id="incorrect_hierarchy_data_validation",
        sql_or_path=incorrect_hierarchy_sql,
        database="snowflake",
        alert_type="mail",
        subject="Alert: Incorrect Hierarchy in UBT",
        body=f"""<html>
        <h1 style="font-size: 5em; color: red">INCORRECT HIERARCHY</h1>
        <p>query for this: <br>{incorrect_hierarchy_sql}</p>
        <h2 style="font-size: 4em">see below for details</h2>
        </html>
        """,
        distribution_list=alert_email_list,
    )

    multiple_categories = SnowflakeAlertOperator(
        task_id="multiple_categories_data_validation",
        sql_or_path=multiple_categories_sql,
        database="snowflake",
        alert_type="mail",
        subject="Alert: Multiple categories for a single style",
        body=f"""<html>
        <h1 style="font-size: 5em; color: red">MULTIPLE CATEGORIES</h1>
        <p>query for this: <br>{multiple_categories_sql}</p>
        <h2 style="font-size: 4em">see below for details</h2>
        </html>
        """,
        distribution_list=alert_email_list,
    )
    empty_task = EmptyOperator(task_id="group_in_tasks")
    trigger = SFTPPutTouchFileOperator(
        task_id="trigger_file_drop_completed",
        ssh_conn_id=conn_ids.SFTP.sftp_blue_yonder_prod,
        local_filepath="pdcinbound_trigger.done",
        remote_filepath="/regular/pdcinbound_trigger.done",
    )

    tasks = {}

    interfaces_list = [
        "transportload",
        "schedrcpts",
        "inventory",
        "inventory_transaction",
        "network",
        "dfu",
        "history",
        "worklist",
        "sku",
        "calendar",
        "loc",
        "item",
        "itemhierarchy",
        "lochierarchy",
    ]
    for i in interfaces_list:
        cfg = blueyonder.config_list[i]
        tasks[i] = BlueYonderExportFeed(
            task_id=f"{cfg.name}_export_data_feed",
            sftp_conn_id=conn_ids.SFTP.sftp_blue_yonder_prod,
            file_name=cfg.file_name,
            remote_filepath="/regular",
            table_name=f"export_by_{cfg.name}_interface",
            append=cfg.append,
            generate_empty_file=cfg.generate_empty_file,
            column_list=cfg.column_list,
            sql_or_path=Path(
                SQL_DIR,
                "reporting_prod",
                "procedures",
                f"blue_yonder.export_by_{cfg.name}_interface.sql",
            ),
        )

        [incorrect_hierarchy, multiple_categories] >> empty_task >> tasks[i] >> trigger

    tasks["loc"] >> tasks["lochierarchy"]
    tasks["item"] >> tasks["itemhierarchy"]
    tasks["item"] >> tasks["history"]
