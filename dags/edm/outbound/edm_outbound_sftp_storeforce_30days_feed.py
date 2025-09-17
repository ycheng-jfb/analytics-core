import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_export import SnowflakeToSFTPOperator
from include.config import conn_ids, owners
from include.config.email_lists import data_integration_support

fl_store_level_grain_edw_sql = """
select
    STORE_CODE,
    TO_CHAR(DATE,'DD/MM/YYYY') AS DATE,
    TO_CHAR(SLOT,'HH24:MI') AS SLOT,
    SUM(ORDER_COUNT) AS ORDER_COUNT,
    SUM(PRODUCT_GROSS_REVENUE)::NUMBER(19,2) AS PRODUCT_GROSS_REVENUE,
    SUM(UNIT_COUNT) AS UNIT_COUNT,
    SUM(REFUND_ORDERS) AS REFUND_ORDERS,
    SUM(PRODUCT_REFUND)::NUMBER(19,2) AS PRODUCT_REFUND,
    SUM(REFUND_UNITS) AS REFUND_UNITS,
    SUM(ACTIVATING_ORDER_COUNT) AS ACTIVATING_VIP_ORDER_COUNT,
    SUM(GUEST_ORDER_COUNT) AS GUEST_ORDER_COUNT,
    SUM(REPEAT_VIP_ORDER_COUNT) AS REPEAT_VIP_ORDER_COUNT,
    SUM(GROSS_MARGIN) AS GROSS_MARGIN
from reporting_prod.retail.realtime_edw
WHERE EVENT_STORE_BRAND='Fabletics' AND EVENT_STORE_LOCATION='Retail'
group by STORE_CODE,
        DATE,
        SLOT
ORDER BY STORE_CODE,
        DATE,
        SLOT
"""
fl_associate_level_grain_edw_sql = """
select
    STORE_CODE,
    EMPLOYEE_NUMBER,
    TO_CHAR(DATE,'DD/MM/YYYY') AS DATE,
    SUM(ORDER_COUNT) AS ORDER_COUNT,
    SUM(PRODUCT_GROSS_REVENUE)::NUMBER(19,2) AS PRODUCT_GROSS_REVENUE,
    SUM(UNIT_COUNT) AS UNIT_COUNT,
    SUM(REFUND_ORDERS) AS REFUND_ORDERS,
    SUM(PRODUCT_REFUND)::NUMBER(19,2) AS PRODUCT_REFUND,
    SUM(REFUND_UNITS) AS REFUND_UNITS,
    SUM(ACTIVATING_ORDER_COUNT) AS ACTIVATING_VIP_ORDER_COUNT,
    SUM(GUEST_ORDER_COUNT) AS GUEST_ORDER_COUNT,
    SUM(REPEAT_VIP_ORDER_COUNT) AS REPEAT_VIP_ORDER_COUNT,
    SUM(GROSS_MARGIN) AS GROSS_MARGIN
from reporting_prod.retail.realtime_edw
WHERE EVENT_STORE_BRAND='Fabletics' AND EVENT_STORE_LOCATION='Retail'
    and EMPLOYEE_NUMBER <> '-1'
group by STORE_CODE,
    EMPLOYEE_NUMBER,
    DATE
ORDER BY STORE_CODE,
    EMPLOYEE_NUMBER,
    DATE
"""

sxf_store_level_grain_edw_sql = """
select
    CONCAT('SXF-',STORE_CODE),
    TO_CHAR(DATE,'DD/MM/YYYY') AS DATE,
    TO_CHAR(SLOT,'HH24:MI') AS SLOT,
    SUM(ORDER_COUNT) AS ORDER_COUNT,
    SUM(PRODUCT_GROSS_REVENUE)::NUMBER(19,2) AS PRODUCT_GROSS_REVENUE,
    SUM(UNIT_COUNT) AS UNIT_COUNT,
    SUM(REFUND_ORDERS) AS REFUND_ORDERS,
    SUM(PRODUCT_REFUND)::NUMBER(19,2) AS PRODUCT_REFUND,
    SUM(REFUND_UNITS) AS REFUND_UNITS,
    SUM(ACTIVATING_ORDER_COUNT) AS ACTIVATING_VIP_ORDER_COUNT,
    SUM(GUEST_ORDER_COUNT) AS GUEST_ORDER_COUNT,
    SUM(REPEAT_VIP_ORDER_COUNT) AS REPEAT_VIP_ORDER_COUNT
from reporting_prod.retail.realtime_edw
WHERE EVENT_STORE_BRAND='Savage X' AND EVENT_STORE_LOCATION='Retail'
    and EMPLOYEE_NUMBER <> '-1'
group by     CONCAT('SXF-',STORE_CODE),
        DATE,
        SLOT
ORDER BY     CONCAT('SXF-',STORE_CODE),
        DATE,
        SLOT
"""
sxf_associate_level_grain_edw_sql = """
select
        CONCAT('SXF-',STORE_CODE),
    EMPLOYEE_NUMBER,
    TO_CHAR(DATE,'DD/MM/YYYY') AS DATE,
    SUM(ORDER_COUNT) AS ORDER_COUNT,
    SUM(PRODUCT_GROSS_REVENUE)::NUMBER(19,2) AS PRODUCT_GROSS_REVENUE,
    SUM(UNIT_COUNT) AS UNIT_COUNT,
    SUM(REFUND_ORDERS) AS REFUND_ORDERS,
    SUM(PRODUCT_REFUND)::NUMBER(19,2) AS PRODUCT_REFUND,
    SUM(REFUND_UNITS) AS REFUND_UNITS,
    SUM(ACTIVATING_ORDER_COUNT) AS ACTIVATING_VIP_ORDER_COUNT,
    SUM(GUEST_ORDER_COUNT) AS GUEST_ORDER_COUNT,
    SUM(REPEAT_VIP_ORDER_COUNT) AS REPEAT_VIP_ORDER_COUNT
from reporting_prod.retail.realtime_edw
WHERE EVENT_STORE_BRAND='Savage X' AND EVENT_STORE_LOCATION='Retail'
    and EMPLOYEE_NUMBER <> '-1'
group by     CONCAT('SXF-',STORE_CODE),
    EMPLOYEE_NUMBER,
    DATE
ORDER BY     CONCAT('SXF-',STORE_CODE),
    EMPLOYEE_NUMBER,
    DATE
"""

fl_store_level_grain_edw_product_classification_sql = """
SELECT
    store_code,
    TO_CHAR(DATE,'DD/MM/YYYY') AS DATE,
    SUM(women_tops_dollar) AS women_tops_dollar,
    SUM(women_tops_units) AS women_tops_units,
    SUM(women_bottoms_dollar) AS women_bottoms_dollar,
    SUM(women_bottoms_units) AS women_bottoms_units,
    SUM(women_onesie_dollar) AS women_onesie_dollar,
    SUM(women_onesie_units) AS women_onesie_units,
    SUM(women_jackets_dollar) AS women_jackets_dollar,
    SUM(women_jackets_units) AS women_jackets_units,
    SUM(women_bra_dollar) AS women_bra_dollar,
    SUM(women_bra_units) AS women_bra_units,
    SUM(women_under_dollar) AS women_under_dollar,
    SUM(women_under_units) AS women_under_units,
    SUM(women_swim_dollar) AS women_swim_dollar,
    SUM(women_swim_units) AS women_swim_units,
    SUM(men_tops_dollar) AS men_tops_dollar,
    SUM(men_tops_units) AS men_tops_units,
    SUM(men_bottoms_dollar) AS men_bottoms_dollar,
    SUM(men_bottoms_units) AS men_bottoms_units,
    SUM(men_jackets_dollar) AS men_jackets_dollar,
    SUM(men_jackets_units) AS men_jackets_units,
    SUM(men_under_dollar) AS men_under_dollar,
    SUM(men_under_units) AS men_under_units,
    SUM(accessories_dollar) AS accessories_dollar,
    SUM(accessories_units) AS accessories_units
FROM reporting_prod.retail.realtime_edw_product_classification
WHERE event_store_location = 'Retail'
GROUP BY store_code,
    TO_CHAR(DATE,'DD/MM/YYYY')
ORDER BY STORE_CODE,
    DATE
"""

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2021, 7, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_outbound_sftp_storeforce_30days_feed",
    default_args=default_args,
    schedule="0 4 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)

with dag:
    realtime_edw = SnowflakeProcedureOperator(
        procedure="retail.realtime_edw.sql", database="reporting_prod", autocommit=False
    )
    fl_store_level_grain_snapshot = SnowflakeProcedureOperator(
        procedure="retail.fl_pos_snapshot.sql",
        database="reporting_prod",
        autocommit=False,
    )
    fl_store_level_grain_edw = SnowflakeToSFTPOperator(
        task_id="fl_store_level_grain_edw",
        sql_or_path=fl_store_level_grain_edw_sql,
        sftp_conn_id=conn_ids.SFTP.sftp_storeforce,
        filename="pos.csv",
        sftp_dir="/POS",
        field_delimiter=",",
    )
    fl_associate_level_grain_snapshot = SnowflakeProcedureOperator(
        procedure="retail.fl_employeesales_snapshot.sql",
        database="reporting_prod",
        autocommit=False,
    )
    fl_associate_level_grain_edw = SnowflakeToSFTPOperator(
        task_id="fl_associate_level_grain_edw",
        sql_or_path=fl_associate_level_grain_edw_sql,
        sftp_conn_id=conn_ids.SFTP.sftp_storeforce,
        filename="employeesales.csv",
        sftp_dir="/EmployeeSales",
        field_delimiter=",",
    )
    sxf_store_level_grain_snapshot = SnowflakeProcedureOperator(
        procedure="retail.sxf_pos_snapshot.sql",
        database="reporting_prod",
        autocommit=False,
    )
    sxf_store_level_grain_edw = SnowflakeToSFTPOperator(
        task_id="sxf_store_level_grain_edw",
        sql_or_path=sxf_store_level_grain_edw_sql,
        sftp_conn_id=conn_ids.SFTP.sftp_storeforce_sxf,
        filename="pos.csv",
        sftp_dir="/POS",
        field_delimiter=",",
    )

    sxf_associate_level_grain_snapshot = SnowflakeProcedureOperator(
        procedure="retail.sxf_employeesales_snapshot.sql",
        database="reporting_prod",
        autocommit=False,
    )
    sxf_associate_level_grain_edw = SnowflakeToSFTPOperator(
        task_id="sxf_associate_level_grain_edw",
        sql_or_path=sxf_associate_level_grain_edw_sql,
        sftp_conn_id=conn_ids.SFTP.sftp_storeforce_sxf,
        filename="employeesales.csv",
        sftp_dir="/EmployeeSales",
        field_delimiter=",",
    )
    realtime_edw_product_classification = SnowflakeProcedureOperator(
        procedure="retail.realtime_edw_product_classification.sql",
        database="reporting_prod",
        autocommit=False,
    )
    fl_store_level_grain_edw_product_classification = SnowflakeToSFTPOperator(
        task_id="fl_store_level_grain_edw_product_classification",
        sql_or_path=fl_store_level_grain_edw_product_classification_sql,
        sftp_conn_id=conn_ids.SFTP.sftp_storeforce,
        filename="Custom.txt",
        sftp_dir="/Custom",
        field_delimiter=",",
    )
    realtime_edw >> [
        fl_store_level_grain_snapshot,
        fl_store_level_grain_edw,
        fl_associate_level_grain_snapshot,
        fl_associate_level_grain_edw,
        sxf_store_level_grain_snapshot,
        sxf_store_level_grain_edw,
        sxf_associate_level_grain_snapshot,
        sxf_associate_level_grain_edw,
    ]
    (
        realtime_edw_product_classification
        >> fl_store_level_grain_edw_product_classification
    )
