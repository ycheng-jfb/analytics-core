import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from edm.acquisition.configs import (
    get_all_lake_consolidated_configs,
    get_lake_consolidated_table_config,
)
from edm.acquisition.configs.edw_lake_consolidated_source_tables import edw_source_list
from edm.acquisition.configs.lake_consolidated_exclusion_list import (
    lake_consolidated_exclusion_table_list as exclusion_list,
)
from edm.acquisition.configs.lake_consolidated_high_frequency_tables import (
    table_list as high_freq_table_list,
)
from edm.acquisition.configs.lake_consolidated_history_high_frequency_tables import (
    history_table_list as high_freq_history_table_list,
)
from edm.acquisition.configs.lake_consolidated_history_other_tables import history_table_list
from edm.acquisition.configs.lake_consolidated_intra_day_tables import (
    table_list as intra_day_table_list,
)
from edm.acquisition.configs.lake_consolidated_history_intra_day_tables import (
    history_table_list as intra_day_history_table_list,
)

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.hvr_sensor import hvr_sensor
from include.config import owners
from include.config.email_lists import engineering_support

default_args = {
    'start_date': pendulum.datetime(2023, 1, 1, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': engineering_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_lake_consolidated_other',
    default_args=default_args,
    schedule="0 13 * * *",
    catchup=False,
    max_active_runs=1,
)

with dag:
    warehouse = 'DA_WH_ETL_LIGHT'
    acquisition_complete = EmptyOperator(task_id='consolidation_completion')

    hvr_sensor_gdpr = hvr_sensor(
        task_id='hvr_sensor_gdpr',
        schema='GDPR',
        lookback_minutes=45,
    )

    hvr_sensor_ultra_cart = hvr_sensor(
        task_id='hvr_sensor_ultra_cart',
        schema='ULTRA_CART',
        lookback_minutes=45,
    )

    hvr_sensor_ultra_cms = hvr_sensor(
        task_id='hvr_sensor_ultra_cms',
        schema='ULTRA_CMS',
        lookback_minutes=45,
    )

    hvr_sensor_ultra_cms_history = hvr_sensor(
        task_id='hvr_sensor_ultra_cms_history',
        schema='ULTRA_CMS_HISTORY',
        lookback_minutes=45,
    )

    hvr_sensor_ultra_merchant = hvr_sensor(
        task_id='hvr_sensor_ultra_merchant',
        schema='ULTRA_MERCHANT',
        lookback_minutes=45,
    )

    hvr_sensor_ultra_merchant_history = hvr_sensor(
        task_id='hvr_sensor_ultra_merchant_history',
        schema='ULTRA_MERCHANT_HISTORY',
        lookback_minutes=45,
    )

    hvr_sensor_ultra_rollup = hvr_sensor(
        task_id='hvr_sensor_ultra_rollup',
        schema='ULTRA_ROLLUP',
        lookback_minutes=45,
    )

    hvr_acquisition = EmptyOperator(task_id='hvr_acquisition')

    hvr_sensor_gdpr >> hvr_acquisition
    hvr_sensor_ultra_cart >> hvr_acquisition
    hvr_sensor_ultra_cms >> hvr_acquisition
    hvr_sensor_ultra_cms_history >> hvr_acquisition
    hvr_sensor_ultra_merchant >> hvr_acquisition
    hvr_sensor_ultra_merchant_history >> hvr_acquisition
    hvr_sensor_ultra_rollup >> hvr_acquisition

    table_config_list = list(get_all_lake_consolidated_configs())
    for cfg in table_config_list:
        if (
            cfg.full_target_table_name.lower() not in map(str.lower, edw_source_list)
            and cfg.full_target_table_name.lower() not in map(str.lower, high_freq_table_list)
            and cfg.full_target_table_name.lower() not in map(str.lower, intra_day_table_list)
            and cfg.full_target_table_name.lower() not in map(str.lower, exclusion_list)
        ):
            to_lake_consolidated = cfg.to_lake_consolidated_operator
            to_lake_consolidated.warehouse = warehouse
            hvr_acquisition >> to_lake_consolidated >> acquisition_complete

    for table_name in history_table_list:
        if (
            # table_name.lower() not in map(str.lower, edw_source_list) and
            table_name.lower() not in map(str.lower, high_freq_history_table_list)
            and table_name.lower() not in map(str.lower, intra_day_history_table_list)
            and table_name.lower() not in map(str.lower, exclusion_list)
        ):
            cfg = get_lake_consolidated_table_config(table_name)
            to_lake_consolidated_history = cfg.to_lake_consolidated_history_operator
            to_lake_consolidated_history.warehouse = warehouse
            hvr_acquisition >> to_lake_consolidated_history >> acquisition_complete
