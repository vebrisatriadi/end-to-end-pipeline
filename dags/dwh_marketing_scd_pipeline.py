#
# GENERATED CODE. DON'T MODIFY THIS FILE !!!
#
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from dags.python_scripts.airflow_callback import callback
from dags.python_scripts.helper import invoke_cloud_functions
from string import Template
from airflow.operators.empty import EmptyOperator

# Airflow Config
default_args = {
    'owner': 'vebrisatriadi@gmail.com',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 31, 17),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': [],
    'retries': 5,
    'retry_delay': timedelta(seconds=300),
    'on_failure_callback': callback
}

dag_id = "dwh_marketing_scd_pipeline"
dag = DAG(
    dag_id,
    default_args=default_args,
    schedule_interval='0 17 * * *',
    catchup=False,
    concurrency=50,
    max_active_runs=1
)

execution_location = Variable.get('VEBRI_EXECUTION_LOCATION')
target_project_id = Variable.get('VEBRI_TARGET_PROJECT_ID')
start_date_iso = (
    '{{ execution_date'
    '.in_timezone("Asia/Jakarta").start_of("day")'
    '.in_timezone("UTC").isoformat() }}'
)
end_date_iso = (
    '{{ execution_date'
    '.in_timezone("Asia/Jakarta").add(days=1).start_of("day")'
    '.in_timezone("UTC").isoformat() }}'
)
run_date_ds = (
    '{{ execution_date'
    '.in_timezone("Asia/Jakarta").add(days=1).start_of("day")'
    '.in_timezone("UTC").date() }}'
)
run_date_ds_nodash = (
    '{{ execution_date'
    '.in_timezone("Asia/Jakarta").add(days=1).start_of("day")'
    '.in_timezone("UTC").date().format("YYYYMMDD") }}'
)
run_date_ds_historical = (
    '{{ execution_date'
    '.in_timezone("Asia/Jakarta").start_of("day")'
    '.in_timezone("UTC").date() }}'
)
run_date_ds_nodash_historical = (
    '{{ execution_date'
    '.in_timezone("Asia/Jakarta").start_of("day")'
    '.in_timezone("UTC").date().format("YYYYMMDD") }}'
)

task_finish_ingestion = EmptyOperator(
    task_id='ingestion_finished',
    dag=dag,
)

sensor_ingestion = EmptyOperator(
    task_id='sensor_ingestion',
    dag=dag,
)
sensor_ingest_marketing = ExternalTaskSensor(
    task_id='sensor_ingest_marketing',
    dag=dag,
    external_dag_id='ingest_marketing',
    external_task_id='ingestion_finished',
    poke_interval=300, # poke (and reschedule) every 5 minutes
    timeout=82800, # for daily dag run, set timeout to 23 hours, hourly -> 50 minutes
    mode="reschedule", # use reschedule mode, don't block the slot for a long time
)
sensor_ingest_marketing >> sensor_ingestion

