#
# GENERATED CODE. DON'T MODIFY THIS FILE !!!
#
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
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

execution_project_id = Variable.get('VEBRI_EXECUTION_PROJECT_ID')
execution_location = Variable.get('VEBRI_EXECUTION_LOCATION')
cf_name = Variable.get('VEBRI_INGEST_CF_NAME')
invoke_url = f"https://{execution_location}-{execution_project_id}.cloudfunctions.net/{cf_name}"
secret = {
    'project_id': Variable.get('VEBRI_MARKETING_SECRET_PROJECT_ID'),
    'name': Variable.get('VEBRI_MARKETING_SECRET_NAME'),
    'version': Variable.get('VEBRI_MARKETING_SECRET_VERSION'),
}
decrypt_secret = {
    'project_id': Variable.get('VEBRI_DECRYPTION_SECRET_PROJECT_ID'),
    'name': Variable.get('VEBRI_DECRYPTION_SECRET_NAME'),
    'version': Variable.get('VEBRI_DECRYPTION_SECRET_VERSION'),
}
target_project_id = Variable.get('VEBRI_TARGET_PROJECT_ID')
decryption_url = Variable.get('VEBRI_DECRYPTION_URL')
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

dag_id = "ingest_marketing"
with DAG(
    dag_id,
    default_args=default_args,
    schedule_interval='0 17 * * *',
    catchup=False,
    concurrency=50,
    max_active_runs=1
    ) as dag:

    task_finish_ingestion = EmptyOperator(
        task_id='ingestion_finished',
        dag=dag,
    )


    with TaskGroup(group_id='activities') as task_group:
        table_name = "activities"
        target_table = f"{target_project_id}.l1_marketing.{table_name}"
        ingest_activities_op = PythonOperator(
            task_id='ingest_activities',
            python_callable=invoke_cloud_functions,
            op_kwargs={
                "data": {
                    'project_id': secret['project_id'],
                    'secret': secret['name'],
                    'version': secret['version'],
                    'db': "marketing",
                    'collection': "activities",
                    'rundt': run_date_ds,
                    'mongo_filter': {
                        'start_date': start_date_iso,
                        'end_date': end_date_iso
                    },
                    'target_table': target_table
                },
                "url": invoke_url
            },
            provide_context=True,
            dag=dag
        )

        table_name = "activities"
        target_table = f"{target_project_id}.l2_marketing.{table_name}"
        op = BigQueryExecuteQueryOperator(
            task_id='l2_activities',
            sql='sql/l2_marketing.activities.sql',
            destination_dataset_table=f'{target_table}${run_date_ds_nodash}',
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            schema_update_options=['ALLOW_FIELD_ADDITION'],
            query_params=[
                {
                    'name': 'start_date',
                    'parameterType': {'type': 'DATE'},
                    'parameterValue': {'value': run_date_ds}
                },
                {
                    'name': 'end_date',
                    'parameterType': {'type': 'DATE'},
                    'parameterValue': {'value': run_date_ds}
                }
            ],
            use_legacy_sql=False,
            time_partitioning={
                'type': 'DAY',
                'field': 'run_date'
            },
            location=execution_location,
            params={
                'project_id': target_project_id
            },
            dag=dag
        )
        l2_activities_op = op

        ingest_activities_op \
            >> l2_activities_op

        table_name = "activities"
        target_table = f"{target_project_id}.l2_marketing.{table_name}"
        op = BigQueryExecuteQueryOperator(
            task_id='l2_activities',
            sql='sql/l2_marketing.activities.sql',
            destination_dataset_table=f'{target_table}${run_date_ds_nodash}',
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            schema_update_options=['ALLOW_FIELD_ADDITION'],
            query_params=[
                {
                    'name': 'start_date',
                    'parameterType': {'type': 'DATE'},
                    'parameterValue': {'value': run_date_ds}
                },
                {
                    'name': 'end_date',
                    'parameterType': {'type': 'DATE'},
                    'parameterValue': {'value': run_date_ds}
                }
            ],
            use_legacy_sql=False,
            time_partitioning={
                'type': 'DAY',
                'field': 'run_date'
            },
            location=execution_location,
            params={
                'project_id': target_project_id
            },
            dag=dag
        )
        l2_activities_op = op
        l2_activities_op >> task_finish_ingestion

        
        list_subdags = []
        with TaskGroup(group_id='decryption') as decryption:
            table_name = "activities"
            field_name = "_id"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities__id = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "_type"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities__type = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "comment"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities_comment = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "created_at"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities_created_at = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "errors"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities_errors = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "field_name"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities_field_name = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "is_deleted"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities_is_deleted = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "is_read"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities_is_read = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "job_id"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities_job_id = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "lead_id"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities_lead_id = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "new_value"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities_new_value = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "old_value"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities_old_value = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "reminder_datetime"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities_reminder_datetime = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "updated_at"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities_updated_at = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "user_id"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities_user_id = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
            table_name = "activities"
            field_name = "validation_context"
            target_table = f"l2_marketing.l2_{table_name}_{field_name}_mapping"
            decrypt_activities_validation_context = PythonOperator(
                task_id=f'decrypt_{table_name}_{field_name}',
                python_callable=invoke_cloud_functions,
                op_kwargs={
                    "data": {
                        "source_project": target_project_id,
                        "source_table": f"l2_marketing.{table_name}",
                        "source_field": f"{field_name}",
                        "secret_project": decrypt_secret['project_id'],
                        "secret_name": decrypt_secret['name'],
                        "secret_version": decrypt_secret['version'],
                        "decryption_source": "",
                        "target_project": target_project_id,
                        "target_table": target_table,
                        "target_partition": run_date_ds_nodash,
                        "source_partition": {
                            "field": "run_date",
                            "start_date": run_date_ds,
                            "end_date": run_date_ds
                        }
                    },
                    "url": decryption_url
                },
                provide_context=True,
                dag=dag
            )
        list_subdags.append(decryption)
        l2_activities_op >> decryption >> l2_activities_op

# End of DAG
