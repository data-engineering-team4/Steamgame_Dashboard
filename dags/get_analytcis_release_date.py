from datetime import datetime, timedelta
import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'analytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    # 'on_failure_callback': slack.on_failure_callback
}

with DAG(
    dag_id = 'get_analytics_release_date',
    start_date=datetime.datetime(2023,6,26),
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    default_args=default_args, 
    catchup=False
) as dag:

    get_analytcis_release_date = BashOperator(
        task_id='get_analytics_release_date',
        bash_command='cd /dbt/steam_api_dbt && dbt run --profiles-dir . --models analytics_release_date',
        env={
            'dbt_user': '{{ var.value.dbt_snowflake_user }}',
            'dbt_password': '{{ var.value.dbt_snowflake_password }}',
            **os.environ
        },
        dag=dag
    )

get_analytcis_release_date