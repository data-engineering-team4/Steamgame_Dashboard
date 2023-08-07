from datetime import datetime, timedelta
import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from plugins import slack

default_args = {
    'owner': 'SUNHO',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    #'on_failure_callback': slack.on_failure_callback
}

with DAG(
    dag_id = 'get_fact_and_analytics',
    start_date=datetime.datetime(2023,6,26),
    max_active_runs=1,
    default_args=default_args, 
    catchup=False
) as dag:
    
    get_fact = BashOperator(
        task_id='get_fact',
        bash_command='cd /dbt/steam_api_dbt && dbt run --profiles-dir . --model fact',
        env={
            'dbt_user': '{{ var.value.dbt_snowflake_user }}',
            'dbt_password': '{{ var.value.dbt_snowflake_password }}',
            **os.environ
        },
        dag=dag
    )

    get_analytics = BashOperator(
        task_id='get_analytics',
        bash_command='cd /dbt/steam_api_dbt && dbt run --profiles-dir . --model analytics',
        env={
            'dbt_user': '{{ var.value.dbt_snowflake_user }}',
            'dbt_password': '{{ var.value.dbt_snowflake_password }}',
            **os.environ
        },
        dag=dag
    )

get_fact >> get_analytics
