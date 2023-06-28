from datetime import datetime
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,8,1),
    'retries': 0
}

with DAG('dbt_test_dag_seed', default_args=default_args, schedule_interval='@once') as dag:

    task_1 = BashOperator(
        task_id='dbt_test_dag_seed',
        bash_command='cd /dbt/steam_api_dbt && dbt seed --profiles-dir .',
        env={
            'dbt_user': '{{ var.value.dbt_snowflake_user }}',
            'dbt_password': '{{ var.value.dbt_snowflake_password }}',
            **os.environ
        },
        dag=dag
    )

task_1