from datetime import datetime, timedelta
import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from plugins import slack

default_args = {
    'owner': 'SUNHO',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    #'on_failure_callback': slack.on_failure_callback
}

with DAG(
    dag_id = 'make_src_and_test_using_dbt',
    start_date=datetime.datetime(2023,6,26),
    max_active_runs=1,
    default_args=default_args, 
    catchup=False
) as dag:

    make_src = BashOperator(
        task_id='make_src',
        bash_command='cd /dbt/steam_api_dbt && dbt run --profiles-dir . --model src',
        env={
            'dbt_user': '{{ var.value.dbt_snowflake_user }}',
            'dbt_password': '{{ var.value.dbt_snowflake_password }}',
            **os.environ
        },
        dag=dag
    )

    do_dbt_src_test = BashOperator(
        task_id='do_dbt_src_test',
        bash_command='cd /dbt/steam_api_dbt && dbt test --profiles-dir .',
        env={
            'dbt_user': '{{ var.value.dbt_snowflake_user }}',
            'dbt_password': '{{ var.value.dbt_snowflake_password }}',
            **os.environ
        },
        dag=dag
    )

    trigger_get_fact_and_analytics_dag = TriggerDagRunOperator(
        task_id='trigger_get_fact_and_analytics_dag',
        trigger_dag_id='get_fact_and_analytics',
        execution_date="{{ execution_date }}"
    )

make_src >> do_dbt_src_test >> trigger_get_fact_and_analytics_dag
