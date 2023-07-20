from datetime import datetime, timedelta
import datetime
from airflow import DAG

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import XCom
from airflow.hooks.base import BaseHook

import logging
import os
from plugins import slack
import requests
import time
import pandas as pd



default_args = {
    'owner': 'Song',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    #'on_failure_callback': slack.on_failure_callback
}

with DAG(
        dag_id='game_status',
        start_date=datetime.datetime(2023, 6, 26),
        max_active_runs=1,
        default_args=default_args,
        catchup=False
) as dag:
    
    bucket_name = "steambucket4-2"
    folder_name = "game_status"
    file_name = "game_status"
    schema = "RAW_DATA"
    table="GAME_STATUS"
    aws_conn = BaseHook.get_connection("S3_conn")
    
    def get_game_info(**context):
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_raw_data')

        # SQL 문 실행 및 결과 가져오기
        sql = f'''
        SELECT GAME_ID
             , POSITIVE_CNT + NEGATIVE_CNT REVIEW_CNT 
          FROM GAME_INFO
        '''
        result = snowflake_hook.get_pandas_df(sql).to_dict(orient='records')

        # 결과를 딕셔너리 변수에 할당
        game_info = result
        logging.info(f"game_info {len(game_info)} data finished")
        
        return game_info
    
    def get_game_status(**context):
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        logging.info(current_date)
        
        game_status = []
        logging.info(context['game_info'])
        
        game_info = context['game_info']
        
        for g in game_info:
            game = {
                "GAME_ID": g["GAME_ID"],
                "REVIEW_CNT": g["REVIEW_CNT"],
                "CREATE_DT": current_date
            }
            
            url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={g['GAME_ID']}"
            response = requests.get(url)
            data = response.json()
            logging.info(g["GAME_ID"])

            if "response" in data and "player_count" in data["response"]:
                game["USER_CNT"] = data["response"]["player_count"]
            else:
                game["USER_CNT"] =  0
                
            game_status.append(game)
            
            time.sleep(1.3)  # 1.3초간 대기
        
        logging.info("get_game_status finished")
        
        return game_status
    
    def make_csv(**context):
        csv_filename = "data/game_status.csv"
        logging.info(context['game_status'])
        game_status = context['game_status']

        # 컬럼 순서를 지정한 리스트 생성
        columns = ['GAME_ID', 'CREATE_DT', 'USER_CNT', 'REVIEW_CNT']

        df = pd.DataFrame.from_records(game_status, columns=columns)  # 데이터프레임 생성
        df.to_csv(csv_filename, index=False)  # CSV 파일로 저장

        return csv_filename
    
    def upload_csv_to_s3(**context):
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
        csv_filename = context['csv_file_path']
        save_file_name = f"{file_name}_{current_date}.csv"
        logging.info(csv_filename)
        
        s3_hook = S3Hook(aws_conn_id='S3_conn')
        logging.info("connection finished")
        
        if csv_filename:
            s3_key = os.path.join(folder_name, save_file_name)
            logging.info(s3_key)
            logging.info("loop files start")
            s3_hook.load_file(
                filename=csv_filename,
                key=s3_key,
                bucket_name=bucket_name,
                replace=True,
            )
            os.remove(csv_filename)
            
        logging.info("loop files finished")
        
        return s3_key
    

    def s3_to_snowflake():
        s3_key= f"{folder_name}/{file_name}_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv"
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_raw_data')
        logging.info(snowflake_hook)
        
        logging.info("run snowflake")
        
        #full_refresh
        query = f'''
            BEGIN TRANSACTION;

            CREATE OR replace TEMPORARY TABLE temp_table AS
                       SELECT
                              $1 AS game_id
                            , $2 AS create_dt
                            , $3 AS user_cnt
                            , $4 AS review_cnt
                        FROM '@s3_stage/{s3_key}';
            
            DELETE FROM {table}
             WHERE DATE(CREATE_DT) = '{datetime.datetime.now().strftime('%Y-%m-%d')}'::DATE;
            
            INSERT INTO {table}
            SELECT tmp.*
              FROM temp_table tmp;
            
            COMMIT;
        '''

        
        logging.info(query)
        snowflake_hook.run(query)
        logging.info("run finished snowflake")
            

    # Task Group #csv 생성
    with TaskGroup("make_csv", tooltip="Transforming the Steamapi data and Saving csv") as section_make_csv:
        get_game_info_task = PythonOperator(
            task_id='get_game_info_task',
            python_callable=get_game_info,
            dag=dag
        )
        get_game_status_task = PythonOperator(
            task_id='get_game_status_task',
            python_callable=get_game_status,
            op_kwargs={
                'game_info': get_game_info_task.output
            },
            dag=dag
        )
        make_csv_task = PythonOperator(
            task_id='make_csv_task',
            python_callable=make_csv,
            op_kwargs={
                'game_status': get_game_status_task.output
            },
            dag=dag
        )
        get_game_info_task >> get_game_status_task >> make_csv_task
        
    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3_task',
        python_callable=upload_csv_to_s3,
        provide_context=True,
        op_kwargs={
            'csv_file_path': make_csv_task.output
        },
        dag=dag
    )
    
    # Task Group #s3 to snowflake 적재 
    with TaskGroup("s3_to_snowflake", tooltip="loading csv file from s3 to snowflake") as section_s3_to_snowflake:
        check_csv_file_exists_task = S3KeySensor(
            task_id='check_csv_file_exists_task',
            bucket_key=f"s3://{bucket_name}/{folder_name}/{file_name}_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv",
            wildcard_match=True,
            aws_conn_id='S3_conn',
            timeout=18 * 60 * 60,
            poke_interval=10 * 60,
        )
        
        
        s3_to_snowflake_task = PythonOperator(
            task_id='s3_to_snowflake_task',
            python_callable=s3_to_snowflake,
            dag=dag
        )
        
        check_csv_file_exists_task >> s3_to_snowflake_task
    
    trigger_comment_data_dag = TriggerDagRunOperator(
        task_id='trigger_comment_data_dag',
        trigger_dag_id='comment_data',  # game_status 대그의 ID로 변경해야 함
        execution_date="{{ execution_date }}"
    )
        
    section_make_csv >> upload_to_s3_task >> section_s3_to_snowflake >> trigger_comment_data_dag