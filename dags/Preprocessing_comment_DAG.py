from datetime import datetime, timedelta
import datetime
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.models import XCom
from airflow.hooks.base import BaseHook
from requests.exceptions import JSONDecodeError
import logging
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pandas as pd

import csv
import requests
import time
import os
from plugins import slack

default_args = {
    'owner': 'Jeesok',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack.on_failure_callback
}

with DAG(
        dag_id='comment_data',
        start_date=datetime.datetime(2023, 6, 26),
        schedule_interval="2 0 * * *",
        max_active_runs=1,
        default_args=default_args,
        catchup=False
) as dag:
    
    bucket_name = "steambucket4-2"
    folder_name = "comment_info"
    file_name = "comment_info"
    schema = "RAW_DATA"
    table="COMMENT_DATA"
    aws_conn = BaseHook.get_connection("S3_conn")
    
    def get_game_info(**context):
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_raw_data')
        # SQL 문 실행 및 결과 가져오기
        sql = f'''
        SELECT GAME_ID
        FROM GAME_INFO
        '''
        result = snowflake_hook.get_pandas_df(sql).to_dict(orient='records')
        # 결과를 딕셔너리 변수에 할당
        game_info = result
        logging.info(f"game_info {len(game_info)} data finished")
        print(game_info)
        return game_info
    
    def get_game_comment_info(game_info):
        game_list = []
        for appid in game_info:
            try:
                url_kor = f"https://store.steampowered.com/appreviews/{str(appid['GAME_ID'])}?json=1&num_per_page=300&l=korean"
                response_kor = requests.get(url_kor)
                data_kor = response_kor.json()
                time.sleep(1.1)
                
                url_eng = f"https://store.steampowered.com/appreviews/{str(appid['GAME_ID'])}?json=1&num_per_page=100"
                response_eng = requests.get(url_eng)
                data_eng = response_eng.json()
                time.sleep(1.1)
                
                languages = ['KOR','ENG']
                for n, review_data in enumerate([data_kor['reviews'],data_eng['reviews']]):
                    language = languages[n]
                    for data in review_data:
                        app_dic = {}
                        app_dic['COMMENT_ID'] = data['recommendationid']
                        app_dic['GAME_ID'] =  str(appid['GAME_ID'])
                        app_dic['USER_ID'] = data['author']['steamid']
                        app_dic['TOTAL_PLAYTIME'] = data['author']['playtime_forever']
                        app_dic['LAST_TWO_WEEKS_PLAYTIME'] = data['author']['playtime_last_two_weeks']
                        app_dic['COMMENT_CONTEXT'] = data['review'].replace('\n'," ").replace(',', " ")
                        app_dic['LANGUAGE'] = language
                        game_list.append(app_dic)
                
            except JSONDecodeError as e:
                logging.info("JSON 디코딩 오류 발생: ", e)
                logging.info(appid['GAME_ID'])
                time.sleep(1.1)  # 1.5초간 대기
            logging.info(f"{appid} 적재 완료")
            
        return game_list

    def make_csv(**context):
        csv_filename = "data/comment_info.csv"
        data = get_game_comment_info(get_game_info())
        
        fieldnames = ['COMMENT_ID', 'GAME_ID', 'USER_ID', 'TOTAL_PLAYTIME', 'LAST_TWO_WEEKS_PLAYTIME', 'COMMENT_CONTEXT', 'LANGUAGE']
        
        df = pd.DataFrame.from_records(data, columns=fieldnames)
        df.to_csv(csv_filename, index=False)

        return csv_filename
    
    def upload_csv_to_s3(**context):
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
        csv_filename = context['csv_file_path']
        save_file_name = f"{file_name}_{current_date}.csv"
        
        logging.info(csv_filename)
        s3_hook = S3Hook(aws_conn_id='S3_conn')
        logging.info("connection finished")
        logging.info(csv_filename)
        

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
            os.remove(context['csv_file_path'])
            
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
                            $1 AS COMMENT_ID
                            , $2 AS GAME_ID
                            , $3 AS USER_ID
                            , $4 AS TOTAL_PLAYTIME
                            , $5 AS LAST_TWO_WEEKS_PLAYTIME
                            , $6 AS COMMENT_CONTENT
                            , $7 AS LANGUAGE
                        FROM '@s3_stage/{s3_key}';
            
            DELETE FROM {table};
            
            INSERT INTO {table}
            SELECT tmp.*
              FROM temp_table tmp;
            
            COMMIT;
        '''
        logging.info(query)
        snowflake_hook.run(query)
        logging.info("run finished snowflake")

    make_csv_task = PythonOperator(
        task_id='make_csv_task',
        python_callable=make_csv,
        provide_context=True,
        dag=dag
    )

    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3_task',
        python_callable=upload_csv_to_s3,
        provide_context=True,
        op_kwargs={
            'csv_file_path': '{{ task_instance.xcom_pull(task_ids="make_csv_task") }}'
        },
        dag=dag
    )
    
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
    
    make_csv_task >> upload_to_s3_task  >> check_csv_file_exists_task >> s3_to_snowflake_task
