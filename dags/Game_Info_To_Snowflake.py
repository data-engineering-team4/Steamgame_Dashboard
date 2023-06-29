from datetime import datetime, timedelta
import datetime
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.models import XCom
from airflow.hooks.base import BaseHook
import logging
from plugins import slack


import steamspypi
import csv
import requests
import time
from requests.exceptions import JSONDecodeError
import os

default_args = {
    'owner': 'Song',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': slack.on_failure_callback
}

with DAG(
        dag_id='game_info',
        start_date=datetime.datetime(2023, 6, 26),
        schedule_interval="0 0 * * *",
        max_active_runs=1,
        default_args=default_args,
        catchup=False
) as dag:
    
    bucket_name = "steambucket4-2"
    folder_name = "game_info"
    file_name = "game_info"
    schema = "RAW_DATA"
    table="GAME_INFO"
    stage="CSV_STAGE"
    aws_conn = BaseHook.get_connection("S3_conn")
    
    def make_csv(**context):
        csv_filename = "data/game_info.csv"
        data = get_popular_game()
        file_exists = os.path.exists(csv_filename)
        mode = 'w' if not file_exists else 'w+'

        with open(csv_filename, mode, newline='') as csvfile:
            fieldnames = ['game_id', 'game_name', 'price', 'discount_percentage', 'release_date', 'thumbnail_path',
                          'genre', 'negative_cnt', 'positive_cnt']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for game in data:
                writer.writerow(game)

        return csv_filename

    def get_popular_game():
        data_request = dict()
        data_request['request'] = 'all'
        data_request['page'] = '0'

        data = steamspypi.download(data_request)
        game_list = []
        for app_id, app_info in data.items():
            game = {
                'game_id': app_info['appid'],
                'positive_cnt': app_info['positive'],
                'negative_cnt': app_info['negative']
            }
            game_list.append(game)

        get_game_detail_info(game_list)

        return game_list


    def get_game_detail_info(game_list):
        for i, game in enumerate(game_list):
            try:
                url = f"https://store.steampowered.com/api/appdetails?appids={str(game['game_id'])}&l=korean"
                response = requests.get(url)
                data = response.json()
                game_info = data[str(game['game_id'])]
                

                if game_info["success"] and game_info["data"]["type"] == "game":
                    game_data = game_info["data"]
                    if not game_data["release_date"]["coming_soon"]:
                        game["game_name"] = game_data["name"].replace(",", " ")
                        try:
                            if game_data["is_free"]:
                                game["price"] = 0
                                game["discount_percentage"] = 0
                            else:
                                game["price"] = int(
                                    game_data["price_overview"]["final_formatted"].split(" ")[1].replace(",", ""))
                                game["discount_percentage"] = int(game_data["price_overview"]["discount_percent"])
                        except KeyError:
                            game["price"] = -1
                            game["discount_percentage"] = 0

                        try:
                            if game_data["release_date"]["date"] != "":
                                date = datetime.datetime.strptime(game_data["release_date"]["date"], '%Y년 %m월 %d일')
                            else:
                                date = game_data["release_date"]["date"]
                        except ValueError:
                            date = datetime.datetime.strptime(game_data["release_date"]["date"], '%Y년 %m월')

                        if game_data["release_date"]["date"] != "":
                            game["release_date"] = date.strftime('%Y-%m')
                        else:
                            game["release_date"] = None

                        game["thumbnail_path"] = game_data["capsule_image"]
                        game["genre"] = ""
                        try:
                            for genre in game_data["genres"]:
                                if i != len(game_data["genres"]) - 1:
                                    game["genre"] += genre["description"].replace(" ", "") + " "
                                else:
                                    game["genre"] += genre["description"].replace(" ", "") 
                        except KeyError:
                            continue
                else:
                    game_list.pop(i)
                    logging.info(game['game_id'], "정보 없음")

                time.sleep(1.5)  # 1.5초간 대기

            except JSONDecodeError as e:
                print("JSON 디코딩 오류 발생: ", e)
                print(game['game_id'])
                time.sleep(1.5)  # 1.5초간 대기
                
    
    
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
    
   #s3keysensor로 파일 없으면 snowflake로 보내지 않기
   #temp에 테이블 데이터 다 저장해 두고 테이블 데이터 삭제 후 카피
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
                            , $2 AS game_name
                            , $3 AS price
                            , $4 AS discount_percentage
                            , $5 AS release_date
                            , $6 AS thumbnail_path
                            , $7 AS genre
                            , $8 AS negative_cnt
                            , $9 AS positive_cnt
                        FROM '@s3_stage/{s3_key}';
            
            DELETE FROM {table};
            
            INSERT INTO GAME_INFO
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