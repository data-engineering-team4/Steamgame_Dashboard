import datetime
from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Test',
}

with DAG(
        dag_id='test_Dag',
        start_date=datetime.datetime(2023, 7, 1),
        max_active_runs=1,
        default_args=default_args,
        catchup=False
) as dag:
    
    def make_csv(**context):
        csv_filename = "data/teset.csv"
        
        list = [[1, 5, 7], [5,6,8], [8,0,11]]

        df = pd.DataFrame(list, columns=['first', 'second','third'])

        df.to_csv('temp.csv', index=False)

        data = [
            ["999999","test","9999","0","2023-07","test","test","0","0"]
        ]

        columns = ['game_id', 'game_name', 'price', 'discount_percentage', 'release_date', 'thumbnail_path', 'genre', 'negative_cnt', 'positive_cnt']
        
        df = pd.DataFrame.from_records(data, columns=columns)  # 데이터프레임 생성
        df.to_csv(csv_filename, index=False)  # CSV 파일로 저장

        return csv_filename
    
    test_make_csv_task = PythonOperator(
        task_id='test_make_csv_task',
        python_callable=make_csv,
        provide_context=True,
        dag=dag
    )

test_make_csv_task