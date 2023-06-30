FROM apache/airflow:latest
RUN pip install --user --upgrade pip
COPY requirements.txt ./
RUN pip install -r /requirements.txt
