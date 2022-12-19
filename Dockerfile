FROM apache/airflow:2.5.0-python3.10
COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt