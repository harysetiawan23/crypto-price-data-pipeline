from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
from bson import ObjectId
from datetime import datetime
import great_expectations as ge
import pandas as pd
import numpy as np
import pymongo
import requests
from sqlalchemy import create_engine
from airflow.utils.db import provide_session
from airflow.models import XCom
from pandas.api.types import is_float_dtype,is_string_dtype

api_endpoint = "https://api.coindesk.com/v1/bpi/currentprice.json"

mongodb_connection = Variable.get("MONGO")
postgres_connection = Variable.get("POSTGRES")


myclient = pymongo.MongoClient(mongodb_connection)
mydb = myclient["warehouse"]



@provide_session
def cleanup(session=None, **context):
    dag = context["dag"]
    dag_id = dag._dag_id

    # It will delete all xcom of the dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()
    
def cleandb(ti):
    # Load to staging-db
    payload_enriched_collection = mydb['payload_enriched']
    db_payload_id = ti.xcom_pull(key="bpi_enriched_payload_id")
    payload_enriched_collection.delete_one({"_id":ObjectId(db_payload_id)})


def crawling(ti):
    # Crawl From API
    res = requests.get(api_endpoint)
    payload = res.json()

    print(payload)

    # Load to staging-db
    collection = mydb['payload_api']
    db_payload = collection.insert_one(payload)
    db_payload_id = str(db_payload.inserted_id)

    print(db_payload_id,type(db_payload_id))
    ti.xcom_push(key="bpi_crawler_payload_id",value=db_payload_id)

def data_enrichment(ti):
    # Load to staging-db
    api_payload_collection = mydb['payload_api']
    db_payload_id = ti.xcom_pull(key="bpi_crawler_payload_id")
    db_payload = api_payload_collection.find_one({"_id":ObjectId(db_payload_id)})

    print(db_payload)

    dataset = {}
    dataset['disclaimer'] = db_payload['disclaimer']
    dataset['chart_name'] = db_payload['chartName']
    dataset['bpi_usd_code'] = db_payload['bpi']['USD']['code']
    dataset['bpi_usd_rate_float'] = float(db_payload['bpi']['USD']['rate'].replace(",",""))
    dataset['bpi_usd_description'] = db_payload['bpi']['USD']['description']
    dataset['bpi_gdp_code'] = db_payload['bpi']['GBP']['code']
    dataset['bpi_gdp_rate_float'] = float(db_payload['bpi']['GBP']['rate'].replace(",",""))
    dataset['bpi_gdp_description'] = db_payload['bpi']['GBP']['description']
    dataset['bpi_eur_code'] = db_payload['bpi']['EUR']['code']
    dataset['bpi_eur_rate_float'] = float(db_payload['bpi']['EUR']['rate'].replace(",",""))
    dataset['bpi_eur_description'] = db_payload['bpi']['EUR']['description']
    dataset['bpi_idr_rate_float'] = dataset['bpi_usd_rate_float']*15000
    dataset['time_updated'] = datetime.strptime(db_payload['time']['updated'],"%b %d, %Y %H:%M:%S %Z") 
    dataset['time_updated'] = datetime.strftime(dataset['time_updated'],"%Y-%m-%d %H:%M:%S")
    dataset['time_updated_iso'] = datetime.strptime(db_payload['time']['updatedISO'],"%Y-%m-%dT%H:%M:%S%z")
    dataset['time_updated_iso'] = datetime.strftime(dataset['time_updated_iso'],"%Y-%m-%d %H:%M:%S")
    dataset['last_updated'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    
    # Load to staging-db
    collection = mydb['payload_enriched']
    db_payload = collection.insert_one(dataset)
    db_payload_id = str(db_payload.inserted_id)

    ti.xcom_push(key="bpi_enriched_payload_id",value=db_payload_id)

def test(ti):
    # Load to staging-db
    payload_enriched_collection = mydb['payload_enriched']
    db_payload_id = ti.xcom_pull(key="bpi_enriched_payload_id")
    db_payload = payload_enriched_collection.find_one({"_id":ObjectId(db_payload_id)})



    df = pd.DataFrame([db_payload])
    print(df.info())
    ge_df = ge.from_pandas(df)

    # time_updated_datetime = ge_df.expect_column_values_to_be_of_type("time_updated",np.datetime64)
    time_updated_iso = ge_df.expect_column_values_to_match_strftime_format("time_updated_iso","%Y-%m-%d %H:%M:%S")
    time_updated = ge_df.expect_column_values_to_match_strftime_format("time_updated","%Y-%m-%d %H:%M:%S")
    last_updated = ge_df.expect_column_values_to_match_strftime_format("last_updated","%Y-%m-%d %H:%M:%S")
    
    assert time_updated_iso['success'] == True
    assert time_updated['success'] == True
    assert last_updated['success'] == True
    assert is_float_dtype(df['bpi_usd_rate_float']) == True
    assert is_float_dtype(df['bpi_gdp_rate_float']) == True
    assert is_float_dtype(df['bpi_eur_rate_float']) == True

    assert is_string_dtype(df['disclaimer']) == True
    assert is_string_dtype(df['chart_name']) == True
    assert is_string_dtype(df['bpi_usd_code']) == True
    assert is_string_dtype(df['bpi_usd_description']) == True
    assert is_string_dtype(df['bpi_gdp_code']) == True
    assert is_string_dtype(df['bpi_gdp_description']) == True
    assert is_string_dtype(df['bpi_eur_code']) == True
    assert is_string_dtype(df['bpi_eur_description']) == True

def load_to_postgres(ti):
    # Load to staging-db
    payload_enriched_collection = mydb['payload_enriched']
    db_payload_id = ti.xcom_pull(key="bpi_enriched_payload_id")
    db_payload = payload_enriched_collection.find_one({"_id":ObjectId(db_payload_id)})

    db = create_engine(postgres_connection)
    conn = db.connect()

    df = pd.DataFrame([db_payload])
    df['job_id'] = str(db_payload_id)
    df = df[['job_id', 'disclaimer', 'chart_name', 'bpi_usd_code', 'bpi_usd_rate_float', 'bpi_usd_description', 'bpi_gdp_code', 'bpi_gdp_rate_float', 'bpi_gdp_description', 'bpi_eur_code', 'bpi_eur_rate_float', 'bpi_eur_description', "bpi_idr_rate_float", 'time_updated', 'time_updated_iso', 'last_updated']]
    df.to_sql('data', con=conn, index=False,if_exists="append")

with DAG(dag_id="bpi_crawler_advanced", start_date=datetime(2021,1,1), 
    schedule_interval="@hourly", catchup=False) as dag:

    crawl = PythonOperator(
        task_id="extract",
        python_callable=crawling)

    enrichment = PythonOperator(
        task_id="transform",
        python_callable=data_enrichment)

    quality_checking = PythonOperator(
        task_id="test",
        python_callable=test)
    
    load_data = PythonOperator(
        task_id="load",
        python_callable=load_to_postgres)

    clean_db = PythonOperator(
        task_id="clean_db",
        python_callable=cleandb)

    clean_xcom = PythonOperator(
        task_id="clean_session",
        python_callable = cleanup,
        provide_context=True, 
        # dag=dag
    )


    crawl >> enrichment >> quality_checking >> load_data >> [clean_db,clean_xcom] 


