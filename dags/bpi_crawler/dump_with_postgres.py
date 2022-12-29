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
exchange_api_key = Variable.get("EXCHANGE_API_KEY")
postgres_connection = Variable.get("POSTGRES")


myclient = pymongo.MongoClient(mongodb_connection)
mydb = myclient["warehouse"]


def crawling(ti):
    # Crawl From API
    res = requests.get(api_endpoint)
    payload = res.json()

    if res.status_code is not 200:
        raise ValueError('Failed to fetch data form Internet')
    else:
        payload = res.json()
        print(payload)

        # Load to staging-db
        collection = mydb['payload_api_batch']
        db_payload = collection.insert_one(payload)
        db_payload_id = str(db_payload.inserted_id)

        print(db_payload_id,type(db_payload_id))
        ti.xcom_push(key="bpi_crawler_payload_id",value=db_payload_id)

def gete_idr_price(ti):
    # Load from staging-db
    api_payload_collection = mydb['payload_api_batch']
    db_payload_id = ti.xcom_pull(key="bpi_crawler_payload_id")
    db_payload = api_payload_collection.find_one({"_id":ObjectId(db_payload_id)})

    amount = str(db_payload['bpi']['USD']['rate']).replace(",","")
    print("Amount",amount)
    key = exchange_api_key
    print("APIKEY",exchange_api_key)
    url = 'https://api.apilayer.com/exchangerates_data/convert'
    params = {
        "from":"USD",
        "to": "IDR",
        "amount": amount,
        "date": datetime.strftime(datetime.now(),'%Y-%m-%d')
    }
    header = {
        "apikey":key
    }
    res = requests.get(url=url,params=params,headers=header)
    
    if res.status_code is not 200:
        raise ValueError('Failed to fetch data form Internet')
    else:
        payload = res.json()
        print("IDR",payload['result'])
        mydb['payload_api']
        api_payload_collection = mydb['payload_api_batch']
        api_payload_collection.update_one({"_id":ObjectId(db_payload_id)},{"$set":{"bpi_idr_rate_float":payload['result']}})

def data_enrichment(ti):
    # Load to staging-db
    api_payload_collection = mydb['payload_api_batch']
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
    dataset['bpi_idr_rate_float'] = float(db_payload['bpi_idr_rate_float'])
    dataset['time_updated'] = datetime.strptime(db_payload['time']['updated'],"%b %d, %Y %H:%M:%S %Z") 
    dataset['time_updated'] = datetime.strftime(dataset['time_updated'],"%Y-%m-%d %H:%M:%S")
    dataset['time_updated_iso'] = datetime.strptime(db_payload['time']['updatedISO'],"%Y-%m-%dT%H:%M:%S%z")
    dataset['time_updated_iso'] = datetime.strftime(dataset['time_updated_iso'],"%Y-%m-%d %H:%M:%S")
    dataset['last_updated'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    
    # Load to staging-db
    collection = mydb['payload_enriched_batch']
    db_payload = collection.insert_one(dataset)
    db_payload_id = str(db_payload.inserted_id)

    ti.xcom_push(key="bpi_enriched_payload_id",value=db_payload_id)

def test(ti):
    # Load to staging-db
    payload_enriched_collection = mydb['payload_enriched_batch']
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
    assert is_float_dtype(df['bpi_idr_rate_float']) == True

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
    payload_enriched_collection = mydb['payload_enriched_batch']
    db_payload = payload_enriched_collection.find({})

    print(db_payload)

    db = create_engine(postgres_connection)
    conn = db.connect()

    df = pd.DataFrame.from_records(db_payload)
    df['_id'] = df['_id'].apply(lambda id: str(id))
    print(df.columns)

    df = df[['_id','disclaimer', 'chart_name', 'bpi_usd_code', 'bpi_usd_rate_float', 'bpi_usd_description', 'bpi_gdp_code', 'bpi_gdp_rate_float', 'bpi_gdp_description', 'bpi_eur_code', 'bpi_eur_rate_float', 'bpi_eur_description', "bpi_idr_rate_float", 'time_updated', 'time_updated_iso', 'last_updated']]
    print(df.head(5))
    df.to_sql('data_batched', con=conn, index=False,if_exists="append")

@provide_session
def cleanup(session=None, **context):
    dag = context["dag"]
    dag_id = dag._dag_id

    # It will delete all xcom of the dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

def cleandb(ti):
    # Load to staging-db
    payload_enriched_collection = mydb['payload_enriched_batch']
    payload_enriched_collection.drop()




with DAG(dag_id="bpi_dump_crawler_advanced", start_date=datetime(2021,1,1), 
    schedule_interval="*/10 * * * *", catchup=False) as dag:

    crawl = PythonOperator(
        task_id="retrieve_bpi_value",
        python_callable=crawling)

    retrieve_idr_value = PythonOperator(
        task_id="retrieve_idr_value",
        python_callable=gete_idr_price
    )

    enrichment = PythonOperator(
        task_id="Enrichment",
        python_callable=data_enrichment)

    quality_checking = PythonOperator(
        task_id="Validation",
        python_callable=test)

    crawl >> retrieve_idr_value >> enrichment >> quality_checking  



with DAG(dag_id="bpi_batch_store_crawler_advanced", start_date=datetime(2021,1,1), 
    schedule_interval="@hourly", catchup=False) as dag:
    
    load_data = PythonOperator(
        task_id="Load",
        python_callable=load_to_postgres)

    clean_db = PythonOperator(
        task_id="clean_db",
        python_callable=cleandb)

    clean_xcom = PythonOperator(
        task_id="clean_xcom_session",
        python_callable = cleanup,
        provide_context=True, 
        # dag=dag
    )

    load_data >> clean_db >> clean_xcom  
