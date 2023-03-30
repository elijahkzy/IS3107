from datetime import datetime, timedelta 
from google.cloud import bigquery 
import pandas as pd
import numpy as np 
import yfinance as yf
import os
import json

from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import csv
import logging

from airflow import DAG

# Change to your key
key = '/mnt/c/Users/Estella Lee Jie Yi/OneDrive - National University of Singapore/Desktop/NUS/Y3S2/IS3107/project/key.json'

def get_stockprice_data(ti):
    '''
    Get Raw Stock Data
    '''
    credentials_path = key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client()

    openfile = open(key)
    jsondata = json.load(openfile)

    project_id = jsondata['project_id']
    staging_table_id = project_id + ".yfinance_data_raw.stock_info"

    table = client.get_table(staging_table_id)
    df = client.list_rows(table).to_dataframe()
    stockprice_data = df.to_json(orient='records')

    print(f'Successfully loaded stock price data with {len(df)} rows')
    ti.xcom_push(key = 'stockprice_data', value = stockprice_data)

def stockprice_staging(ti):
    '''
    Load Stock Data to Staging Table
    '''
    stockprice_data = ti.xcom_pull(key='stockprice_data', task_ids=['get_stockprice_data'])[0]
    df = pd.DataFrame(eval(stockprice_data))
    df['Date'] = df['Date'] = pd.to_datetime(df['Date'], unit='ms') # Convert date from json to date format

    #Get Project ID
    openfile = open(key)
    jsondata = json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".yfinance_data_raw.stock_info_staging"

    #Connect to BigQuery
    credentials_path = key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client()

    #Delete Existing Data in Table
    query = f"DELETE FROM `{staging_table_id}` WHERE True"
    query_job = client.query(query)

    #Load To staging
    job = client.load_table_from_dataframe(df, staging_table_id)
    job.result()

default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'email': ['fxing@example.com'],
     'email_on_failure': False,
     'email_on_retry': False,
     'retries': 1
    }

with DAG(
    'stocks_staging_dag',
    default_args=default_args,
    description='Load Stock Info into Staging Table For Analysis',
    catchup=False, 
    start_date=datetime(2020, 12, 23), 
    schedule_interval=None # to change to 0 0 * * * (daily)
) as dag:
    
    get_stockprice_data = PythonOperator(
        task_id='get_stockprice_data',
        provide_context=True,
        python_callable=get_stockprice_data
    )

    stockprice_staging = PythonOperator(
        task_id='stockprice_staging',
        provide_context=True,
        python_callable=stockprice_staging
    )

get_stockprice_data >> stockprice_staging