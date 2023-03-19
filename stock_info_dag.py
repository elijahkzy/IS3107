import datetime
from google.cloud import bigquery 
import pandas as pd
import yfinance as yf
import os
import json

from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import csv
import logging

from airflow import DAG


from datetime import datetime, timedelta  
import datetime as dt
import numpy as np
import pandas as pd



def stockdata_extract(ti):
    '''
    Extract stock information in STI Components for past five days
    Gets the stock data using Yahoo Finance API in Pandas Dataframe and push as JSON

    Input: None
    Output: None
    '''
    #STI Index
    tickers = ['C6L.SI','D05.SI','C52.SI','G13.SI','C38U.SI',
            'U11.SI','Y92.SI','N2IU.SI','S58.SI','F34.SI',
            'M44U.SI','Z74.SI','C09.SI','BS6.SI','V03.SI']
    stock_dat = yf.download(tickers,period="2d",actions=False,group_by=tickers)
    i = 0
    df = pd.DataFrame()

    for ticker in tickers:
        prices = yf.download(ticker, period = '5d').iloc[: , :6].dropna(axis=0, how='any')
        prices = prices.loc[~prices.index.duplicated(keep='last')]
        prices = prices.reset_index()

        prices.insert(loc = 1, column = 'Ticker', value = ticker)
        prices = prices.rename({'Adj Close': 'Adj_Close'}, axis=1)
        df = pd.concat([df, prices],ignore_index = True)
        i+= 1

    stock_info_daily = df.to_json(orient='records')
    ti.xcom_push(key='stock_info_daily', value=stock_info_daily)

def stockdata_upload(ti):
    '''push the raw stockdata into google bigquery dataset yfinance_data_raw
    '''
    stock_info_daily = ti.xcom_pull(key='stock_info_daily', task_ids=['get_stock_info'])[0]
    df = pd.DataFrame(eval(stock_info_daily))

    openfile=open('/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/test-proj-378801-e260b3ef768e.json')
    jsondata=json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']

    # Construct a BigQuery client object.
    credentials_path = '/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/test-proj-378801-e260b3ef768e.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".yfinance_data_raw.stock_info"
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

with DAG( 'stocks_info_dag',
            default_args=default_args,
            description='Collect Stock Info For Analysis',
            catchup=False, 
            start_date= datetime(2020, 12, 23), 
            schedule_interval= None # to change to 0 0 * * * (daily)
  
          )  as dag:
    
    get_daily_stock_info = PythonOperator(
        task_id='get_stock_info',
        provide_context=True,
        python_callable=stockdata_extract
    )

    push_daily_stock_info = PythonOperator(
        task_id='push_stock_info',
        provide_context=True,
        python_callable=stockdata_upload
    )

get_daily_stock_info >> push_daily_stock_info
