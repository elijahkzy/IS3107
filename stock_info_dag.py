from datetime import datetime, timedelta 
import yfinance as yf
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import os
import json

from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import csv
import logging

from airflow import DAG

# Change to your key
key = '/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/test-proj-378801-e260b3ef768e.json'
# key = '/mnt/c/Users/Estella Lee Jie Yi/OneDrive - National University of Singapore/Desktop/NUS/Y3S2/IS3107/project/key.json'

def financials_extract(ti):
    '''
    Extract stock information in STI Components for past five days
    Gets the stock data using Yahoo Finance API in Pandas Dataframe and push as JSON

    Input: None
    Output: None
    '''
    #STI Index
    tickers = ['YF8.SI','BS6.SI', 'J36.SI', 'O39.SI', 'D05.SI', 'U11.SI', 'C09.SI', '9CI.SI', 'C07.SI', 'A17U.SI', 'Z74.SI', 'ME8U.SI', 'AJBU.SI',	
               'C38U.SI', 'M44U.SI', 'U14.SI', 'V03.SI', 'D01.SI', 'H78.SI', 'BN4.SI', 'BUOU.SI', 'Y92.SI', 'C52.SI', 'S63.SI',	'G13.SI', 'EMI.SI',
               'N2IU.SI', 'F34.SI', 'U96.SI', 'S58.SI']
    i = 0
    df = pd.DataFrame()

    for ticker in tickers:
        prices = yf.download(ticker, period = '5d').iloc[: , :6].dropna(axis=0, how='any')
        prices = prices.loc[~prices.index.duplicated(keep='last')]
        prices = prices.reset_index()

        prices.insert(loc = 1, column = 'Ticker', value = ticker)
        prices = prices.rename({'Adj Close': 'Adj_Close'}, axis=1)
        df = pd.concat([df, prices],ignore_index = True)
        i += 1

    print(df)
    stock_info_daily = df.to_json(orient='records')
    ti.xcom_push(key='stock_info_daily', value=stock_info_daily)

def financials_stage(ti):
    '''
    Push the raw stockdata into google bigquery dataset yfinance_data_raw
    '''
    stock_info_daily = ti.xcom_pull(key='stock_info_daily', task_ids=['financials_extract'])[0]
    df = pd.DataFrame(eval(stock_info_daily))
    df['Date'] = pd.to_datetime(df['Date'], unit='ms') # Convert date from json to date format

    openfile = open(key)
    jsondata = json.load(openfile)
    openfile.close()

    # Construct a BigQuery client object.
    credentials_path = key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client()
    
    project_id = jsondata['project_id']
    # staging_table_id = project_id + ".yfinance_data_raw.stock_info"
    # job = client.load_table_from_dataframe(df, staging_table_id)
    # job.result()
    staging_table_id = "yfinance_data_raw.stock_info"
    pandas_gbq.to_gbq(df, destination_table=staging_table_id, 
                      project_id=project_id,
                      if_exists='append')

def financials_transform():
    '''
    Load Stock Data from Staging Table to Main Table
    Derive Moving-Average(5) and Signal columns using SQL, and pass Close, Moving-Average(5) and Signal to Main Tables
    '''
    #Get Project ID
    openfile = open(key)
    jsondata = json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".yfinance_data_raw.stock_info"
    actual_table_id = project_id + ".yfinance_data.stock_info"
    
    #Connect To Bigquery
    credentials_path = key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client()

    #Load Data from Staging table to Main table
    query = f"""
        INSERT INTO `{actual_table_id}`
        SELECT *
        FROM (SELECT *, AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS MA_5day,
        CASE
        WHEN ((Close - AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) > 0.1) or (AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - Close) > 0.1 THEN 'Neutral'
        WHEN Close > AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) THEN 'Buy'
        WHEN Close < AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) THEN 'Sell'
        else 'Neutral'
        END AS Signal,
        FROM
        `{staging_table_id}` as p
        ) T
        where CAST(DATE as Date) = CURRENT_DATE();

        TRUNCATE TABLE `{staging_table_id}`;
    """
    query_job = client.query(query)
    print('Successfully loaded stock prices')

default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'email': ['fxing@example.com'],
     'email_on_failure': False,
     'email_on_retry': False,
     'retries': 1
    }

with DAG(
    'stocks_info_dag',
    default_args=default_args,
    description='Collect Stock Info For Analysis',
    catchup=False, 
    start_date=datetime(2020, 12, 23), 
    schedule_interval='@daily' #None # to change to 0 0 * * * (daily)
) as dag:
    
    financialsExtract = PythonOperator(
        task_id='financials_extract',
        provide_context=True,
        python_callable=financials_extract
    )

    financialsStage = PythonOperator(
        task_id='financials_stage',
        provide_context=True,
        python_callable=financials_stage
    )

    financialsTransform = PythonOperator(
        task_id='financials_transform',
        provide_context=True,
        python_callable=financials_transform
    )

financialsExtract >> financialsStage >> financialsTransform