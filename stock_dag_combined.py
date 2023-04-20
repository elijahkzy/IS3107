from datetime import datetime, timedelta 
import yfinance as yf
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import os
import json
from sqlalchemy import create_engine

from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import csv
import logging
import smtplib
from airflow import DAG

# Change to your key
key = '/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/key.json'

# local postgres db credentials 
username = "postgres"
password = "postgres"
host = "localhost"
port = "5432"
database = "is3107"

# create database engine
engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")


def financials_extract(ti):
    '''
    Extract stock information in STI Components for past five days
    Gets the stock data using Yahoo Finance API in Pandas Dataframe and push as JSON
    Input: None
    Output: None
    '''
    #STI Index
    tickers =  ['D05.SI', 'SE', 'O39.SI', 'U11.SI' ,'Z74.SI', 'F34.SI', 'C6L.SI', 'GRAB', 'G13.SI', 'C38U.SI','G07.SI', 'C07.SI', 'A17U.SI', 'S63.SI', 'BN4.SI']
    i = 0
    df = pd.DataFrame()

    for ticker in tickers:
        prices = yf.download(ticker, period='1d').iloc[: , :6].dropna(axis=0, how='any')
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
    staging_table_id = "yfinance_data.stock_raw"
    pandas_gbq.to_gbq(df, destination_table=staging_table_id, 
                      project_id=project_id,
                      if_exists='append')
    df.to_sql("raw_stock_data", con=engine, if_exists="append", index=False)


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
    staging_table_id = project_id + ".yfinance_data.stock_raw"
    actual_table_id = project_id + ".yfinance_data.stock_info"
    
    #Connect To Bigquery
    credentials_path = key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client()

    #Load Data from Staging table to Main table
    query =  f"""
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
    
    if query_job.state == 'DONE':
        if query_job.error_result:
            print(f"Query job failed with error: {query_job.error_result['message']}")
        else:
            print("Query job succeeded!")
    else:
            print("Query job is still running.")

    query_local = """CREATE TABLE IF NOT EXISTS processed_stock_data AS
        SELECT *
        FROM raw_stock_data 
        WHERE 1 = 0;

        alter table processed_stock_data
        add column if not exists"ma_5day" FLOAT ,
        add column if not exists "signal" Varchar(10);

        INSERT INTO processed_stock_data  
        SELECT *,
        AVG(p."Close") OVER (PARTITION BY p."Ticker" ORDER BY p."Date"  ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS MA_5day,
        CASE
        WHEN ((p."Close" - AVG(p."Close") OVER (PARTITION BY p."Ticker" ORDER BY p."Date" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) > 0.1) or (AVG(p."Close") OVER (PARTITION BY p."Ticker" ORDER BY p."Date" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - p."Close") > 0.1 THEN 'Neutral'
        WHEN p."Close" > AVG(p."Close") OVER (PARTITION BY p."Ticker" ORDER BY p."Date" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) THEN 'Buy'
        WHEN p."Close" < AVG(p."Close") OVER (PARTITION BY p."Ticker" ORDER BY p."Date" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) THEN 'Sell'
        else 'Neutral'
        END AS Signal
        FROM raw_stock_data as p
        WHERE CAST(p."Date" as Date) = CURRENT_DATE;

        INSERT INTO processed_stock_data SELECT * FROM  raw_stock_data;
        TRUNCATE raw_stock_data
        """

    with engine.connect() as connection:
        connection.execute(query_local)

def financials_load():
    '''
    Load Stock Data to combined_data together with twitter data
    '''
    #Get Project ID
    openfile = open(key)
    jsondata = json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']
    finance_table_id = project_id + ".yfinance_data.stock_info"
    twitter_table_id = project_id + ".twitter_data.stock_aggregated"
    load_table_id = project_id + ".combined_data.stock_info"
    
    #Connect To Bigquery
    credentials_path = key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client()

    #Joining finance table and twitter scores table per day
    query = f"""
        INSERT INTO `{load_table_id}`
        SELECT 
            A.Date,
            A.Ticker,
            A.Open,
            A.High,
            A.Low,
            A.Close,
            A.Adj_Close,
            A.Volume,
            A.MA_5days,
            A.Signal,
            B.WeightedCompoundScore
        FROM
            `{finance_table_id}` as A
    
        JOIN `{twitter_table_id}` as B ON A.Ticker = B.Ticker AND A.Date = B.Date
    """

    query_job = client.query(query)
    if query_job.state == 'DONE':
        if query_job.error_result:
            print(f"Query job failed with error: {query_job.error_result['message']}")
        else:
            print("Query job succeeded!")
    else:
            print("Query job is still running.")

def send_email():
    EMAIL = 'is3107group9@gmail.com'
    PASSWORD = 'is3107group9!!'
    '''
    Sends an email to the recipient if the daily pipeline has been executed successfully. 

    Input: email and password 
    Output: None
    '''
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.starttls()
    server.login(EMAIL,PASSWORD)
    server.sendmail(EMAIL,EMAIL,'The daily pipeline has been executed successfully.')


default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'email': ['fxing@example.com'],
     'email_on_failure': False,
     'email_on_retry': False,
     'retries': 1
    }

with DAG(
    'stocks_info_dag_combined',
    default_args=default_args,
    description='Collect Stock Info For Analysis',
    catchup=False, 
    start_date=datetime(2023, 4, 20), 
    schedule_interval=timedelta(days=1)
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

    financialsLoad = PythonOperator(
        task_id='financials_load',
        provide_context=True,
        python_callable=financials_load
    )

    sendEmail = PythonOperator(
        task_id='sendEmail',
        python_callable=send_email,
        dag=dag,  
    )

financialsExtract >> financialsStage >> financialsTransform >> financialsLoad >> sendEmail
