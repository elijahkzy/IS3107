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

from stock_data_methods import financials_extract, financials_stage

from datetime import datetime, timedelta  
import datetime as dt
import numpy as np
import pandas as pd


default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'email': ['fxing@example.com'],
     'email_on_failure': False,
     'email_on_retry': False,
     'retries': 1
    }

with DAG('stocks_info_dag',
            default_args=default_args,
            description='Collect Stock Info For Analysis',
            catchup=False, 
            start_date= datetime(2020, 12, 23), 
            schedule_interval= None # to change to 0 0 * * * (daily)
  
          )  as dag:
    
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

financialsExtract >> financialsStage
