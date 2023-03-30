import datetime
from google.cloud import bigquery 
import pandas as pd
import yfinance as yf
import os
import json
from twitter_data_methods import tweetdata_extract, tweetdata_transform

from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import csv
import logging
import io

from airflow import DAG
import tweepy
from datetime import datetime, timedelta  

from tweepy import OAuthHandler
from timeit import default_timer as timer

default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'email_on_failure': False,
     'email_on_retry': False,
     'retries': 1
    }

with DAG('tweets_data_dag',
            default_args=default_args,
            description='Collect Twitter Info For Analysis',
            catchup=False, 
            start_date= datetime(2020, 12, 23), 
            schedule_interval=timedelta(days=1)
  
          ) as dag:
    
    twitterExtract = PythonOperator(
        task_id='tweetdata_extract',
        provide_context=True,
        python_callable=tweetdata_extract
    )

    twitterTransform = PythonOperator(
        task_id='tweetdata_transform',
        provide_context=True,
        python_callable=tweetdata_transform
    )

twitterExtract >> twitterTransform