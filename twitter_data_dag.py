import datetime
from google.cloud import bigquery 
import pandas as pd
import yfinance as yf
import os
import json

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

import pandas as pd

def tweetdata_extract(ti):
    tickers=[
    'singapore airlines',
    'dbs',
    'comfortdelgro',
    'genting',
    'capitaland',
    'uob',
    'mapletree logistics',
    'mapletree commercial',
    'sats',
    'wilmar',
    'singtel',
    'city developments',
    'yangzijiang shipbuilding',
    'thai beverage public company',
    'venture corporation',
    'sembcorp',
    'ascendas',
    'frasers',
    'hongkong land holdings',
    'st engineering'
    ]
    
    consumer_key = 'omhSwvUaRXdSh4fkq1Q4VhU36'
    consumer_secret = 'a4uNlAvLxQ1AE48sP9bdLTDKP8IKxONdLZXff7Ue6foSliY3s5'
    access_token = '1278982307303419908-RewTtBb6Uj2ySXfAr6G5o8NjDb8IRm'
    access_secret = '1oY9ZCHHXWykuyctbtGKPposnfpZKIJpLQxUSC3Xylhwa'

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    client = tweepy.Client(
        bearer_token='AAAAAAAAAAAAAAAAAAAAAGzylgEAAAAA5UESpwQdxCu2wO5e8x8A0looTk0%3DCIIzaw5kytSTDaW6zWNvs7b1MsshuYE9gyA06vy0dkNwqk07gB',
        return_type=dict)

    tickerName = []
    author_id = []
    texts = []
    created_dates = []
   
    for ticker in tickers:
        ticker = f'"{ticker}"'
        print(ticker)

        tweets = client.search_recent_tweets(query=ticker, max_results=50, 
            tweet_fields = ['author_id','created_at','text','source','lang','geo'],
            user_fields = ['name','username','location','verified'],
            expansions = ['geo.place_id', 'author_id'],
            place_fields = ['country','country_code'])
        try:
            for tweet in tweets['data']:
                if tweet['lang'] == 'en':
                    tickerName.append(ticker)
                    texts.append(tweet['text'])
                    created_dates.append(tweet['created_at'])
                    author_id.append(tweet['author_id'])
        except:
            tickerName.append('')
            texts.append('')
            created_dates.append('')
            author_id.append('')
        
    df = pd.DataFrame({'tickers': tickerName,
                        #'username': usernames, 
                        'texts':texts, 
                        #'name': names, 
                        'dates': created_dates})
    twitter_data = df.to_json(orient='records')
    print(twitter_data)  
    ti.xcom_push(key='twitter_data', value=twitter_data)

def tweetdata_upload(ti):
    '''push the twitter data into bigquery 
    '''
    twitter_data = ti.xcom_pull(key='twitter_data', task_ids=['get_twitter_data'])[0]
    openfile=open('/mnt/c/Users/hsinz/Desktop/nus/Y3S2/IS3107/Proj/privateKey.json')
    jsondata=json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']
    df = pd.read_json(io.StringIO(twitter_data), encoding='utf-8')

    df['texts'] = df['texts'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))
    df['tickers'] = df['tickers'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))
    df['dates'] = df['dates'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))

    print(df)

    # Construct a BigQuery client object.
    credentials_path = '/mnt/c/Users/hsinz/Desktop/nus/Y3S2/IS3107/Proj/privateKey.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".twitter_data.stock_info"
    job_config = bigquery.LoadJobConfig(
        encoding='UTF-8',
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
    job.result()


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
    
    get_twitter_data = PythonOperator(
        task_id='get_twitter_data',
        provide_context=True,
        python_callable=tweetdata_extract
    )

    push_twitter_data = PythonOperator(
        task_id='push_twitter_data',
        provide_context=True,
        python_callable=tweetdata_upload
    )

get_twitter_data >> push_twitter_data