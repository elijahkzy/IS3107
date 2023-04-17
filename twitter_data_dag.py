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

# Change to your key
key = '/mnt/c/Users/hsinz/Desktop/nus/Y3S2/IS3107/Proj/privateKey.json'

def tweetdata_extract(ti):
    tickers=[
    ('d05.SI', 'dbs'),
    ('SE', 'garena'),
    ('SE', 'shopee'),
    ('039.SI', 'ocbc'),
    ('U11.SI', 'uob'),
    ('Z74.SI', 'singtel'),
    ('F34.SI', 'wilmar'),
    ('C6L.SI', 'singapore airlines'),
    ('GRAB', 'grab'),
    ('G13.SI', 'genting'),
    ('C38U.SI', 'capitaland'),
    ('G07.SI', 'great eastern'),
    ('C07.SI', 'jardine'),
    ('A17U.SI', 'ascendas'),
    ('S63.SI', 'st engineering'),
    ('BN4.SI', 'keppel')
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
    tickerCode = []
   
    for ticker in tickers:
        keyword = f'"{ticker[1]}"'
        print(keyword)

        tweets = client.search_recent_tweets(query=keyword, max_results=50, 
            tweet_fields = ['author_id','created_at','text','source','lang','geo'],
            user_fields = ['name','username','location','verified'],
            expansions = ['geo.place_id', 'author_id'],
            place_fields = ['country','country_code'])
        try:
            for tweet in tweets['data']:
                if tweet['lang'] == 'en':
                    tickerName.append(keyword)
                    texts.append(tweet['text'])
                    created_dates.append(tweet['created_at'])
                    author_id.append(tweet['author_id'])
                    tickerCode.append(ticker[0])
        except:
            tickerName.append('')
            texts.append('')
            created_dates.append('')
            author_id.append('')
            tickerCode.append(ticker[0])
        
    df = pd.DataFrame({'tickers': tickerName,
                       'ticker_code': tickerCode,
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
    openfile=open(key)
    jsondata=json.load(openfile)
    openfile.close()
    df = pd.read_json(io.StringIO(twitter_data), encoding='utf-8')

    df['texts'] = df['texts'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))
    df['tickers'] = df['tickers'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))
    df['dates'] = df['dates'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))
    df['ticker_code'] = df['ticker_code'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))

    print(df)

    # Construct a BigQuery client object.
    credentials_path = key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".twitter_data_raw.stock_info"
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

with DAG(
    'tweets_data_dag',
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