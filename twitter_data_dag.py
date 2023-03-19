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

   
    for ticker in tickers:
        ticker = f'"{ticker}"'
        print(ticker)
        tickers = []
        author_id = []
        texts = []
        created_dates = []
        #usernames = []
        #names = []
        tweets = client.search_recent_tweets(query=ticker, max_results=50, 
            tweet_fields = ['author_id','created_at','text','source','lang','geo'],
            user_fields = ['name','username','location','verified'],
            expansions = ['geo.place_id', 'author_id'],
            place_fields = ['country','country_code'])
        try:
            for tweet in tweets['data']:
                tickers.append(ticker)
                texts.append(tweet['text'])
                created_dates.append(tweet['created_at'])
                author_id.append(tweet['author_id'])
        except:
            tickers.append('')
            texts.append('')
            created_dates.append('')
            author_id.append('')
        # for id in author_id:
        #     user_data=client.get_user(id=id)
        #     username = user_data['data']['username']
        #     name = user_data['data']['name']
        #     usernames.append(username)
        #     names.append(name)


        # try: 
        #     for tweet in tweets['includes']['users']:
        #         try: 
        #             locations.append(tweet['location'])
        #         except:
        #             locations.append('')

        #         try: 
        #             usernames.append(tweet['username'])
        #         except:
        #             usernames.append('')
        # except:
        #     locations.append('')
        #     usernames.append('')
        # ticker_dict[ticker]['locations'] = locations
        # ticker_dict[ticker]['texts'] = texts
        # ticker_dict[ticker]['created_dates'] = created_dates
        # ticker_dict[ticker]['usernames'] = usernames
        #print(len(locations), len(tickers), len(usernames), len(texts), len(created_dates))
    df = pd.DataFrame({'tickers': tickers,
                        #'username': usernames, 
                        'texts':texts, 
                        #'name': names, 
                        'dates': created_dates})
    twitter_data = df.to_json(orient='records')  
    ti.xcom_push(key='twitter_data', value=twitter_data)

def tweetdata_upload(ti):
    '''push the twitter data into bigquery 
    '''
    twitter_data = ti.xcom_pull(key='twitter_data', task_ids=['get_twitter_data'])[0]
    openfile=open('/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/test-proj-378801-e260b3ef768e.json')
    jsondata=json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']
    df = pd.DataFrame(eval(twitter_data))
    df['texts'] = list(map(lambda x: x.encode('utf-8','replace'), df['texts']))
    #df.to_csv("twitter_data.csv", index=False)
    #df = df.to_json(orient='records')
    df['tickers'] = list(map(lambda x: x.encode('utf-8','strict'), df['tickers']))
    #df['texts'] = list(map(lambda x: x.encode('utf-8','replace'), df['texts']))
    df['dates'] = list(map(lambda x: x.encode('utf-8','strict'), df['dates']))

    #df.columns = df.columns.str.replace(' ', '_')


    # Construct a BigQuery client object.
    credentials_path = '/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/test-proj-378801-e260b3ef768e.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".twitter_data.stock_info"
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

with DAG('tweets_data_dag',
            default_args=default_args,
            description='Collect Twitter Info For Analysis',
            catchup=False, 
            start_date= datetime(2020, 12, 23), 
            schedule_interval= None # to change to 0 0 * * * (daily)
  
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