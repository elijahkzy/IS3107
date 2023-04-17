import datetime
from google.cloud import bigquery 
import pandas as pd
import yfinance as yf
import os
import json
import snscrape.modules.twitter as sntwitter
from datetime import datetime, timedelta  

from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import csv
import logging
import io

from airflow import DAG
import tweepy
from datetime import datetime, timedelta, date

from tweepy import OAuthHandler
from timeit import default_timer as timer

import pandas as pd

# Change to your key
# key = '/mnt/c/Users/hsinz/Desktop/nus/Y3S2/IS3107/Proj/privateKey.json'
key = '/mnt/c/Users/hsinz/Desktop/nus/Y3S2/IS3107/Proj/sharedkey.json'

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

    # Create an empty list to store the tweets
    tweets = []
    for tup in tickers:
        keyword = f'{tup[1]}' #can use OR to specify other things #hashtag also can
        # start_date = date(2023, 4, 1)
        # end_date = date(2023, 4, 7)
        end_date = date.today()
        start_date = end_date - timedelta(days = 7)
        user = ''

        # Create a list of dates between start_date and end_date
        delta = timedelta(days=1)
        date_range = pd.date_range(start_date, end_date, freq='D').tolist()


        #limit of tweets per day
        limit = 10

        # Loop over each day in the date range and retrieve X tweets per day
        #if count == limit or when the loop ends
        for i in range(len(date_range)):
            day = date_range[i].strftime('%Y-%m-%d')
            dayNext = (date_range[i] + timedelta(days = 1)).strftime('%Y-%m-%d')
            query = f'{keyword} from:{user} since:{day} until:{dayNext} lang:en'
            count = 0
            for tweet in sntwitter.TwitterSearchScraper(query).get_items():
                if (count == limit):
                    break
                tweets.append([keyword, tup[0], tweet.date, tweet.content, tweet.likeCount])
                count+= 1
            print(f'{keyword}_{day}: {count} total tweets retrieved')

    # Convert the list of tweets to a Pandas DataFrame
    df = pd.DataFrame(tweets, columns=['Ticker_Name', 'Ticker', 'Datetime', 'Texts', 'Likecount'])
    
    twitter_data = df.to_json(orient='records')
    # print(twitter_data)  
    ti.xcom_push(key='twitter_data', value=twitter_data)

def tweetdata_upload(ti):
    '''push the twitter data into bigquery 
    '''
    twitter_data = ti.xcom_pull(key='twitter_data', task_ids=['get_twitter_data'])[0]
    openfile=open(key)
    jsondata=json.load(openfile)
    openfile.close()
    df = pd.read_json(io.StringIO(twitter_data), encoding='utf-8')

    df['Texts'] = df['Texts'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))
    df['Ticker_Name'] = df['Ticker_Name'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))
    # df['Datetime'] = df['Datetime'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))
    df['Ticker'] = df['Ticker'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))
    df['Likecount'] = df['Likecount'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))

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
