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
from datetime import datetime, timedelta  
from timeit import default_timer as timer

import pandas as pd
import re
import nltk
from nltk.corpus import stopwords
from nltk.sentiment import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')
nltk.download('stopwords')

# Change to your key
key = '/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/test-proj-378801-e260b3ef768e.json'
# key = '/mnt/c/Users/Estella Lee Jie Yi/OneDrive - National University of Singapore/Desktop/NUS/Y3S2/IS3107/project/key.json'

def get_twitter_data(ti):
    # pull data from staging area of twitter table in bigquery

    credentials_path = key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client()

    openfile = open(key)
    jsondata = json.load(openfile)

    project_id = jsondata['project_id']
    staging_table_id = project_id + ".twitter_data_raw.stock_info"

    table = client.get_table(staging_table_id)
    df = client.list_rows(table).to_dataframe()
    twitter_data = df.to_json(orient='records')

    print(f'Successfully loaded twitterdata with {len(df)} rows')
    ti.xcom_push(key = 'twitter_data', value = twitter_data)

def clean_tweet(tweet):
        # method for cleaning tweets used in clean_twitter_data dag

        tweet = re.sub(r'http\S+', '', tweet)
        # Remove mentions
        tweet = re.sub(r'@[A-Za-z0-9_]+', '', tweet)
        # Remove hashtags
        tweet = re.sub(r'#', '', tweet)
        # Remove non-alphabetic characters
        tweet = re.sub(r'[^a-zA-Z\s]', '', tweet)
        # Convert to lowercase
        tweet = tweet.lower()
        # Remove stop words
        stop_words = set(stopwords.words('english'))
        words = tweet.split()
        filtered_words = [word for word in words if word not in stop_words]
        return ' '.join(filtered_words)

def clean_twitter_data(ti):
    # cleans the data, removing hashtags, stop words etc.

    twitter_data_str = ti.xcom_pull(key='twitter_data', task_ids=['get_twitter_data'])
    json_str = ''.join(twitter_data_str)
    df = pd.read_json(json_str, encoding='utf-8', orient = 'records')

    df['texts_cleaned'] = df['texts'].apply(lambda x: clean_tweet(x))
    twitter_data_cleaned = df.to_json(orient = 'records')
    ti.xcom_push(key = 'twitter_data_cleaned', value = twitter_data_cleaned)
    print('Sucessfully cleaned data')

def process_twitter_data(ti):
    # NLP on the cleaned data using sentiment analysis 

    twitter_data_str = ti.xcom_pull(key='twitter_data_cleaned')
    json_str = ''.join(twitter_data_str)
    df = pd.read_json(json_str, encoding='utf-8', orient = 'records')

    sia = SentimentIntensityAnalyzer()
    df['score'] = df['texts_cleaned'].apply(lambda x: sia.polarity_scores(x))

    twitter_data_processed = df.to_json(orient = 'records')
    ti.xcom_push(key = 'twitter_data_processed', value = twitter_data_processed)

    print('Successfully processed data')

def tweetdata_upload(ti):
    #  uploads data into bigquery

    twitter_data_processed = ti.xcom_pull(key='twitter_data_processed')
    json_str = ''.join(twitter_data_processed)
    df = pd.read_json(json_str, encoding='utf-8', orient = 'records')

    openfile=open(key)
    jsondata=json.load(openfile)
    openfile.close()

    # Construct a BigQuery client object.
    credentials_path = key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client()

    project_id = jsondata['project_id']
    staging_table_id = project_id + ".twitter_data.stock_processed"
    job_config = bigquery.LoadJobConfig(
        encoding='UTF-8',
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
    job.result()

    print(f'Pushed data into big query table {staging_table_id}')

default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'email_on_failure': False,
     'email_on_retry': False,
     'retries': 1
    }

with DAG(
    'nlp_tweets_dag',
    default_args=default_args,
    description='Apply NLP on tweets For Analysis',
    catchup=False, 
    start_date= datetime(2020, 12, 23), 
    schedule_interval=timedelta(days=1)
) as dag:
    
    get_twitter_data = PythonOperator(
        task_id='get_twitter_data',
        provide_context=True,
        python_callable=get_twitter_data
    )

    clean_twitter_data = PythonOperator(
        task_id='clean_twitter_data',
        provide_context=True,
        python_callable=clean_twitter_data
    )

    process_twitter_data = PythonOperator(
        task_id='process_twitter_data',
        provide_context=True,
        python_callable=process_twitter_data
    )

    push_twitter_data = PythonOperator(
        task_id='push_twitter_data',
        provide_context=True,
        python_callable=tweetdata_upload
    )

get_twitter_data >> clean_twitter_data >> process_twitter_data >> push_twitter_data
