from google.cloud import bigquery 
import pandas as pd
import yfinance as yf
import os
import json
import snscrape.modules.twitter as sntwitter
from datetime import timedelta  
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
import tweepy
from datetime import datetime, timedelta
from tweepy import OAuthHandler
import re
import nltk
from nltk.corpus import stopwords
from nltk.sentiment import SentimentIntensityAnalyzer

# Change to your key
# key = '/mnt/c/Users/hsinz/Desktop/nus/Y3S2/IS3107/Proj/privateKey.json'
key = '/mnt/c/Users/hsinz/Desktop/nus/Y3S2/IS3107/Proj/sharedkey.json'

def tweetdata_extract(ti):
    #scrape 1 day of tweets up to 100 tweets based on tickers
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
    likeCount = []
   
    # Get the current UTC date and time
    utc_now = datetime.utcnow()

    # Set the time to 00:00:00
    utc_today = datetime.combine(utc_now.date(), datetime.min.time())

    # Format the datetime object to a string
    formatted_utc_today = utc_today.strftime('%Y-%m-%dT%H:%M:%SZ')
    currentDate = formatted_utc_today[:10]
    currentTime = utc_now.strftime("%H:%M:%S")
    print(f'Retrieving tweets for date: {currentDate} as of time: {currentTime}')

    for ticker in tickers:
        print(ticker[1])
        keyword = ticker[1] if ticker[1] != 'grab' else '#grab'

        tweets = client.search_recent_tweets(query= keyword, max_results=100,
            start_time = formatted_utc_today,
            tweet_fields = ['author_id','created_at','text','source','lang','geo', 'public_metrics'],
            user_fields = ['name','username','location','verified'],
            expansions = ['geo.place_id', 'author_id'],
            place_fields = ['country','country_code'])
        try:
            for tweet in tweets['data']:
                if tweet['lang'] == 'en':
                    tickerName.append(keyword if keyword != '#grab' else 'grab')
                    texts.append(tweet['text'])
                    created_dates.append(tweet['created_at'])
                    author_id.append(tweet['author_id'])
                    tickerCode.append(ticker[0])
                    likeCount.append(tweet['public_metrics']['like_count'])
        except:
            tickerName.append('')
            texts.append('')
            created_dates.append('')
            author_id.append('')
            tickerCode.append(ticker[0])
            likeCount.append(0)

    df = pd.DataFrame({'Ticker_Name': tickerName,
                       'Ticker': tickerCode,
                        'Texts':texts, 
                        'Like_Count': likeCount,
                        'Datetime': created_dates})
    df = df.dropna(axis = 1)
    twitter_data = df.to_json(orient='records')
    ti.xcom_push(key='twitter_data', value=twitter_data)

def tweetdata_upload(ti):
    '''push 1 day of twitter data into bigquery 
    '''
    twitter_data = ti.xcom_pull(key='twitter_data', task_ids=['get_twitter_data'])
    json_str = ''.join(twitter_data)
    df = pd.read_json(json_str, encoding='utf-8', orient = 'records')

    openfile=open(key)
    jsondata=json.load(openfile)
    openfile.close()

    # Construct a BigQuery client object.
    credentials_path = key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".twitter_data.stock_info"
    job_config = bigquery.LoadJobConfig(
        encoding='UTF-8',
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
    job.result()
    print(f'Appended {len(df)} rows of date')

def get_staging_data(ti):
    # pull data from staging area of twitter table in bigquery

    credentials_path = key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client()

    openfile = open(key)
    jsondata = json.load(openfile)

    project_id = jsondata['project_id']
    staging_table_id = project_id + ".twitter_data.stock_info"

    table = client.get_table(staging_table_id)
    df = client.list_rows(table).to_dataframe()
    twitter_staging_data = df.to_json(orient='records')

    print(f'Successfully loaded twitterdata with {len(df)} rows')
    ti.xcom_push(key = 'twitter_staging_data', value = twitter_staging_data)

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

def splitScore(df):
    df['Negative'] = df['Score'].apply(lambda x: x.get('neg'))
    df['Neutral'] = df['Score'].apply(lambda x: x.get('neu'))
    df['Positive'] = df['Score'].apply(lambda x: x.get('pos'))
    df['Compound'] = df['Score'].apply(lambda x: x.get('compound'))
    return df

def weighted_avg(df):
    weights = df['Like_Count'] + 1
    return (df['Compound'] * weights).sum() / weights.sum()

def clean_twitter_data(ti):
    # cleans the data, removing hashtags, stop words etc.

    # twitter_data_str = ti.xcom_pull(key='twitter_data', task_ids=['get_twitter_data'])
    twitter_data_str = ti.xcom_pull(key='twitter_staging_data')
    json_str = ''.join(twitter_data_str)
    df = pd.read_json(json_str, encoding='utf-8', orient = 'records')

    df['Texts_cleaned'] = df['Texts'].apply(lambda x: clean_tweet(x))
    twitter_data_cleaned = df.to_json(orient = 'records')
    ti.xcom_push(key = 'twitter_data_cleaned', value = twitter_data_cleaned)
    print('Sucessfully cleaned data')

def process_twitter_data(ti):
    # NLP on the cleaned data using sentiment analysis 

    twitter_data_str = ti.xcom_pull(key='twitter_data_cleaned')
    json_str = ''.join(twitter_data_str)
    df = pd.read_json(json_str, encoding='utf-8', orient = 'records')
    
    sia = SentimentIntensityAnalyzer()
    df['Score'] = df['Texts_cleaned'].apply(lambda x: sia.polarity_scores(x))
    df = splitScore(df)
    df['Date'] = df['Datetime'].apply(lambda x: x.date())
    df.to_json(orient='records')

    aggregated_data = df.groupby(['Ticker_Name', 'Ticker', 'Date'], as_index = False).apply(weighted_avg)
    aggregated_data.rename(columns = {None: 'Weighted_Compound_Score'}, inplace = True)

    twitter_data_processed = df.to_json(orient = 'records')
    aggregated_data_processed = aggregated_data.to_json(orient = 'records')
    ti.xcom_push(key = 'twitter_data_processed', value = twitter_data_processed)
    ti.xcom_push(key = 'aggregated_data_processed', value = aggregated_data_processed)

    print('Successfully processed data')

def tweet_processed_data_upload(ti):
    #  uploads data into bigquery

    twitter_data_processed = ti.xcom_pull(key='twitter_data_processed')
    json_str = ''.join(twitter_data_processed)
    df = pd.read_json(json_str, encoding='utf-8', orient = 'records')

    aggregated_data_processed = ti.xcom_pull(key = 'aggregated_data_processed')
    json_str_agg = ''.join(aggregated_data_processed)
    df_aggregated = pd.read_json(json_str_agg, encoding='utf-8', orient = 'records')

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
    aggregated_table_id = project_id + '.twitter_data.stock_aggregated'
    job = client.load_table_from_dataframe(df_aggregated, aggregated_table_id, job_config=job_config)

    job.result()

    print(f'Pushed data into big query table {staging_table_id}')
    print(f'Pushed aggregated data into big query table {aggregated_table_id}')

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

    get_staging_data = PythonOperator(
        task_id='get_staging_data',
        provide_context=True,
        python_callable=get_staging_data
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

    push_processed_twitter_data = PythonOperator(
        task_id='push_processed_twitter_data',
        provide_context=True,
        python_callable=tweet_processed_data_upload
    )

get_twitter_data >> push_twitter_data >> get_staging_data >> clean_twitter_data >> process_twitter_data >> push_processed_twitter_data
