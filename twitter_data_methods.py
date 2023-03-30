import datetime
import yfinance as yf
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import os
import json
import tweepy
from datetime import datetime, timedelta 
import io 

from tweepy import OAuthHandler
from timeit import default_timer as timer

def tweetdata_extract(ti):
    tickers_list=[
    'ocbc',
    'jardine matheson',
    'comfortdelgro',
    'genting',
    'hongkong land holdings',
    'dbs',
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
    'st engineering',
    'cycle and carriage',
    'emperador',
    'keppel'
    'uol group',
    'yangzijiang financial'
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

    tickers = []
    author_id = []
    texts = []
    created_dates = []
    for ticker in tickers_list:
        ticker = f'"{ticker}"'
        print(ticker)
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
        
    df = pd.DataFrame({'tickers': tickers,
                        #'username': usernames, 
                        'texts':texts, 
                        #'name': names, 
                        'dates': created_dates})
    twitter_data = df.to_json(orient='records')
    print(twitter_data)  
    ti.xcom_push(key='twitter_data', value=twitter_data)


def tweetdata_transform(ti):
    '''push the twitter data into bigquery 
    '''
    twitter_data = ti.xcom_pull(key='twitter_data', task_ids=['tweetdata_extract'])[0]
    openfile=open('/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/is3107-test.json')
    jsondata=json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']
    df = pd.read_json(io.StringIO(twitter_data), encoding='utf-8')

    df['texts'] = df['texts'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))
    df['tickers'] = df['tickers'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))
    df['dates'] = df['dates'].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))

    # Construct a BigQuery client object.
    credentials_path = '/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/is3107-test.json'
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