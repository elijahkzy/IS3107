import datetime
import yfinance as yf
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import os
import json

def financials_extract(ti):
    '''
    Extract stock information in STI Components for past five days
    Gets the stock data using Yahoo Finance API in Pandas Dataframe and push as JSON

    Input: None
    Output: None
    '''
    #STI Index
    tickers = ['YF8.SI','BS6.SI','J36.SI', 'O39.SI', 'D05.SI',	'U11.SI', 'C09.SI',	'9CI.SI','C07.SI',	'A17U.SI',	'Z74.SI','ME8U.SI','AJBU.SI',	
            'C38U.SI',	'M44U.SI','U14.SI','V03.SI','D01.SI', 'H78.SI',	'BN4.SI','BUOU.SI',	'Y92.SI','C52.SI','S63.SI',	'G13.SI',
            'EMI.SI','N2IU.SI',	'F34.SI','U96.SI','S58.SI']
    # stock_dat = yf.download(tickers,period="2d",actions=False,group_by=tickers)
    i = 0
    df = pd.DataFrame()

    for ticker in tickers:
        prices = yf.download(ticker, period = '1d').iloc[: , :6].dropna(axis=0, how='any')
        prices = prices.loc[~prices.index.duplicated(keep='last')]
        prices = prices.reset_index()

        prices.insert(loc = 1, column = 'Ticker', value = ticker)
        prices = prices.rename({'Adj Close': 'Adj_Close'}, axis=1)
        df = pd.concat([df, prices],ignore_index = True)
        i+= 1

    stock_info_daily = df.to_json(orient='records')
    ti.xcom_push(key='stock_info', value=stock_info_daily)

def financials_stage(ti):
    '''push the raw stockdata into google bigquery dataset yfinance_data_raw
    '''
    stock_info_daily = ti.xcom_pull(key='stock_info', task_ids=['financials_extract'])[0]
    df = pd.DataFrame(eval(stock_info_daily))

    openfile=open('/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/is3107-test.json')
    jsondata=json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']

    # Construct a BigQuery client object.
    credentials_path = '/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/is3107-test.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".yfinance_data_raw.stock_info"
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".yfinance_data_raw.stock_info"
    pandas_gbq.to_gbq(df, destination_table=staging_table_id, 
                    project_id=project_id,
                    if_exists ='append')
    # job = client.load_table_from_dataframe(df, staging_table_id)
    # job.result()