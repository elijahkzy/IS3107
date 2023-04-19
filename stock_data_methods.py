import datetime
import yfinance as yf
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from sklearn.preprocessing import MinMaxScaler
import os
import json
from sqlalchemy import create_engine


def financials_extract(ti):
    '''
    Extract stock information in STI Components for past five days
    Gets the stock data using Yahoo Finance API in Pandas Dataframe and push as JSON

    Input: None
    Output: None
    '''
    #STI Index
    tickers = ["D05.SI",
               "SE", 
               "O39.SI", 
               "U11.SI", 
               "Z74.SI",
               "F34.SI",
               "C6L.SI", 
               "GRAB",
               "G13.SI",
               "C38U.SI",
               "G07.SI",
               "C07.SI",
               "A17U.SI",
               "S63.SI",
               "BN4.SI"]
    # stock_dat = yf.download(tickers,period="2d",actions=False,group_by=tickers)
    i = 0
    df = pd.DataFrame()

    for ticker in tickers:
        prices = yf.download(ticker,period='1d').iloc[: , :6].dropna(axis=0, how='any')
        prices = prices.loc[~prices.index.duplicated(keep='last')]
        prices = prices.reset_index()

        prices.insert(loc = 1, column = 'Ticker', value = ticker)
        prices = prices.rename({'Adj Close': 'Adj_Close'}, axis=1)
        df = pd.concat([df, prices],ignore_index = True)
        i+= 1

    openfile=open('/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/key.json')
    jsondata=json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']

    # Construct a BigQuery client object.
    credentials_path = '/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".yfinance_data.raw_stock_info"
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".yfinance_data.raw_stock_info"
    df['Date'] = pd.to_datetime(df['Date'], unit='ms')

    pandas_gbq.to_gbq(df, destination_table=staging_table_id, 
                    project_id=project_id,
                    if_exists ='append')
    
    username = "postgres"
    password = "postgres"
    host = "localhost"
    port = "5432"
    database = "is3107"

    # Create a SQLAlchemy engine to connect to PostgreSQL database
    engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")

    # Load a pandas dataframe into a PostgreSQL table
    df.to_sql("raw_stock_data", con=engine, if_exists="append", index=False)

    pandas_gbq.to_gbq(df, destination_table=staging_table_id, 
                    project_id=project_id,
                    if_exists ='append')


    stock_info_daily = df.to_json(orient='records')
    ti.xcom_push(key='stock_info', value=stock_info_daily)

def financials_stage(ti):
    '''push the raw stockdata into google bigquery dataset yfinance_data_raw after calculating some metrics
    '''
    stock_info_daily = ti.xcom_pull(key='stock_info', task_ids=['financials_extract'])[0]
    #main_df = pd.DataFrame(eval(stock_info_daily))
    new_df = pd.DataFrame()

    credentials_path = '/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()

    openfile=open('/mnt/c/Users/darkk/OneDrive/NUS/Y3S2/IS3107/proj/key.json')
    jsondata=json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']

    tickers = ["D05.SI",
               "SE", 
               "O39.SI", 
               "U11.SI", 
               "Z74.SI",
               "F34.SI",
               "C6L.SI", 
               "GRAB",
               "G13.SI",
               "C38U.SI",
               "G07.SI",
               "C07.SI",
               "A17U.SI",
               "S63.SI",
               "BN4.SI"]
    
    for ticker in tickers:
        query = f"""
        SELECT *
        FROM hypnotic-shard-383009.yfinance_data.raw_stock_info
        WHERE Ticker = '{ticker}'
        """
        # Load query results into a DataFrame
        df = client.query(query).to_dataframe()

        df['Money_Flow_Multiplier'] = ((df['Close'] - df['Low']) - (df['High'] - df['Close'])) / (df['High'] - df['Low'])
        df['Money_Flow_Volume'] = df['Money_Flow_Multiplier'] * df['Volume']
        df['AD_Indicator'] = df['Money_Flow_Volume'].cumsum()
        df['AD_Indicator_Change'] = df['Money_Flow_Volume'] - df['Money_Flow_Volume'].shift(1)

        n = 7
        # lowest_low = df['Low'].rolling(window=n).min()
        # highest_high = df['High'].rolling(window=n).max()
        # df['%K'] = (df['Close'] - lowest_low) / (highest_high - lowest_low) * 100
        # df['%D'] = df['%K'].rolling(window=3).mean()

        highest_high = df['High'].rolling(window=7).max()
        lowest_low = df['Low'].rolling(window=7).min()
        close = df['Close']
        williams_r = -100 * (highest_high - close) / (highest_high - lowest_low)
        df['WilliamsR'] = williams_r
        new_df = pd.concat([new_df, df])

    scalar = MinMaxScaler()
    columns = ['AD_Indicator_Change','WilliamsR']
    new_df[columns] = scalar.fit_transform(new_df[columns])
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".yfinance_data.stock_info"
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".yfinance_data.stock_info"

    new_df['Date'] = pd.to_datetime(new_df['Date'], unit='ms')

    username = "postgres"
    password = "postgres"
    host = "localhost"
    port = "5432"
    database = "is3107"

    # Create a SQLAlchemy engine to connect to PostgreSQL database
    engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")

    # Load a pandas dataframe into a PostgreSQL table
    new_df.to_sql("staged_stock_data", con=engine, if_exists="append", index=False)

    pandas_gbq.to_gbq(new_df, destination_table=staging_table_id, 
                    project_id=project_id,
                    if_exists ='append')
    # job = client.load_table_from_dataframe(df, staging_table_id)
    # job.result()