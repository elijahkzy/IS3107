from google.cloud import bigquery
import os
import json

# Change to your key
key = 'C:/AA NUS/Y3S2/IS3107/Project Testing/key.json'

#Get Project ID
openfile = open(key)
jsondata = json.load(openfile)
openfile.close()
project_id = jsondata['project_id']

# Construct a BigQuery client object.
credentials_path = key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
client = bigquery.Client()

# TODO(developer): Set dataset_id to the ID of the dataset to create.
# dataset_id = "{}.your_dataset".format(client.project)

def setupDataset():
    '''
    Sets up the dataset
    '''
    dataset = ["yfinance_data", "twitter_data", "combined_data"] #list of datasets to make 

    for d in dataset:
        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(project_id  + "." + d)
        # TODO(developer): Specify the geographic location where the dataset should reside.
        dataset.location = "asia-southeast1"
        # Send the dataset to the API for creation, with an explicit timeout.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    

def setupTable():
    '''
    Sets up the respective tables
    '''

    #Setup YFinance Table
    schema = [
        bigquery.SchemaField("Date", "TIMESTAMP"),
        bigquery.SchemaField("Ticker", "STRING"),
        bigquery.SchemaField("Open", "FLOAT"),
        bigquery.SchemaField("High", "FLOAT"),
        bigquery.SchemaField("Low", "FLOAT"),
        bigquery.SchemaField("Close", "FLOAT"),
        bigquery.SchemaField("Adj_Close", "FLOAT"),
        bigquery.SchemaField("Volume", "INTEGER"),
        bigquery.SchemaField("MA_5days", "FLOAT"),
        bigquery.SchemaField("Signal", "STRING")
    ]
    table = bigquery.Table(project_id + "." + "yfinance_data.stock_info", schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

    #Setup Combined Table
    schema = [
        bigquery.SchemaField("Date", "TIMESTAMP"),
        bigquery.SchemaField("Ticker", "STRING"),
        bigquery.SchemaField("Open", "FLOAT"),
        bigquery.SchemaField("High", "FLOAT"),
        bigquery.SchemaField("Low", "FLOAT"),
        bigquery.SchemaField("Close", "FLOAT"),
        bigquery.SchemaField("Adj_Close", "FLOAT"),
        bigquery.SchemaField("Volume", "INTEGER"),
        bigquery.SchemaField("MA_5days", "FLOAT"),
        bigquery.SchemaField("Signal", "STRING"),
        bigquery.SchemaField("Weighted_Compound_Score", "FLOAT"),
    ]
    table = bigquery.Table(project_id + "." + "combined_data.stock_info", schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

setupDataset()
setupTable()
