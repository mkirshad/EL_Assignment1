"""
***** 10/07/2022 *****
* A Program to upload CSV files from GCS to BigQuery based upon configuration *
* Developer: Muhammadd Kashif Irshad *
"""
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import pandas as pd
import os
import json
import math
import requests
from google.cloud import storage
import threading
import re

# Load Configuration
def load_config():
    f = open('config2.json')
    config = json.load(f)
    f.close()
    return config

# Get Load to BigQuery from CSV Configuration by Building it
def get_job_config(folder, over_write=True, blob_name=""):
    job_config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.CSV,
        autodetect = True,
        ignore_unknown_values = True,
        allow_jagged_rows = True,
        max_bad_records = 1000,
        allow_quoted_newlines = True,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    if bool(re.match(".*.[c|C][s|S][v|V]", blob_name)) == False:
        job_config.quote_character = ''

    if "fileProperties" in folder:
        for fileProperty in folder["fileProperties"]:
            if (bool(re.match(fileProperty["filePattern"], blob_name)) == True) or ("filePattern2" in fileProperty and bool(re.match(fileProperty["filePattern2"], blob_name)) == True):
                if "skip_leading_rows" in fileProperty:
                    job_config.skip_leading_rows = fileProperty["skip_leading_rows"]
                if "quote_character" in fileProperty:
                    if fileProperty["quote_character"] == "None":
                        job_config.quote_character = None
                if "schema" in fileProperty:
                    schemaArray = []
                    for schemaField in fileProperty["schema"]:
                        schemaArray.append(bigquery.SchemaField(schemaField["fieldName"], schemaField["fieldType"]))
                    job_config.schema = schemaArray
    return job_config

# Get BigQuery dataset by name or create it if not found
def getDataSet(bq_client, dataset_id, DeleteDataset):
    dataset_id = "{}.{}".format(bq_client.project, dataset_id)
    if DeleteDataset:
        bq_client.delete_dataset(
            dataset_id, delete_contents=True, not_found_ok=True
        )  # Make an API request.
    try:
        bq_client.get_dataset(dataset_id)  # Make an API request.
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = bq_client.create_dataset(dataset, timeout=30)
    return dataset_id

# Get Table name
def get_table_name(dataset_id, blob_name):
    return "{}.{}".format(dataset_id, blob_name.replace('/','_').replace('(','').replace(')','').split('.')[0] ) 

# Upload Group of files in Folder
def uploadFiles(bq_client, storage_client, config, dataset_id, folder):
    v_over_write = True
    for blob in storage_client.list_blobs(config['gcsBucketName'], prefix= folder['folderName']):
        if folder['active'] == True:
            job_config = get_job_config(folder, v_over_write, blob.name)
            uri = "gs://" + config['gcsBucketName']+ "/" + blob.name
            try:
                table_id = get_table_name(dataset_id, blob.name)
                load_job = bq_client.load_table_from_uri(
                    uri, table_id, job_config=job_config
                )
            except Exception as e: 
                print(table_id)
                print(blob.name)
                print(e)
                print('')

# Main function
def main_fun(test_folder_entry = None):
    config = load_config()
    key_path = "key/" + config['serviceAccountKey']
    storage_client = storage.Client.from_service_account_json(key_path)
    bq_client = bigquery.Client.from_service_account_json(key_path)
    dataset_id = getDataSet(bq_client, config['bqDatasetName'], config['DeleteDataset'])
    for folder in config['folderList']:
        if test_folder_entry != None: 
            if folder['folderName'] == test_folder_entry:
                t1 = threading.Thread(target=uploadFiles, args=(bq_client, storage_client, config, dataset_id, folder))
                t1.start()
        else:
            t1 = threading.Thread(target=uploadFiles, args=(bq_client, storage_client, config, dataset_id, folder))
            t1.start()


