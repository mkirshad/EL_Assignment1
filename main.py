from google.cloud import bigquery
from google.oauth2 import service_account
import os
from google.cloud import storage
import main2

key_path = "key/sk-erp-48d92fdfc506.json"

storage_client = storage.Client.from_service_account_json(key_path)

## 20221130 Start
def loadDataBQ():
    ###Item Name Start   
    vlist = os.listdir('data/')    
    fileName = None
    for fileName in vlist:
        bucket = storage_client.get_bucket('mki-assignment1')
        blob = bucket.blob('data' + '/' + fileName)
        blob.upload_from_filename('data/{}'.format(fileName))  
    main2.main_fun('data')

try:
    loadDataBQ()
except Exception as e:
    print("Oops!", e, "occurred.")
