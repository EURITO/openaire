import config
import mmap
import multiprocessing
import subprocess
import io
from itertools import repeat
from itertools import product
import sys
import os
import glob
import process_blob
import process_proj_blob
import datetime
from azure.storage.blob import BlobServiceClient
import pandas as pd
import pathlib

def zip_with_scalar(l, o):
    result = []
    for item in l:
        item_array = [item,o]
        result.append(item_array)
    return result

'''
    Run this script as follows: 
    python3 process_folder ##Data_Type## - here data type can be Publications, Projects or Relationships

    This script connects to the blob storage and writes cleaned entries to SQL database
    Config file should contain the following info:
        STORAGEACCOUNTURL= Azure storage account url
        STORAGEACCOUNTKEY= Azure storage account key
        CONTAINERNAME= Azure container name
        DBKEY = Target Azure SQL DB key
        DBUID = Target Azure SQL DB ID
        DBNAME = Target Azure SQL DB ID
        DBURL = 'tcp:#Database url (from Azure portal)#,1433'
    
    The Blob storage should have the following structure:
    container
    ----Projects
        ----part00xxx.json.gz
        ----part00xxx.json.gz
        ----part00xxx.json.gz
        ----part00xxx.json.gz
    ----Publications
        ----part00xxx.json.gz
        ----part00xxx.json.gz
        ----part00xxx.json.gz
        ----part00xxx.json.gz
    ----Relationships
        ----part00xxx.json.gz
        ----part00xxx.json.gz
        ----part00xxx.json.gz
        ----part00xxx.json.gz
'''
if __name__ ==  '__main__': 
    
    gz_files = []

    starttime = datetime.datetime.now().time()

    file_type = sys.argv[1]

    #Connect to Azure blob storage 
    blob_service_client_instance = BlobServiceClient(account_url=config.STORAGEACCOUNTURL, credential=config.STORAGEACCOUNTKEY)    
    container_client = blob_service_client_instance.get_container_client(config.CONTAINERNAME) 
    blob_list = container_client.list_blobs()

    #Get the list of files for the specified file type
    for blob in blob_list:
        if blob.name.startswith(file_type):
            gz_files.append(blob.name)
    
    try:
        
        p = multiprocessing.Pool(processes = 8) #set this number according to your machine

        zipped = zip_with_scalar(gz_files, file_type)
 
        if (file_type == 'Publications'):
            result = p.map(process_blob.process_blob, zipped)
        elif (file_type == 'Relationships'):
            result = p.map(process_rel_blob.process_blob, zipped)
        elif (file_type == 'Projects'):
            result = p.map(process_proj_blob.process_blob, zipped)
    
        p.close()
        p.join() # Wait for all child processes to close.
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, terminating workers")
        pool.terminate()
        pool.join()

    print('Overall start time:', starttime)
    print('Overall finish time:', datetime.datetime.now().time())