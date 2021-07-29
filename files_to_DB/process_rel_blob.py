from azure.storage.blob import BlobServiceClient
import config
import gzip
import json
import os
import urllib
import urllib.parse
import sys
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import text
import pandas as pd
from process_blob import read_azure_blob, upload_batches, connect_to_azure, write_history, check_blob_history

#Processes single file from blob storage and creates a record in SQL database
#This script runs from process_folder.py script
def process_blob(params):

    table_name = 'oa_publications_final'

    input_blobname = params[0]
    
    input_filetype = params[1]

    try:
        read_azure_blob(input_blobname, table_name)
    except:
        print('Error at blob:', input_blobname)

#Read and parse json file, return dataframe
def prepare_df(temp_file):
    with gzip.open(temp_file,'rb') as f:
        row_list = []
        for line_count, json_line in enumerate(f):
            line_str = json.loads(json_line)
            if (line_str['source']['type'] == 'project' and line_str['target']['type'] == 'result') or (line_str['target']['type'] == 'project' and line_str['source']['type'] == 'result'):
                row = {}
                row['src_id'] = line_str['source']['id']
                row['trg_id'] = line_str['target']['id']
                row['src_type'] = line_str['source']['type']
                row['trg_type'] = line_str['target']['type']
                row['rel_type'] = line_str['reltype']['name']
                #append data to list of dicts
                row_list.append(row)
            else:
                continue
                
    df = pd.DataFrame(row_list) 
        
    return df