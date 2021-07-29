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
    table_name = 'oa_projects_final'

    input_blobname = params[0]
    
    input_filetype = params[1]
    try:
        read_azure_blob(input_blobname, table_name)
    except Exception as e:
        print('Error at blob:', input_blobname)

#Read and parse json file, return dataframe
def prepare_df(temp_file):
    with gzip.open(temp_file,'rb') as f:
        obj_list = []
        for line_count, json_line in enumerate(f):
            try:
                line_str = json.loads(json_line)
                obj = {}
                if line_str['funding'] == []:
                    continue

                obj['oaid'] = line_str['id']
                obj['code'] = line_str['code']
                if 'title' in line_str:
                    obj['title'] = line_str['title']
                else:
                    obj['title'] = 'empty'
                if 'startdate' in line_str:
                    obj['startdate'] = line_str['startdate']
                else:
                    obj['startdate'] = 'empty'
                if 'enddate' in line_str:    
                    obj['enddate'] = line_str['enddate']
                else:
                    obj['enddate'] = 'empty'    
                obj['currency'] = 'empty'
                obj['amount'] = 0
                if 'granted' in line_str:
                    obj['currency'] = line_str['granted']['currency']
                    obj['amount'] = line_str['granted']['fundedamount']

                obj['jurisdiction'] = ''
                obj['longname'] = ''
                obj['shortname'] = ''

                for funding in line_str['funding']:
                    obj['jurisdiction'] = obj['jurisdiction'] + funding['jurisdiction'] + ';'
                    obj['longname'] = obj['longname'] + funding['name'] + ';'
                    obj['shortname'] = obj['shortname'] + funding['shortName'] + ';'
                
                obj_list.append(obj)
            except:
                print('error found in one of the lines')
                print(json_line)
                continue 
        #convert obj_list into dataframe
        df = pd.DataFrame(obj_list)
        
    return df