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

#SUPPORT FUNCTIONS
def read_azure_blob(blobname, table_name):
    engine_azure = connect_to_azure()

    #Check if already processed
    if not check_blob_history(blobname):
        #Download from blob
        blob_service_client_instance = BlobServiceClient(account_url=config.STORAGEACCOUNTURL, credential=config.STORAGEACCOUNTKEY)

        container_client = blob_service_client_instance.get_container_client(config.CONTAINERNAME) 

        blob_client = container_client.get_blob_client(blobname)

        #Generate separate files for each process, otherwise they cause locks
        system_pid = os.getpid()

        temp_filename = './temp_%s.json' % system_pid

        with open(temp_filename, "wb") as my_blob:
            download_stream = blob_client.download_blob()
            my_blob.write(download_stream.readall())

        pub_df = prepare_df(temp_filename)

        #Make sure the title does not exceed 1000 characters - this is for SQL DB
        pub_df['title'] = pub_df['title'].str.slice(0,1000)

        pub_df.to_sql(table_name, con=engine_azure, index=False, if_exists='append', chunksize=100, method='multi')

        #writes blobname to history column
        write_history(blobname)

        #remove created temp file
        os.remove(temp_filename)
    else:
        print('blob already processed')

#Read json file, return dataframe
def prepare_df(temp_file):
    with gzip.open(temp_file,'rb') as f:
        obj_list = []
        for line_count, json_line in enumerate(f):
            line_str = json.loads(json_line)
            obj = {}

            #Consider only publications with pid
            if line_str['type'] != 'publication' or line_str['pid'] == []:
                continue

            obj['oaid'] = line_str['id']

            if 'maintitle' in line_str:
                obj['title'] = line_str['maintitle']
            elif 'description' in line_str:
                obj['title'] = line_str['description']

            #Dealing with some title that come out as lists
            if isinstance(obj['title'], list):
                if len(obj['title']) == 0:
                    continue
                obj['title'] = obj['title'][0]
            
            #If not empty, parse pid array into doi and pmid fields
            obj = parse_pid_array(obj, line_str['pid'])
            obj['publication_date'] = 'empty'
            obj_list.append(obj) 
        
        #Convert obj_list into dataframe
        df = pd.DataFrame(obj_list)
        
    return df

#Parses pid array into respective doi or pmid columns
def parse_pid_array(obj, pid):
    obj['doi'] = 'empty'
    obj['pmid'] = 'empty'
    obj['arxiv'] = 'empty'
    for item in pid:
        if item['scheme'] == 'doi':
            obj['doi'] = item['value']
        if item['scheme'] == 'pmid':
            obj['pmid'] = item['value']
        if item['scheme'] == 'arxiv':
            obj['arxiv'] = item['value']
    return obj

#Send prepared batches of data to Azure SQL DB
def upload_batches(batches, engine, table_obj):
    for count, item in enumerate(batches):
        try:
            ins_statement = table_obj.insert().values(item)
            engine.execute(ins_statement)
        except:
            print('error in batch:')
            for record in item:
                print(record)
    print('done uploading batches')

def connect_to_azure():
    #connect to database
    params = urllib.parse.quote_plus(r'Driver={ODBC Driver 17 for SQL Server};Server=#DB_URL#;Database=#DBNAME#;Uid=#DB_ID#;Pwd=#DB_KEY#;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine_azure = create_engine(conn_str,echo=False)

    return engine_azure

#Store blobnames that we have already processed
def write_history(blobname):
    engine_azure = connect_to_azure()
    engine_azure.execute("INSERT INTO history VALUES ('%s');" % (blobname))

#If blob name is processed, do not check it again
def check_blob_history(blobname):
    engine_azure = connect_to_azure()
    result = engine_azure.execute("SELECT blobname FROM history WHERE blobname = '%s'" % blobname)
    resultset = result.fetchall()
    if len(resultset) > 0:
        return True
    else:
        return False