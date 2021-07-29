import json
import gzip
import pyodbc
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import text
import urllib
from urllib.request import urlopen
import traceback
import logging
from azure.storage.blob import BlobServiceClient
import time
import pandas as pd
import config
from process_blob import connect_to_azure

#This script takes the publications from CORD19 data and ingests them into SQL database
#The dataset metadata.csv has to be stored in the same folder as script
#To run this script: python ./process_cord.py

#Creating table in SQL DB for publications
def create_table(tablename):
    engine_azure = connect_to_azure()

    meta = MetaData()
    
    publications = Table(
        tablename, meta, 
        Column('cord_uid', String(255)),
        Column('title', String(1024)),
        Column('doi', String(255)),
        Column('pmcid', String(255)),
        Column('pubmed_id', String(255)), 
        Column('arxiv_id', String(255)),
        Column('url', String(1024))
    )
    meta.create_all(engine_azure)

#MAIN FUNCTION
if __name__ ==  '__main__':
    #Read cord data from the file
    with open('./metadata.csv', 'r', encoding='utf-8') as file:
        data_df = pd.read_csv(file)

    #Leave only needed columns
    filtered_df = data_df[["cord_uid", "title", "doi", "pmcid", "pubmed_id", "arxiv_id", "url"]]

    #Truncate title and url values to 1000 characters
    filtered_df['title'] = filtered_df['title'].str.slice(0,1000)

    filtered_df['url'] = filtered_df['url'].str.slice(0,1000)

    engine_azure = connect_to_azure()
    
    #Load publications into database
    filtered_df.to_sql('cord_publications_final', con=engine_azure, index=False, if_exists='append', chunksize=100, method='multi')