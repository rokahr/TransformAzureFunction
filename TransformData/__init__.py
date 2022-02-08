import logging

import azure.functions as func

import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
from io import BytesIO
from azure.identity import ChainedTokenCredential,ManagedIdentityCredential
import pyarrow
import fastparquet

def initialize_storage_account(storage_account_name):
    
    try:
        global service_client
        MSI_credential = ManagedIdentityCredential()
    
        credential_chain = ChainedTokenCredential(MSI_credential)
        
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name), credential=credential_chain)
    
    except Exception as e:
        print(e)

def initialize_storage_account_local(storage_account_name, storage_account_key):
    
    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)

def getDF(container, filepath, filename):
    try:
        file_system_client = service_client.get_file_system_client(file_system=container)
        directory_client = file_system_client.get_directory_client(filepath)
        file_client = directory_client.get_file_client(filename)

        download = file_client.download_file()
        downloaded_bytes = download.readall()

        return pd.read_csv(BytesIO(downloaded_bytes))
    except Exception as e:
        return NULL

def writeDF(dataframe, container, filepath, filename):
    try:
        file_system_client = service_client.get_file_system_client(file_system=container)
        directory_client = file_system_client.get_directory_client(filepath)
        file_client = directory_client.get_file_client(filename)

        processedDF = dataframe.to_parquet(index=False)

        file_client.upload_data(data=processedDF,overwrite=True,length=len(processedDF))
        file_client.flush_data(len(processedDF))

        return func.HttpResponse(
             "Successfully written dataframe to destination",
             status_code=200
        )

    except Exception as e:
        return func.HttpResponse(
             "Could not write DataFrame",
             status_code=500
        )



def main(req: func.HttpRequest) -> func.HttpResponse:

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
             "Could not read body",
             status_code=500
        )

    in_container = req_body.get('in_container')
    in_path = req_body.get('in_path')
    in_file = req_body.get('in_file')

    out_container = req_body.get('out_container')
    out_path = req_body.get('out_path')
    out_file = req_body.get('out_file')

    storagekey = req_body.get('storagekey')

    try:
        if not storagekey:
            initialize_storage_account('externalstoragesr')
        else:
            initialize_storage_account_local('externalstoragesr',storagekey)
    except Exception as e:
        return func.HttpResponse(
             "Could not initialize DataLakeFileSystemClient. Make sure that you passed the key in local development and if running as Azure Function, MSI is activated and has permissions.",
             status_code=500
        )


    ### Get DF
    df = getDF(container=in_container, filepath=in_path, filename=in_file)

    ### Do fancy python stuff
    df2 = df.groupby('Name').sum()

    ### Write DF back
    return writeDF(dataframe=df2, container=out_container, filepath=out_path, filename=out_file)
