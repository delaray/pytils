# ****************************************************************
# KGML Google BigQuery, Google Storage, Data Persistence API
# ****************************************************************
#
# Part 1: Google BigQuery & Pandas GBQ
# Part 2: Google Cloud Storage
# Part 3: Data Persistence API
#
# ****************************************************************

# Python import
import os
import re

# Data science imports
import numpy as np
from numpy import dtype
import pandas as pd

# GCP Imports
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
from google.cloud import storage
import gcsfs
import pandas_gbq

# Project Imports
from utils.files import load_csv, save_csv
from utils.files import load_text_file, save_text_file
from utils.data import save_dict

import logging
logger = logging.getLogger()

# ****************************************************************
# DATA LOCATION 
# ****************************************************************

# GCP KGUTILS Google BQ Defaults
PROJECT_ID = 'opus-dev-hmbu-cdp'
PROJECT_DATASET = 'hmbu_opus_knowledge_graph'

# GCP KGUTILS Google Storage Defaults
KGML_BUCKET = 'ccdp-kgml-hmbu-dev'
KGML_STORAGE = 'orphans'

# Local FileSystem Data Directory 
DATA_DIR = os.environ['DATA_DIR']

# ****************************************************************
# Part 1:  Pandas GBQ
# ****************************************************************

# ----------------------------------------------------------------
# Pandas GBQ API
# ----------------------------------------------------------------

def run_pdbq(query, project_id=PROJECT_ID):
    'Uses Pandas-GBQ to run a Google BQ query and returns a dataframe.'

    df = pd.read_gbq(query, project_id=project_id)
    return df


# ****************************************************************
# Part 2: Google BigQuery
# ****************************************************************

# def bq_table_id(project, dataset, table_name):
#     return f'`{project}.{dataset}.{table_name}`'


# # --------------------------------------------------------------
# # Run BQ Query
# # --------------------------------------------------------------

# def run_bq_query(query, client=None):
#     'Runs a qury on Google Big Query and returns the results.'

#     client = bigquery.Client(project=PROJECT_ID) if client is None else client
#     query_job = client.query(query)

#     # Waits for job to complete.
#     results = query_job.result()
#     return results


# # --------------------------------------------------------------
# # Create BQ Table 
# # --------------------------------------------------------------

# def create_bq_table(project, dataset, table_name, df):
#     table_id = bq_table_id(project, dataset, table_name)
#     pandas_gbq.to_gbq(df, table_id, project_id=project)
#     return True


# # --------------------------------------------------------------
# # Delete BQ Table
# # --------------------------------------------------------------

# def delete_bq_table(project, dataset, table_name, client=None):
#     'Deletes the specified table in specified Opus hmbu environment.'

#     # Construct a BigQuery client object unless provide
#     if client is None:
#         client = bigquery.Client(project=project)

#     table_id = bq_table_id(project, dataset, table_name)

#     # Delete the table if it exists.
#     try:
#         client.delete_table(table_id, not_found_ok=True)
#         print("Deleted table '{}'.".format(table_id))
#         return True
#     except Exception as e:
#         print(f'Error deleting table:\n{e}')
#         return False

# ****************************************************************
# Part 5: GCP
# ****************************************************************

BQ_DATASET_NAME = 'hmbu_opus_knowledge_graph'

# ----------------------------------------------------------------

def bq_project_name(env='dev'):
    return f'opus-{env}-hmbu-cdp'


# ----------------------------------------------------------------

def bq_table_id(table_name, env='dev'):
    project_name = bq_project_name(env)
    table_id = f'`{project_name}.{BQ_DATASET_NAME}.{table_name}`'
    # table_id = f'{project_name}.{BQ_DATASET_NAME}.{table_name}'
    logger.warning(f'\nTable_id: {table_id}\n')
    return table_id


# --------------------------------------------------------------
# Pandas GBQ API
# --------------------------------------------------------------

def run_pdbq(query, project_id=PROJECT_ID):
    'Uses Pandas-GBQ to run a Google BQ query and returns a dataframe.'

    df = pd.read_gbq(query, project_id=project_id)
    return df


# --------------------------------------------------------------

def create_bq_table(table_name, df, project_id=PROJECT_ID, env='dev'):
    # table_id = bq_table_id(table_name, env=env)
    project_name = bq_project_name(env)
    table_id = f'{project_name}.{BQ_DATASET_NAME}.{table_name}'
    pandas_gbq.to_gbq(df, table_id, project_id=project_id)
    return True


# --------------------------------------------------------------
# Google BQ API
# --------------------------------------------------------------

def run_bq_query(query, client=None):
    'Runs a qury on Google Big Query and returns the results.'

    client = bigquery.Client(project=PROJECT_ID) if client is None else client
    query_job = client.query(query)

    # Waits for job to complete.
    results = query_job.result()
    return results


# --------------------------------------------------------------

def delete_bq_table(table_name, project_id=PROJECT_ID, env='dev', client=None):
    'Deletes the specified table in specified Opus hmbu environment.'

    # Construct a BigQuery client object unless provide
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)

    table_id = bq_table_id(table_name, env=env)[1:-1]

    # Delete the table if it exists.
    try:
        client.delete_table(table_id, not_found_ok=True)
        logger.warning("Deleted table '{}'.".format(table_id))
        return True
    except Exception as e:
        logger.error(f'Error deleting table:\n{e}')
        return False


# --------------------------------------------------------------
# Create BQ Table
# --------------------------------------------------------------

def ensure_bq_column_type_values(df):
    'Replaces NaN values with actual value based on column type.'
    
    types = df.dtypes
    if 'alternatives' in df.columns:
        df['alternatives'].fillna('')
    for col in df.columns:
        col_type = types.loc[col]
        if col_type == dtype('int32') or col_type == dtype('int64'):
            df[col] = df[col].fillna(0)
        if col_type == dtype('float32') or col_type == dtype('float64'):
            df[col] = df[col].fillna(0.0)
        if col_type == dtype('object'):
            df[col] = df[col].fillna('')
    df = df.fillna('')
    
    return df


# ----------------------------------------------------------------------

def table_to_bq(df, project=PROJECT_ID, dataset='', table_name=''):
    'Write dataframe to Big Query.'

    # Delete table if it already exists
    delete_bq_table(table_name, project_id=project)

    # Ensure resasonable values
    df = ensure_bq_column_type_values(df)
    
    # Create the new table
    create_bq_table(table_name,  df, project_id=project)

    return True


# **************************************************************
# Part 2: Google Cloud Storage
# **************************************************************

# --------------------------------------------------------------
# Read & Write Buckets
# --------------------------------------------------------------

def blobs_in_bucket(bucket=KGML_BUCKET, prefix=""):
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket)
    blobs = client.list_blobs(bucket, prefix=prefix)
    return blobs


def blob_name(blob):
    'Returns the last part of the name of a blob'
    return blob.name.split('/')[-1]


def named_blobs(blob_pathname, bucket=KGML_BUCKET):
    'Returns the blob object corresponding to pathname.'
    blobs = list(blobs_in_bucket(prefix=blob_pathname, bucket=bucket))
    if len(blobs) > 0:
        return blobs
    else:
        return None


def blob_exists(name):
    'Returns True if the named blob exists.'
    blobs = list(blobs_in_bucket(prefix=name))
    if len(blobs) > 0:
        return True
    else:
        return False


def create_blob(name, bucket=KGML_BUCKET):
    'Create a new google storgae blob directory.'

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket)
    blob = bucket.blob(name)
    blob.upload_from_string('')
    return blob


def load_csv_gs(pathname, project=PROJECT_ID):
    'Loads a CSV dirtectly from a blob and returns a dataframe.'

    fs = gcsfs.GCSFileSystem()
    with fs.open(pathname) as f:
        df = pd.read_csv(f)
    return df


# **************************************************************
# Part 3: Data Persistence API
# **************************************************************

# -----------------------------------------------------------
# data Local File System
# -----------------------------------------------------------

def data_directory_fs(name, target=None):
    'Return the directory path of a data.'

    if target is not None:
        return os.path.join(DATA_DIR, name, target)
    else:
        return os.path.join(DATA_DIR, name)
        


# -----------------------------------------------------------

def model_directory_fs(model_name):
    return data_directory_fs(model_name, target='model')


def model_pathname(name, filename):
    "Returns a pathaname to the data's model directory."
    return os.path.join(model_directory_fs(name), filename)


# -----------------------------------------------------------

def data_directory(model_name, folder='data'):
    return data_directory_fs(model_name, target=folder)


def data_pathname(model_name, filename, folder='data'):
    "Returns a pathaname to the data directory."
    return os.path.join(data_directory(model_name, folder=folder), filename)


# -----------------------------------------------------------

def results_directory_fs(model_name):
    return data_directory_fs(model_name, target='results')


def results_pathname(model_name, filename):
    "Returns a pathaname to the model directory."
    return os.path.join(results_directory_fs(model_name), filename)


# -----------------------------------------------------------

def ensure_model_directory_fs(name, folder=None):
    "Create directories if they don't exist."

    directory = data_directory_fs(name)
    if os.path.isdir(directory) is False:
        
        # This needs to be deprecated (see classifier)
        os.makedirs(directory)
        os.makedirs(directory + '/data')
        os.makedirs(directory + '/model')
        os.makedirs(directory + '/results')
        
    # Create subdirectory if specified
    if folder is not None:
        folder_dir = f'{directory}/{folder}'
        if os.path.isdir(folder_dir) is False:
            os.makedirs(folder_dir)
            
    return True


# -----------------------------------------------------------

# NB: Nature should be one of data, models or results.

def data_pathname_fs(name, filename, nature):
    'Return a pathname to the location of filename.'

    directory = data_directory_fs(name)
    return os.path.join(directory, nature, filename)


# -----------------------------------------------------------
# data Google Storage
# -----------------------------------------------------------

def target_directory_gs(model_name, target=None, bucket=KGML_BUCKET,
                        storage=KGML_STORAGE):
    'Return the google storage blob of a data.'

    if target is None:
        return f'{storage}/{model_name}'
    else:
        return f'{storage}/{model_name}/{target}/'


# -----------------------------------------------------------

def model_directory_gs(model_name, bucket=KGML_BUCKET,
                       storage=KGML_STORAGE):
    'Return the google storage model path of a data.'
    
    return target_directory_gs(model_name, target='model',
                               bucket=bucket, storage=storage)


def data_directory_gs(model_name, bucket=KGML_BUCKET,
                      storage=KGML_STORAGE, folder='data'):
    'Return the google storage data path of a data.'
    
    return target_directory_gs(model_name, target=folder,
                               bucket=bucket, storage=storage)


def results_directory_gs(model_name, bucket=KGML_BUCKET,
                         storage=KGML_STORAGE):
    'Return the google storage results path of a data.'
    
    return target_directory_gs(model_name, target='results',
                               bucket=bucket, storage=storage)


# -----------------------------------------------------------

def ensure_model_directory_gs(name, bucket=KGML_BUCKET,
                              storage=KGML_STORAGE):
    'Create data blob if it does not exist.'

    blob_path = data_directory_gs(name, bucket=bucket, storage=storage) + '/'
    if blob_exists(blob_path) is False:
        create_blob(blob_path + '/', bucket=KGML_BUCKET)
        create_blob(blob_path + 'data/', bucket=KGML_BUCKET)
        create_blob(blob_path + 'model/', bucket=KGML_BUCKET)
        create_blob(blob_path + 'results/', bucket=KGML_BUCKET)
        return True
    else:
        return False


# -----------------------------------------------------------

# NB: Nature should be one of data, models or results.

def data_pathname_gs(name, filename, nature, folder='data'):
    'Return a pathname to the location of filename.'

    directory = data_directory_gs(name, folder=folder)
    return f'{directory}/{nature}/{filename}'


def data_blobs(name, bucket=KGML_BUCKET, storage=KGML_STORAGE):
    'Return the list of blobs that comprise the named data.'

    blob_path = model_directory_gs(name, bucket=bucket, storage=storage)
    blobs = list(blobs_in_bucket(prefix=blob_path, bucket=bucket))
    if len(blobs) > 0:
        return blobs[1:]
    else:
        return None


# -----------------------------------------------------------
# Downloading Blobs
# -----------------------------------------------------------

def download_blob(blob_pathname, file_name, model_name, folder = 'data'):
    'Downloads a blob from the named model.'

    blobs = named_blobs(blob_pathname)
    if blobs is None:
        # print(f'Warning:\nNo blob named {blob_filenmae} was found on path {blob_pathname}.')
        # print(f'The file {file_pathname} was not dowloaded to local filesystem.\n')
        return False
    else:
        directory_path = data_directory_fs(model_name, target=folder)
        file_path = os.path.join(directory_path, file_name)
        for blob in blobs:
            blob.download_to_filename(file_path)
            return True


def download_data_blob(model_name, file_pathname, storage=KGML_STORAGE,
                       bucket=KGML_BUCKET, folder='data'):
    'Downloads a data blob from the named model.'

    blob_path = data_directory_gs(model_name, bucket=bucket, storage=storage,
                                  folder=folder)

    logger.warning(f'\nBlob path: {blob_path}\nFile pathname: {file_pathname}\n')
    blob_pathname = blob_path + os.path.basename(file_pathname)

    download_blob(blob_pathname, file_pathname, model_name, folder=folder)


def download_data_blobs(model_name, bucket=KGML_BUCKET, folder=None):
    'Downloads all data blobs for the named data.'

    # Ensure the local directory exists
    ensure_model_directory_fs(model_name, folder=folder)

    # Get the list of data blobs
    path = model_directory_gs(model_name)
    blobs = list(blobs_in_bucket(prefix=path), bucket=bucket)

    # Download each individual blob to local files
    for blob in blobs[1:]:
        filename = blob_name(blob)
        file_pathname = data_pathname(model_name, filename, folder=folder)
        download_data_blob(model_name, file_pathname, bucket=bucket)
    return True


def download_results_blob(model_name, filename, storage=KGML_STORAGE, bucket=KGML_BUCKET):
    'Downloads a results blob from the named model.'

    blob_path = results_directory_gs(model_name, bucket=bucket, storage=storage)
    download_blob(blob_path, filename, model_name)


# -----------------------------------------------------------
# Uploading Blobs
# -----------------------------------------------------------

def upload_blob(blob_pathname, file_pathname, bucket=KGML_BUCKET):
    'Upload a blob to Google Storage.'

    blob = create_blob(blob_pathname, bucket=bucket)
    blob.upload_from_filename(file_pathname)


def upload_data_blob(name, file_pathname, bucket=KGML_BUCKET,
                     storage=KGML_STORAGE, folder='data'):
    'Upload a data blob from the named data.'

    blob_path = data_directory_gs(name, bucket=bucket, storage=storage,
                                  folder=folder)

    logger.warning(f'Blob path: {blob_path}\nFile pathname: {file_pathname}\n')

    blob_pathname = blob_path + os.path.basename(file_pathname)
    upload_blob(blob_pathname, file_pathname)


def upload_data_blobs(name, bucket=KGML_BUCKET, storage=KGML_STORAGE,
                      folder=None):
    'Upload all data blobs for the named data.'

    # Ensure the directory exists
    ensure_model_directory_gs(name, bucket=bucket, storage=storage)

    directory = model_directory_fs(name)
    files = files_in_dir(directory)
    for file_pathname in files:
        upload_data_blob(name, file_pathname)
    return True


def upload_results_blob(name, file_pathname, bucket=KGML_BUCKET,
                        storage=KGML_STORAGE, folder='results'):
    'Upload a data blob from the named data.'

    blob_path = data_directory_gs(name, bucket=bucket, storage=storage, folder=folder)
    blob_pathname = blob_path + os.path.basename(file_pathname)
    
    upload_blob(blob_pathname, file_pathname)


# ----------------------------------------------------------
# Save Data
# -----------------------------------------------------------

def data_filename(model_name, data_name, data_type):
    return f'{model_name}-{data_name}.{data_type}'

def data_type_pathname(model_name, data_name, data_type, folder=None):
    filename = data_filename(model_name, data_name, data_type)
    return data_pathname(model_name, filename, folder=folder)


# -----------------------------------------------------------

def save_data_fs(model_name, data_name, data_type, data, folder='data'):
    'Saves data to local filesystem.'
    
    pathname = data_type_pathname(model_name, data_name, data_type,
                                  folder=folder)

    logger.warning(f'\nSaving to local filesystem:\nPathname: {pathname}\n')
          
    if data_type == 'csv':
        save_csv(data, pathname)
        
    elif data_type == 'json':
        save_dict(data, pathname)
        
    elif data_type == 'xlsx':
        write_excel(data, pathname)

    elif data_type == 'text' or data_type == 'html':
        save_text_file(data, pathname)
        
    else:
        logger.error(f'Error: Unrecognized data type {data_type}.')


# -----------------------------------------------------------

def save_data_gs(model_name, data_name, data_type, tdf, bucket=KGML_BUCKET,
                 storage=KGML_STORAGE, folder='data'):
    'Saves data to Google Storage.'

    # First save to local filesystem
    save_data_fs(model_name, data_name, data_type, tdf, folder=folder)
    # Now upload to Google storage
    file_pathname = data_type_pathname(model_name, data_name, data_type, folder=folder)
    upload_data_blob(model_name, file_pathname, bucket=bucket,
                     storage=storage, folder=folder)
    return True


# -----------------------------------------------------------

def save_data_bq(model_name, data_name, tdf, project, dataset):
    'Saves data to Google BQ.'

    if project is not None and dataset is not None:
        table_name = f'{model_name}-{data_name}'
        logger.warning(f'\nProject: {project}\nDataset: {dataset}\nTable: {table_name}\n')
        table_to_bq(tdf, project=project, dataset=dataset, table_name=table_name)
        return True
    else:
        logger.error('Error: Must specify BQ project and dataset.')
        return False


# ----------------------------------------------------------

def save_data(model_name, data_name, data_type, tdf, destination,
              project=PROJECT_ID, dataset=PROJECT_DATASET,
              bucket=KGML_BUCKET, storage=KGML_STORAGE,
              folder='data'):
    'Saves data to either local filesystem, Google Storage or Google BQ.'

    ensure_model_directory_fs(model_name, folder=folder)
    
    if destination == 'file':
        save_data_fs(model_name, data_name, data_type, tdf, folder=folder)
        
    elif destination == 'storage':
        save_data_gs(model_name, data_name, data_type, tdf,
                     bucket=bucket, storage=storage, folder=folder)
        
    elif destination == 'bq' or destination == 'db':
        save_data_bq(model_name, data_name, tdf, project, dataset)
                     
    else:
        logger.error(f'Error: Invalid destination {destination}.')


# ----------------------------------------------------------
# Load Data
# -----------------------------------------------------------

def load_data_fs(model_name, data_name, data_type, folder='data'):
    'Loads data from local filesystem.'

    pathname = data_type_pathname(model_name, data_name, data_type, folder=folder)

    logger.warning(f'\nLoading from local filesystem:\nPathname: {pathname}\n')
    if data_type == 'csv':
        return load_csv(pathname)
    
    elif data_type == 'json':
        return load_dict(pathname)
    
    elif data_type == 'xlsx':
        return read_excel(pathname)
    else:
        logger.error(f'Error: Unrecognized data type {data_type}.')
        return None


# -----------------------------------------------------------

def load_data_gs(model_name, data_name, data_type, bucket=KGML_BUCKET,
                 storage=KGML_STORAGE, folder='data'):
    'Loads data from Google Storage.'

    filename = f'{model_name}-{data_name}.{data_type}'
    file_pathname = data_pathname(model_name, filename, folder=folder)
    
    # First download from Google storage
    download_data_blob(model_name, file_pathname, bucket=bucket, storage=storage,
                       folder=folder)

    # Now load from local filesystem
    data = load_data_fs(model_name, data_name, data_type, folder=folder)

    return data


# -----------------------------------------------------------

def load_data_bq(model_name, data_name, project, dataset):
    'Loads data from Google BQ.'

    if project is not None and dataset is not None:
        table_name = f'{model_name}-{data_name}'
        table_id = bq_table_id(table_name)
        # print(f'\nBQ Table ID: {table_id}')
        query = f"SELECT * FROM {table_id}"
        df = run_pdbq(query)
        return df
    else:
        logger.error('Error: Either project or dataset was not specified.')
        return None
    

# -----------------------------------------------------------

def load_data(model_name, data_name, data_type, source,
              project=PROJECT_ID, dataset=PROJECT_DATASET,
              bucket=KGML_BUCKET, storage=KGML_STORAGE,
              folder='data'):
    'Loads data from either local filesystem, Google Storage or Google BQ.'

    ensure_model_directory_fs(model_name, folder=folder)
    
    if source == 'file':
        return load_data_fs(model_name, data_name, data_type, folder=folder)
    
    elif source == 'storage':
        return load_data_gs(model_name, data_name, data_type, bucket=bucket,
                            storage=storage, folder=folder)
    
    elif source == 'bq' or source == 'db':
        return load_data_bq(model_name, data_name, project, dataset)
    
    else:
        logger.error(f'Error: Invalid destination {source}.')
        return None


# ----------------------------------------------------------
# Save Results
# -----------------------------------------------------------

def results_type_pathname(model_name, data_name, data_type):
    filename = f'{model_name}-{data_name}.{data_type}'
    return results_pathname(model_name, filename)


def save_results_fs(model_name, results_name, data_type, results):
    'Saves data to local filesystem.'

    pathname = results_type_pathname(model_name, results_name, data_type)

    if data_type == 'csv':
        save_csv(results, pathname)
    elif data_type == 'json':
        save_dict(results, pathname)
    elif data_type == 'xlsx':
        write_excel(results, pathname)
    else:
        logger.error(f'Error: Unrecognized data type {data_type}.')


def save_results_gs(model_name, results_name, data_type, results,
                    bucket=KGML_BUCKET, storage=KGML_STORAGE):
    'Saves data to Google Storage.'

    # First save to local filesystem
    save_results_fs(model_name, results_name, data_type, results)
    
    # Now upload to Google storage
    pathname = results_type_pathname(model_name, results_name, data_type)
    upload_results_blob(model_name, pathname, bucket=bucket, storage=storage)
    
    return True


# ----------------------------------------------------------

def save_results_bq(model_name, results_name, results,
                    project=PROJECT_ID, dataset=PROJECT_DATASET):
    'Saves data to Google Big Query.'

    if project is not None and dataset is not None:
        table_name = f'{model_name}-{data_name}'
        table_to_bq(tdf, project=project, dataset=data_set, table_name=table_name)
    else:
        logger.error('Error: Must specify BQ project and dataset.')
        return False

# ----------------------------------------------------------

def save_results(model_name, results_name, data_type, results, destination,
                 project=PROJECT_ID, dataset=PROJECT_DATASET,
                 bucket=KGML_BUCKET, storage=KGML_STORAGE,
                 folder='results'):
    'Saves results to local filesystem or to Google Storage.'

    ensure_model_directory_fs(model_name, folder=folder)
    
    if destination == 'file':
        save_results_fs(model_name, results_name, data_type, results)
        
    elif destination == 'storage':
        save_results_gs(model_name, results_name, data_type, results,
                        storage=results)
        
    elif destination == 'bq' or destination == 'db':
        save_results_bq(model_name, results_name, results,
                        project=project, dataset=dataset)
    else:
        logger.error(f'Error: Invalid destination {destination}.')
        return False

    return True


# ----------------------------------------------------------
# Load Results
# -----------------------------------------------------------

def load_results_fs(model_name, results_name, data_type):
    'Loads data from local filesystem.'
    
    pathname = results_type_pathname(model_name, results_name, data_type)
    
    if data_type == 'csv':
        return load_csv(pathname)
    
    elif data_type == 'json':
        return load_dict(pathname)
    
    elif data_type == 'xlsx':
        return read_excel(pathname)
    
    elif data_type == 'text' or data_type == 'html':
        save_text_file(data, pathname)
        
    else:
        logger.error(f'Error: Unrecognized data type {data_type}.')
        return None


# -----------------------------------------------------------

def load_results_gs(model_name, results_name, data_type):
    'Loads data from Google Storage.'

    # First download from Google storage
    pathname = results_type_pathname(model_name, results_name, data_type)
    download_results_blob(model_name, pathname)
    
    # Now load from local filesystem
    results = load_results_fs(model_name, results_name, data_type)
    
    return results


# -----------------------------------------------------------

def load_results(model_name, results_name, data_type, source,
                 project=PROJECT_ID, dataset=PROJECT_DATASET,
                 bucket=KGML_BUCKET, storage=KGML_STORAGE,
                 folder='results'):
    'Loads data from either filesystem or Google Storage.'

    ensure_model_directory_fs(model_name, folder=folder)
    
    if source == 'file':
        return load_results_fs(model_name, results_name, data_type)
    
    elif source == 'storage':
        return load_results_gs(model_name, results_name, data_type,
                               bucket=bucket, storage=storage)
    
    elif source == 'bq' or source == 'db':
        return load_data_bq(model_name, data_name, project, dataset)
    
    else:
        logger.error(f'Error: Invalid destination {source}.')
        return None


# ****************************************************************
# Google Sheets 
# ****************************************************************

AUTH_FILE = os.environ['GOOGLE_APPLICATION_CREDENTIALS']

# In this code:

# We first import the necessary libraries.
# We then define a function load_google_sheet that takes two arguments: the URL of the Google Sheet and the path to your JSON key file.
# We define the scope of permissions required to access the Google Sheet and Drive APIs.
# We load the credentials from the JSON key file and authorize the client.
# We get the instance of the Google Sheet using the URL.
# We select the first worksheet of the Google Sheet.
# We retrieve all the records from the worksheet.
# We convert the list of dictionaries into a DataFrame using pd.DataFrame.
# Finally, we return the DataFrame.

def get_google_client(json_key_file=AUTH_FILE):
    # Define the scope of permissions
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]

    # Load the credentials
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_file, scope)
    
    # Authorize the clientsheet 
    return gspread.authorize(creds)


# -----------------------------------------------------------

def get_google_sheet(url: str, json_key_file=AUTH_FILE):

    # Get an authorized client
    client = get_google_client(json_key_file=json_key_file)

    # Get the instance of the Spreadsheet
    try:
    # Get the instance of the Spreadsheet
        sheet = client.open_by_url(url)
        # Get the first sheet of the spreadsheet
        return sheet.worksheet('Sheet1'), client

    except Exception as err:
        print(f'\nError accessing Google sheet:\n{url}')
        return None, client
    

# -----------------------------------------------------------

def load_google_sheet(sheet_url: str, json_key_file=AUTH_FILE):

    # Get the instance of the Spreadsheet
    worksheet, client = get_google_sheet(sheet_url, json_key_file=json_key_file)

    if worksheet is not None:
        # Get all the records of the data
        data = worksheet.get_all_records()
    
        # Convert the list of dictionaries into a DataFrame
        df = pd.DataFrame(data)
        return df
    else:
        return None


# -----------------------------------------------------------

def save_google_sheet(df, sheet_url, json_key_file=AUTH_FILE):

    # Get the instance of the Spreadsheet
    worksheet, client = get_google_sheet(sheet_url, json_key_file=json_key_file)

    # Create worksheet if needed
    if worksheet is None:
        worksheet =  worksheet.create(sheet_url)

    # Replace contents with dataframe(sheet_url
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    return True


# ****************************************************************
# Google Drive  
# ****************************************************************

def extract_folder_id(folder_url):
    # Use a regular expression to extract the folder ID from the URL
    match = re.search(r'folders/([\w-]+)', folder_url)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid folder URL")


def list_files_in_folder(folder_url, json_key_file=AUTH_FILE):

    # Get the folder id
    folder_id = extract_folder_id(folder_url)
    
    # Load the credentials
    creds = service_account.Credentials.from_service_account_file(
        json_key_file,
        scopes=['https://www.googleapis.com/auth/drive']
    )

    # Build the service
    service = build('drive', 'v3', credentials=creds)

    # Call the Drive v3 API to list the files in the folder
    results = service.files().list(
        q=f"'{folder_id}' in parents",
        fields="nextPageToken, files(id, name)"
    ).execute()

    # Get the list of files
    files = results.get('files', [])

    if not files:
        print('No files found.')
    else:
        print('Files:')
        for file in files:
            print(f'{file["name"]} ({file["id"]})')

    return files

# Usage:
# Replace 'your-folder-id' with the ID of your Google Drive folder
# Replace 'your-json-key-file.json' with the path to your JSON key file

# files = list_files_in_folder('your-folder-id', 'your-json-key-file.json')

# ****************************************************************
# End of File
# ****************************************************************
