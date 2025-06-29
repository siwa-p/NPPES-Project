import azure.functions as func
from azure.storage.blob import BlobServiceClient
import logging
import os
import pandas as pd
import io
from ticktock import tick
import csv
STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
FILE_NAME = os.getenv("AZURE_STORAGE_BLOB_NAME_SAMPLE")
POSTGRES_CONN_STRING = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
TABLE_NAME = "nppes"
columns_to_keep = [
    "NPI",
    "Entity Type Code",
    "Employer Identification Number (EIN)",
    "Provider Organization Name (Legal Business Name)",
    "Provider Last Name (Legal Name)",
    "Provider First Name",
    "Provider Middle Name",
    "Provider Name Prefix Text",
    "Provider Name Suffix Text",
    "Provider Credential Text",
    "Provider Other Organization Name",
    "Provider Other Organization Name Type Code",
    "Provider Other Last Name",
    "Provider Other First Name",
    "Provider Other Middle Name",
    "Provider Other Name Prefix Text",
    "Provider Other Name Suffix Text",
    "Provider Other Credential Text",
    "Provider Other Last Name Type Code",
    "Provider First Line Business Practice Location Address",
    "Provider Second Line Business Practice Location Address",
    "Provider Business Practice Location Address City Name",
    "Provider Business Practice Location Address State Name",
    "Provider Business Practice Location Address Postal Code",
    "Provider Business Practice Location Address Country Code (If outside U.S.)",
    "Provider Business Practice Location Address Telephone Number",
    "Provider Business Practice Location Address Fax Number",
    "Provider License Number_1",
    "Provider License Number State Code_1",
    "Healthcare Provider Primary Taxonomy Switch_1",
    "Healthcare Provider Taxonomy Code_2",
    "Healthcare Provider Primary Taxonomy Switch_2",
    "Healthcare Provider Taxonomy Code_3",
    "Healthcare Provider Primary Taxonomy Switch_3",
    "Healthcare Provider Taxonomy Code_4",
    "Healthcare Provider Primary Taxonomy Switch_4",
    "Healthcare Provider Taxonomy Code_5",
    "Healthcare Provider Primary Taxonomy Switch_5",
    "Healthcare Provider Taxonomy Code_6",
    "Healthcare Provider Primary Taxonomy Switch_6",
    "Healthcare Provider Taxonomy Code_7",
    "Healthcare Provider Primary Taxonomy Switch_7",
    "Healthcare Provider Taxonomy Code_8",
    "Healthcare Provider Primary Taxonomy Switch_8",
    "Healthcare Provider Taxonomy Code_9",
    "Healthcare Provider Primary Taxonomy Switch_9",
    "Healthcare Provider Taxonomy Code_10",
    "Healthcare Provider Primary Taxonomy Switch_10",
    "Healthcare Provider Taxonomy Code_11",
    "Healthcare Provider Primary Taxonomy Switch_11",
    "Healthcare Provider Taxonomy Code_12",
    "Healthcare Provider Primary Taxonomy Switch_12",
    "Healthcare Provider Taxonomy Code_13",
    "Healthcare Provider Primary Taxonomy Switch_13",
    "Healthcare Provider Taxonomy Code_14",
    "Healthcare Provider Primary Taxonomy Switch_14",
    "Healthcare Provider Taxonomy Code_15",
    "Healthcare Provider Primary Taxonomy Switch_15"
]

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
@app.route(route="load_sample_nppes", methods=["GET", "POST"])
def load_sample_nppes(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HTTP trigger function for debugging started')
    try:    
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=FILE_NAME
        )
        process_nppes_data(blob_client)
        
        return func.HttpResponse(
            "NPPES sample data processing completed successfully.",
            status_code=200
        )
    except Exception as e:  
        return func.HttpResponse(
            f"An error occurred: {e}",
            status_code=500
        )
      
def process_nppes_data(blob_client, chunk_size=1*200*1024):
    start = 0
    blob_properties = blob_client.get_blob_properties()
    blob_size = blob_properties.size
    bytes_remaining = blob_size
    last_chunk = blob_size//chunk_size +1
    chunk_num =1 
    while bytes_remaining > 0:
        start_time = tick()
        if bytes_remaining < chunk_size:
            bytes_to_fetch = bytes_remaining
        else:
            bytes_to_fetch = chunk_size 
        downloader = blob_client.download_blob(start, bytes_to_fetch)
        blob_data = downloader.read()
        text_read = blob_data.decode('utf-8')
        lines = text_read.split("\n")
        if chunk_num < last_chunk and not text_read.endswith('\n'):
            left_over = lines.pop()
        clean_text = '\n'.join(lines)
        if chunk_num == 1:
            chunk_df = pd.read_csv(io.StringIO(clean_text), usecols = columns_to_keep, index_col=False)
            data_returned, headers = process_and_load(chunk_df)
            data_returned.to_csv(f'src/{chunk_num}_chunk.csv', index=False)
            load_header(data_returned, TABLE_NAME, POSTGRES_CONN_STRING)
            load_data(data_returned, TABLE_NAME, POSTGRES_CONN_STRING)
        else:
            if chunk_num > last_chunk:
                break
            chunk_df = pd.read_csv(io.StringIO(clean_text), header=None, names=headers, usecols = headers)
            chunk_df.to_csv(f'src/{chunk_num}_chunk.csv', index=False)
            load_data(data_returned, TABLE_NAME, POSTGRES_CONN_STRING)      
                
        logging.info(f"Chunk {chunk_num} processed with {len(data_returned)} rows.")
        start, chunk_num, bytes_remaining = clear_bytes(start, left_over, bytes_to_fetch, chunk_num, bytes_remaining)
        start_time.tock()

def process_and_load(df):
    headers = df.columns.tolist()
    headers_new = fix_column_names(headers)
    df.columns = headers_new
    return df, headers_new
    
    
def clear_bytes(start, left_over, bytes_to_fetch, chunk_num, bytes_remaining):
    left_over_bytes = len(left_over.encode('utf-8'))
    bytes_utilized = bytes_to_fetch-left_over_bytes    
    bytes_remaining -= bytes_utilized
    start += bytes_utilized
    chunk_num+=1
    return start, chunk_num, bytes_remaining


def load_header(data:pd.DataFrame, table_name:str, engine):
    data.head(0).to_sql(
        name = table_name,
        con=engine,
        if_exists='replace',
        index=False
    )
    logging.info(f"Header loaded into {table_name} table successfully.")

def load_data(data:pd.DataFrame, table_name:str, engine):
    data.to_sql(
        name = table_name,
        con=engine,
        if_exists='append',
        index=False
    )
    logging.info(f"Data loaded into {table_name} table successfully.")
    
def fix_column_names(column_name:list):
    new_column = [name.replace(' ', '_').lower().strip() for name in column_name]
    return new_column