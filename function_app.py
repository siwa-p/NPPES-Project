import azure.functions as func
from azure.storage.blob import BlobServiceClient
import logging
import test
import os
import pandas as pd
import io
import sqlalchemy
from ticktock import tick

STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
FILE_NAME = os.getenv("AZURE_STORAGE_BLOB_NAME_SAMPLE")
POSTGRES_CONN_STRING = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
TABLE_NAME = "nppes_sample"

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="load_sample_nppes", methods=["GET", "POST"])
def load_postgres(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HTTP trigger function for debugging started')
    try:    
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=FILE_NAME
        )
        #clock = tick()
        process_nppes_data(blob_client)
        #clock.tock()  
    except Exception as e:
        
        return func.HttpResponse(
            f"An error occurred: {e}",
            status_code=500
        )
        

def process_nppes_data(blob_client, chunk_size=15000):
    start = 0
    blob_properties = blob_client.get_blob_properties()
    blob_size = blob_properties.size
    bytes_remaining = blob_size
    last_chunk = blob_size//chunk_size +1
    chunk_num =1 
    while bytes_remaining > 0:
        if bytes_remaining < chunk_size:
            bytes_to_fetch = bytes_remaining
        else:
            bytes_to_fetch = chunk_size 
        downloader = blob_client.download_blob(start, bytes_to_fetch)
        data = downloader.read()
        first_chunk_text = data.decode('utf-8')
        # how many lines?
        lines  = first_chunk_text.split('\n')
        if chunk_num == 1:
            headers = lines[0]
            columns = [col.strip().replace('"', '').replace("'", "") for col in headers.split(",")]
            data_lines = lines[1:len(lines)-2]
            #engine = sqlalchemy.create_engine(POSTGRES_CONN_STR)
            #create_table_from_headers(headers, "nppes", engine)
            #load_data()sample, engine)
        elif chunk_num < last_chunk:
            data_lines = lines[0:len(lines)-2]
            #load_data()
        else:
            #load_data()    

        #df[header] to sql (add arguments to make tables for you)
        #df[data] to sql (append)

        #if len(lines)>1:
        #    if start == 0:
        #        headers = lines[0]
        #        data_lines = lines[1:len(lines)-2]
        #    else:
        #        data_lines = lines[0:len(lines)-2] # fix last line exclusion for the last chunk
                # can use something like blob_size//chunk_size to figure out chunk number
            
            
            # change csv to dataframe here
            
            # add header to postgres table
            
            # add data to postgres table
            
            
            last_line = lines[-1]
            last_line_bytes = len(last_line.encode('utf-8'))
            bytes_utilized = bytes_to_fetch-last_line_bytes    
            bytes_remaining -= bytes_utilized
            start += bytes_utilized
            
    
    # need to add the remaining data to the next chunk!
    
    
#     in chunks:
#         loads(data from blob)
#         # fix data types
#         # pands load_chunk method
#         cleans()
#         test()
#         push_to_database()


# def load_api():
#     new table
    

# def post_process(myblob: func.InputStream):
#     logging.info(f"Python blob trigger function processed blob"
#                 f"Name: {myblob.name}"
#                 f"Blob Size: {myblob.length} bytes")
    
#     stored procs()
    
#     # part 3
#     # send it to a table
    
#     merge with postgres



# def loads()
#     # from blob
# def cleans()

# def loads_db()


# local emulator - blob

# postgres - docker-containers



