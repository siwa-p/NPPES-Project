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
            columns = [col for col in headers.split(",")]
            columns_to_keep_indices = [columns.index(col) for col in columns_to_keep if col in columns]
            
            data_lines = lines[1:len(lines)-2]
            # data is a list of values
            data_keep = []
            for line in data_lines:
                values = [item for item in line.split(",")]
                values_to_keep = [values[i] for i in columns_to_keep_indices]
                data_keep.append(values_to_keep)
                

            # print(data_keep)
            start, chunk_num, bytes_remaining = clear_bytes(start,lines,bytes_to_fetch,chunk_num, bytes_remaining)
            
            #engine = sqlalchemy.create_engine(POSTGRES_CONN_STR)
            #create_table_from_headers(headers, "nppes", engine)
            #load_data()sample, engine)
        elif chunk_num < last_chunk:
            data_lines = lines[0:len(lines)-2]
            
            data_keep = []
            for line in data_lines:
                values = [item for item in line.split(",")]
                values_to_keep = [values[i] for i in columns_to_keep_indices]
                data_keep.append(values_to_keep)
            
            # print(data_keep)
            #load_data()
            start, chunk_num, bytes_remaining = clear_bytes(start, lines,bytes_to_fetch, chunk_num, bytes_remaining)
        else:
            #load_data()    
            # change csv to dataframe here
            # add header to postgres table
            # add data to postgres table
            start, chunk_num, bytes_remaining = clear_bytes(start, lines,bytes_to_fetch, chunk_num, bytes_remaining)
            
    
def clear_bytes(start, lines, bytes_to_fetch, chunk_num, bytes_remaining):
    last_line = lines[-1]
    last_line_bytes = len(last_line.encode('utf-8'))
    bytes_utilized = bytes_to_fetch-last_line_bytes    
    bytes_remaining -= bytes_utilized
    start += bytes_utilized
    chunk_num+=1
    return start, chunk_num, bytes_remaining
    
    
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



