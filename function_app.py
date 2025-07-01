import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os
import io
from sqlalchemy import create_engine
import polars as pl
from sqlalchemy.orm import sessionmaker
import pandas as pd

STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")

FILE_NAME_NPPES = os.getenv("AZURE_STORAGE_BLOB_NAME_NPPES")
FILE_NAME_NPPES_SAMPLE = os.getenv("AZURE_STORAGE_BLOB_NAME_NPPES_SAMPLE")
FILE_NAME_NPPES_NUCC = os.getenv("AZURE_STORAGE_BLOB_NAME_NUCC")
FILE_NAME_NPPES_FIPS = os.getenv("AZURE_STORAGE_BLOB_NAME_FIPS")
FILE_NAME_NPPES_ZIP = os.getenv("AZURE_STORAGE_BLOB_NAME_ZIP")

POSTGRES_CONN_STRING = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"

engine = create_engine(POSTGRES_CONN_STRING)

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
@app.route(route="load_nppes", auth_level=func.AuthLevel.ANONYMOUS)
def load_nppes(req: func.HttpRequest) -> func.HttpResponse:
    filename = req.params.get('file')
    if filename == 'nppes':
        blob_name = FILE_NAME_NPPES
        tablename = 'nppes'
    elif filename == 'nucc':
        blob_name = FILE_NAME_NPPES_NUCC
        tablename = 'nucc'
    elif filename == 'fips':
        blob_name = FILE_NAME_NPPES_FIPS
        tablename = 'fips'
    elif filename == 'zip':
        blob_name = FILE_NAME_NPPES_ZIP
        tablename = 'zip'
    else:
        return FileNotFoundError
    try:    
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=blob_name
        )
        if filename == 'nppes':
            process_nppes_data(blob_client, tablename)
        elif filename == 'nucc':
            process_data(blob_client, tablename)
            # pass
        elif filename == 'fips':
            process_data(blob_client, tablename)
            # pass
        elif filename == 'zip':
            process_data(blob_client, tablename)           
            # pass
        else:
            return func.HttpResponse("Invalid file type.", status_code=400)
        
        return func.HttpResponse(
            "NPPES sample data processing completed successfully.",
            status_code=200
        )
    except Exception as e:  
        return func.HttpResponse(
            f"An error occurred: {e}",
            status_code=500
        )
        

def process_nppes_data(blob_client, tablename):
    downloader = blob_client.download_blob()
    reader = downloader.readall()
    query = pl.scan_csv(io.BytesIO(reader), ignore_errors=True)
    query = query.select(columns_to_keep)
    result_df = query.collect(streaming = True)
    result_df.columns = fix_column_names(result_df.columns)
    insert_using_copy_with_sqlalchemy(result_df, tablename)
 
 
def process_data(blob_client, tablename):
    downloader = blob_client.download_blob()
    reader = downloader.readall()
    query = pl.scan_csv(io.BytesIO(reader), ignore_errors=True)
    result_df = query.collect(streaming = True)
    result_df = result_df.fill_null("")
    result_df.columns = fix_column_names(result_df.columns)
    insert_with_pl(result_df, tablename)


def insert_with_pl(df:pl.DataFrame, tablename, engine= engine):
    with sessionmaker(bind=engine)() as session:
        load_data(df, table_name=tablename, engine=engine)
        session.commit()
    return f"inserted using pl write database"

def insert_using_copy_with_sqlalchemy(df:pl.DataFrame, tablename):
    with sessionmaker(bind=engine)() as session:
        load_header(df, table_name=tablename, engine=engine)
        with session.connection().connection.cursor() as cursor:
            with io.StringIO() as buffer:
                df.write_csv(buffer, include_header=False)
                buffer.seek(0)
                cursor.copy_expert(
                    f"COPY {tablename} ({','.join(df.columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')", buffer
                )
        session.commit()
    return f"insert using copy with sqlalchemy completed"

def fix_column_names(cols):
    return [
        c.lower()
         .strip()
         .replace(' ', '_')
         .replace('(', '')
         .replace(')', '')
         .replace('.', '')
         .replace('-', '_')
         .replace('/', '_')
        for c in cols
    ]

def load_header(data:pl.DataFrame, table_name:str, engine):
    data.head(0).write_database(
        table_name = table_name,
        connection =engine,
        if_table_exists='replace'
    )
    
def load_data(data:pl.DataFrame, table_name:str, engine):
    data.write_database(
        table_name = table_name,
        connection =engine,
        if_table_exists='replace'
    )