import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os
import io
from sqlalchemy import create_engine, text
import polars as pl
from sqlalchemy.orm import sessionmaker
import csv
import requests

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
    "Healthcare Provider Taxonomy Code_1",
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
    file_map = {
        'nppes': (FILE_NAME_NPPES, 'nppes', 'blob'),
        'nucc': (FILE_NAME_NPPES_NUCC, 'nucc', 'blob'),
        'fips': (FILE_NAME_NPPES_FIPS, 'fips', 'blob'),
        'zip': (FILE_NAME_NPPES_ZIP, 'zip', 'blob'),
        'county_pop': (None, 'county_pop', 'api')
    } 
    params = file_map.get(filename)
    if not params:
        return func.HttpResponse("Invalid file type.", status_code=400)
    blob_name, tablename, source_type = params
    try:
        if source_type == 'blob':
            blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
            blob_client = blob_service_client.get_blob_client(
                container=CONTAINER_NAME,
                blob=blob_name
            )
            process_map = {
                'nppes': process_nppes_data,
                'nucc': process_data,
                'fips': process_data,
                'zip': process_data,
            }
            process_func = process_map.get(filename)
            if not process_func:
                return func.HttpResponse("Invalid file type.", status_code=400)
            result = process_func(blob_client, tablename)
        elif source_type == 'api':
            result = process_api(tablename)
        return func.HttpResponse(result, status_code=200)
    except Exception as e:
        return func.HttpResponse(
            f"An error occurred: {e}",
            status_code=500
        )
   
@app.route(route="parse_records")
def parse_records(req) -> func.HttpResponse:
    Session = sessionmaker(bind = engine)
    session = Session()
    try:
        session.execute(text("CALL createtable()"))
        session.execute(text("CALL merge_county()"))
        result = session.execute(text("SELECT * FROM view_county LIMIT 1000"))
        keys = result.keys()
        rows = result.fetchall()   
        with open("result.csv", 'w') as file:
            writer = csv.writer(file, delimiter=',')
            writer.writerow(keys)
            writer.writerows(rows)
        session.commit()
        return func.HttpResponse(
            "Successfully parsed",
            status_code=200
        )
    except Exception as e:
        session.rollback()
        return func.HttpResponse(
            f"An error occurred: {e}",
            status_code=500
        )
    finally:
        session.close()
        
@app.route(route="get_data")
def get_data(req) -> func.HttpResponse:
    try:
        process_api('county_pop')
        return func.HttpResponse(
            "Successfully parsed",
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
    return "Data processing completed"
 
 
def process_data(blob_client, tablename):
    downloader = blob_client.download_blob()
    reader = downloader.readall()
    query = pl.scan_csv(io.BytesIO(reader), ignore_errors=True)
    result_df = query.collect(streaming = True)
    result_df = result_df.fill_null("")
    result_df.columns = fix_column_names(result_df.columns)
    insert_with_pl(result_df, tablename)
    return "Data processing completed successfully."


def insert_with_pl(df:pl.DataFrame, tablename, engine= engine):
    with sessionmaker(bind=engine)() as session:
        session.execute(text(f"DROP TABLE IF EXISTS {tablename} CASCADE"))
        session.commit()
        load_data(df, table_name=tablename, engine=engine)
        session.commit()
    return f"inserted using pl write database"

def insert_using_copy_with_sqlalchemy(df:pl.DataFrame, tablename):
    with sessionmaker(bind=engine)() as session:
        session.execute(text(f"DROP TABLE IF EXISTS {tablename} CASCADE"))
        session.commit()
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
    return f"Data loaded into the database"


def process_api(tablename):
    keys_dict = {
        'get': 'NAME,B01001_001E',
        'for': 'county'
    }
    root_api = 'https://api.census.gov/data/2023/acs/acs5'
    response = requests.get(root_api, params=keys_dict)
    print(response)
    print(response.url)
    if response.status_code == 200:
        data = response.json()
        headers = data[0]
        values = data[1:]
        pop_df = pl.DataFrame(values, schema=headers)
        insert_with_pl(pop_df, tablename)
        return f"Data loaded into the database"
    else:
        return "Unauthorized access"