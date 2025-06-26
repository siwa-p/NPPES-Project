import azure.functions as func
import logging
import test

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="nppesfunction",
                               connection="828825_STORAGE") 
def load_postgres(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    
    
    in chunks:
        loads(data from blob)
        # fix data types
        # pands load_chunk method
        cleans()
        test()
        push_to_database()


def load_api():
    new table
    

def post_process(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    
    stored procs()
    
    # part 3
    # send it to a table
    
    merge with postgres



def loads()
    # from blob
def cleans()

def loads_db()


local emulator - blob

postgres - docker-containers

