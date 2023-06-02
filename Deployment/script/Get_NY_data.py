import pandas as pd
import requests
import os
import pyarrow.parquet as pq
import io
from memory_profiler import profile
from helper.Storedata import Storedata

@profile
def fetch_NY_Data(year:int,writeobj:object,database_write=True):
    url =  f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-01.parquet"
    
    response = requests.get(url, stream=True,verify = False) #verify = False to get around self signed certificate error, need to see how at self signed certificates to trusted certificates 
    chunks = []

    # Process the response content in chunks
    for chunk in response.iter_content(chunk_size=4096):
        if chunk:
            chunks.append(chunk)

    #create a byte file from the chunks
    parquet_content = b"".join(chunks)
    #converting the byte file to a file like object
    parquet_buffer = io.BytesIO(parquet_content)
    parent_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir,'data'))

    #Direct reading of parquet file using read_parquet_file leads to high memory consumption as observed from docker-stats 
    #df = pd.read_parquet(parquet_file)

    #Set up the file pointer to Parquet object
    parquet_file = pq.ParquetFile(parquet_buffer)
    batch_size = 1024 #Experiment for performance 
    batches = parquet_file.iter_batches(batch_size) #batches will be a generator
    file_name = None
    
    cnt= 0
    for batch in batches:
        #need to check if to_pandas is required
        df = batch.to_pandas()
        if database_write:        
            try:
                writeobj.write_data_to_db(df)
            except Exception as e:
                print (f'Error: {e}')
                return e
    
        #Construct the file name
        file_name = os.path.join(parent_dir, f"{year}_{cnt}.parquet") 
        try:
            writeobj.write_file_to_path(df,file_name)
        except Exception as e:
            print(f"Error writing: {e}")
            return e
        cnt = cnt+1

      
if __name__ == "__main__":
    storedata = Storedata()
    fetch_NY_Data(2022,storedata,database_write = False)

