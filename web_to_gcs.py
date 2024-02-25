import io
import os, sys
import requests
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv
import time

load_dotenv()

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET


[I WANT TO UPLOAD HUGE LOCAL PARQUET FILE TO GCS (as a single parquet file)]: 
Other than fhv_tripdata_2019-01.csv.gz, all months of 2019 fhv dataset were succesfully uploaded. 
fhv_tripdata_2019-01.csv.gz is too large. 

Thought 1.
Reading from pandas dataframe chunk-by-chunk to avoid loading whole dataset to memory. 
    => but google cloud storage bucket isn't immutable, 
        we can't "append" (aka modify) to the gcs file after uploading to gcs. 

Thought 2.
Streaming Upload with BytesIO?
    put chunks together into a single BytesIO stream, and stream to gcs. 
    => For csv, 
        implemented csv streaming for a few chunks, but killed(OOM) for streaming all the chunks with same single BytesIO.  
    => For parquet, 
        streaming in parquet is difficult.
    P.S. googlebigquery doesn't allow multiple formats when creating single external table. 
        either every bucket files are all parquet or all csv. 

Thought 3.
Upload parquet chunk-by-chunk into different filenames under a same folder fhv_tripdata_2019-01. 
and create external tables all at once specifying all the gcs parquet files. 

"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET_NAME = os.environ.get("BUCKET_NAME", "provide the data-lake-bucketname")
CREDENTIALS_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "provide the credentials from gcs")

# service = fhv
service_dtypes = {
    'dispatching_base_num': str, 
    'pickup_datetime': str, 
    'dropOff_datetime': str, 
    'PUlocationID': float, 
    'DOlocationID': float,
    'SR_Flag': float,
    'Affiliated_base_number': str
}


# Stream upload it to gcs 
def upload_to_gcs(bucket, object_name, buffer, credentials_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
        https://cloud.google.com/storage/docs/streaming-uploads#client-libraries 
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client().from_service_account_json(credentials_file)
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name) # object_name is the desired name shown on gcs. 
    buffer.seek(0)
    blob.upload_from_file(buffer) # the buffer for stream upload to gcs. 


def web_to_gcs(year, service):
    tt0 = time.time()
    for i in range(12):
        t0 = time.time()
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        if not os.path.exists(file_name):
            # download it using requests via a pandas df
            request_url = f"{init_url}{service}/{file_name}"
            r = requests.get(request_url)
            open(file_name, 'wb').write(r.content)
            print(f"Local: {file_name}")
        else: 
            print(f"Local (already downloaded): {file_name}")

        # parquet simple chunking
        df_iter = pd.read_csv(file_name, compression='gzip', iterator=True, chunksize=100000, dtype=service_dtypes)
        df_len = 0
        df_memsize = 0
        for i, df in enumerate(df_iter):
            # print(f'CHUNK {i}\nread csv memory usage: \n{df.memory_usage()}')
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow')
            df_len+=len(df)
            df_memsize+=sys.getsizeof(parquet_buffer) >> 20

            # file_name = file_name.replace('.csv.gz', '.parquet')
            folder_name = file_name[:file_name.find('.')]
            parquet_buffer.seek(0)
            upload_to_gcs(BUCKET_NAME, f"{service}/{folder_name}/chunk{i}.parquet", parquet_buffer, CREDENTIALS_FILE)
            print(f'{year}-{month} chunk{i}.parquet uploaded')
            print(f'row count: {df_len}')
            print(f'memsize: {df_memsize} MB')

        print(f'SUCCESSFULLY uploaded to GCS: {service}/{folder_name}')
        print(f'TOTAL of {df_len} Rows, {df_memsize} MB')
        print(f'DONE in time: {time.time() - t0} secs!!\n\n\n')
    print(f'FINISHED ALL IN {time.time()-tt0} secs')


# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')
web_to_gcs('2019', 'fhv')

