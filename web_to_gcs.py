import io
import os
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

GCS doesn't allow uploaded object be immuted. Can't write and append in chunks!
with open(local_file, 'rb') as f: ??
    blob.upload_from_filename(f) ??
status, response = request.next_chunk() ??


[Streaming Upload to GCS]: 

import pandas as pd
from io import BytesIO
file_io = BytesIO()
with open('file.csv', 'rb') as f:
   file_io.write(f.read())

# Rewind the stream to the beginning. This step can be omitted if the input
# stream will always be at a correct position.
file_io.seek(0)
# Upload data from the stream to your bucket.
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
client = storage.Client().from_service_account_json(credentials_file)
bucket = client.bucket(bucket)
blob = bucket.blob(object_name)
blob.upload_from_file(file_io)

What if file.csv.gz???? how to make it parquet?????
    csv_buffer = BytesIO()
    with gzip.open('file.csv.gz', 'rb') as f:
        csv_buffer.write(f.read())
    df = pd.read_csv(csv_buffer)
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer) 

"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET_NAME = os.environ.get("BUCKET_NAME", "provide the data-lake-bucketname")
CREDENTIALS_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "provide the credentials from gcs")


def upload_to_gcs(bucket, object_name, local_file, credentials_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client().from_service_account_json(credentials_file)
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name) # object_name is the desired name shown on gcs. 
    blob.upload_from_filename(local_file) # local_file is the local file name desired to upload to gcs. 

def web_to_gcs(year, service):
    for i in range(12):
        t0 = time.time()
        if i==0: continue
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

        # read it back into a parquet file
        df = pd.read_csv(file_name, compression='gzip')
        print(f'read csv memory usage: \n{df.memory_usage()}')
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f'to parquet memory usage: \n{df.memory_usage()}')
        print(f"Parquet: {file_name}")
        print(f'Size file = {os.path.getsize(file_name)  >> 20} MB')

        # upload it to gcs 
        upload_to_gcs(BUCKET_NAME, f"{service}/{file_name}", file_name, CREDENTIALS_FILE)
        print(f'Successfully uploaded! time spent: {time.time()-t0}')
        print(f"GCS: {service}/{file_name} \n")


# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')
web_to_gcs('2019', 'fhv')

