# Pipeline for downloading FHV data trip, Convert to a parquet, then upload to Google Cloud Storage.
import requests
from datetime import timedelta
from prefect_gcp.cloud_storage import GcsBucket
from prefect import task, flow
from prefect.tasks import task_input_hash
import pandas as pd 
import gzip

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_file(url: str, file_path: str):
    response = requests.get(url)
    open(file_path, "wb").write(response.content)
    return file_path

@task
def convert_to_df(file_path: str):
    with gzip.open(file_path, "rb") as f:
        import IPython; IPython.embed()
        return pd.read_csv(f)

@task 
def convert_to_parquet(file_path: str, df):
    parquet_file = df.to_parquet(file_path, compression='gzip')
    import IPython; IPython.embed()
    return parquet_file
@task
def upload_to_gcs(file_path: str, bucket_block: GcsBucket, blob_name: str):
    bucket = bucket_block.get_bucket()
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)
    return blob.public_url

@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    file_path = download_file(url=f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz', file_path=f'fhv_tripdata_{year}-{month:02}.csv.gz')
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoomcamp-gcs")
    df = convert_to_df(file_path=file_path)
    converted_file = convert_to_parquet(file_path=file_path, df=df)
    
    public_url = upload_to_gcs(file_path=converted_file, bucket_block=gcp_cloud_storage_bucket_block, blob_name=f'fhv_tripdata_{year}-{month:02}.csv.gz')

@flow()
def etl_parent_flow(months: list[int] = [1, 2], year: int = 2019) -> None:
    for month in months:
        etl_web_to_gcs(year, month)

if __name__ == '__main__':
    months = [1,2]
    year = 2019
    etl_parent_flow(months, year)