import requests
from datetime import timedelta
from prefect_gcp.cloud_storage import GcsBucket
from prefect import task, flow
from prefect.tasks import task_input_hash

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_file(url: str, file_path: str):
    response = requests.get(url)
    open(file_path, "wb").write(response.content)
    return file_path

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
    public_url = upload_to_gcs(file_path=file_path, bucket_block=gcp_cloud_storage_bucket_block, blob_name=f'fhv_tripdata_{year}-{month:02}.csv.gz')

@flow()
def etl_parent_flow(months: list[int] = [1, 2], year: int = 2019) -> None:
    for month in months:
        etl_web_to_gcs(year, month)

if __name__ == '__main__':
    months = [1,2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    year = 2019
    etl_parent_flow(months, year)