import os

from make_gcp_blocks import gcp_zs_bucket
from prefect import flow, task


@task(log_prints=True)
def write_gcs(from_path: str, to_path:str) -> None:
    """Upload local CSV to Google Cloud Bucket

    Args:
        from_path (str): Location of local CSV
        to_path (str): Location of GCS
    """
    gcp_zs_bucket.upload_from_path(from_path=from_path, to_path=to_path)
    print("Uploaded to GCS")

@flow(log_prints=True)
def etl_web_to_gcs(month: int):
        write_gcs(from_path=f"{zs_path}/zs_{raster_name}.csv",
                  to_path=f"{zs_path}/zs_{raster_name}.csv")


@flow()
def etl_parent_flow(months: list[int] = [1, 2]):
    for month in months:
        etl_web_to_gcs(month)

if __name__ == "__main__":
    months = [1,2,3]
    etl_parent_flow(months)
