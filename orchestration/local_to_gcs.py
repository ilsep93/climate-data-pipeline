from make_gcp_blocks import gcp_zs_bucket
from prefect import flow, task


@task(log_prints=True)
def write_gcs(
    in_path: str,
    out_path:str
    ) -> None:
    """Upload local CSV to Google Cloud Bucket

    Args:
        in_path (str): Location of local CSV
        out_path (str): Location of GCS
    """
    gcp_zs_bucket.upload_from_path(in_path=in_path, to_path=out_path)
    print("Uploaded to GCS")


@flow(log_prints=True)
def local_to_gcs(month: int):
    rast_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/cmip5/2061-2080/temp/CHELSA_tas_mon_ACCESS1-0_rcp45_r1i1p1_g025.nc_{month}_2061-2080_V1.2.tif"
    raster_name = rast_url.split("/")[-1].replace(".tif", "")
    write_gcs(
        in_path=f"data/zonal_statistics/zs_{raster_name}.csv",
        out_path=f"zs_{raster_name}.csv"
        )

@flow()
def local_to_gcs_parent_flow(months: list[int]):
    for month in months:
        local_to_gcs(month)


if __name__ == "__main__":
    months = [1,2,3]
    local_to_gcs_parent_flow(months)
