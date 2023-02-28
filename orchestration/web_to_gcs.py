import os

from make_gcp_blocks import gcp_zs_bucket
from prefect import flow, task
from utils import (mask_raster, write_local_geometry, write_local_raster,
                   write_zonal_statistics)


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
    rast_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/cmip5/2061-2080/temp/CHELSA_tas_mon_ACCESS1-0_rcp45_r1i1p1_g025.nc_{month}_2061-2080_V1.2.tif"
    raster_name = rast_url.split("/")[-1].replace(".tif", "")

    adm2_url = "https://data.humdata.org/dataset/b20cd345-93fb-43bd-9c6e-7bc7d87b63eb/resource/30b6979a-d3f3-4982-971f-dc53f076bc52/download/wca_admbnda_adm2_ocha.zip"
    shapefile_name = adm2_url.split("/")[-1].replace(".zip", "")

    adm_level = "adm2"
    shp_path = f"data/{adm_level}/{shapefile_name}.shp"
    raw_path = "data/rasters/raw"
    masked_path = "data/rasters/masked"
    zs_path = "data/zonal_statistics"

    if os.path.exists(f"{raw_path}/{raster_name}.tif") is False:
        print("Downloading raster")
        write_local_raster(
            url=rast_url,
            out_path=f"{raw_path}/{raster_name}.tif"
            )

    if os.path.exists(f"data/adm2/{shapefile_name}.shp") is False:
        print("Downloading shapefile")
        write_local_geometry(url=adm2_url,
            adm_level=adm_level
            )
    
    if os.path.exists(f"{masked_path}/masked_{raster_name}.tif") is False:
        print("Masking raster")
        mask_raster(
            raw_path=f"{raw_path}/{raster_name}.tif",
            shp_path=shp_path,
            masked_path=f"{masked_path}/masked_{raster_name}.tif"
                    )

    if os.path.exists(f"{zs_path}/zs_{raster_name}.csv") is False:
        write_zonal_statistics(
            masked_rast=f"{masked_path}/masked_{raster_name}.tif",
            shp_path=f"{shp_path}",
            zs_path=f"{zs_path}/zs_{raster_name}.csv"
                                )
        write_gcs(from_path=f"{zs_path}/zs_{raster_name}.csv",
                  to_path=f"{zs_path}/zs_{raster_name}.csv")


@flow()
def etl_parent_flow(months: list[int] = [1, 2]):
    for month in months:
        etl_web_to_gcs(month)

if __name__ == "__main__":
    months = [1,2,3]
    etl_parent_flow(months)
