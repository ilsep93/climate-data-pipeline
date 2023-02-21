import io
import os
from pathlib import Path
from zipfile import ZipFile

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import requests
from prefect import flow, task
from rasterio import mask
from rasterstats import zonal_stats

rast_url = "https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/cmip5/2061-2080/temp/CHELSA_tas_mon_ACCESS1-0_rcp85_r1i1p1_g025.nc_1_2061-2080_V1.2.tif"
raster_name = rast_url.split("/")[-1]

adm2_url = "https://data.humdata.org/dataset/b20cd345-93fb-43bd-9c6e-7bc7d87b63eb/resource/30b6979a-d3f3-4982-971f-dc53f076bc52/download/wca_admbnda_adm2_ocha.zip"
shapefile_name = adm2_url.split("/")[-1]
shapefile_name = shapefile_name.replace(".zip", "")

@task(log_prints=True, retries=0)
def write_local_raster(url:str, raster_inpath: str) -> None:
    """Download CHELSA raster and print descriptive statistics

    Args:
        url (str): URL for raster of interest
    """
    with rasterio.open(url, "r") as rast:
        profile = rast.profile

        print(f"Number of bands: {rast.count}")
        print(f"Raster profile: {rast.profile}")
        print(f"Bounds: {rast.bounds}")
        print(f"Dimensions: {rast.shape}")

        raster = rast.read()
        print(type(raster))

    with rasterio.open(f"{raster_inpath}/{raster_name}", "w", **profile) as dest:
        print(dest)
        dest.write(raster)
        
    
@task(log_prints=True, retries=3)
def fetch_geometry(url:str) -> ZipFile:
    """Retrieves shapefile from Humanitarian Data Exchange

    Args:
        url (str): The URL to download the shapefile

    Returns:
        response.content (ZipFile): Zipped shapefile
    """
    response = requests.get(url, allow_redirects=True, stream=True)
    response.raise_for_status()

    return ZipFile(io.BytesIO(response.content))
    
@task(log_prints=True)
def extract_geometry(zip: ZipFile, adm_level:str) -> gpd.GeoDataFrame:
    """Saves shapefile locally and returns geodataframe

    Args:
        adm_level (str): Level of administrative division.
        Eg. "adm1", "adm2", "adm3"

    Returns:
        gpd.GeoDataFrame: Vector file with all available attributes
    """
    shapefile_name = adm2_url.split("/")[-1]
    shapefile_name = shapefile_name.replace(".zip", "")
    zip.extractall(f"data/{adm_level}")
    return gpd.read_file(f"data/{adm_level}/{shapefile_name}.shp")
    
@task()
def mask_raster(raster_inpath: str, raster_outpath: str, adm_level: str):
    """Mask raster with shapefile

    Args:
        raster_inpath (str): Location of raw raster
        raster_outpath (str): Save location for masked raster
        adm_level (str): ADM level (eg. "adm2")
    """
    
    with fiona.open(f"data/{adm_level}/{shapefile_name}.shp") as shapefile:
        shapes = [feature["geometry"] for feature in shapefile]

    with rasterio.open(raster_inpath, "r") as raster:
        print(raster.crs)
        profile = raster.profile
        out_image, out_transform = rasterio.mask.mask(raster, shapes, crop=True)
    
    with rasterio.open(raster_outpath, "w", **profile) as dest:
        dest.write(out_image)
        

@task(log_prints=True)
def create_zonal_statistics(masked_rast: str, zs_path: str, shp_path: str) -> None:
    
    with rasterio.open(f"{masked_rast}", "r") as src:
        array = src.read(1)
        affine = src.transform
        nodata = src.nodata

        stats = zonal_stats(f"{shp_path}",
                            array,
                            nodata=nodata,
                            affine=affine,
                            stats="count min mean max median",
                            geojson_out=True)
        stats = pd.DataFrame(stats)
        stats.to_csv(f"{zs_path}", index=False)

@flow(log_prints=True)
def main_flow():
    adm_level = "adm2"
    raster_inpath = "data/rasters/raw"
    raster_outpath = "data/rasters/masked"

    if os.path.exists(f"data/rasters/raw/{raster_name}") is False:
        print("Downloading raster")
        write_local_raster(rast_url, raster_inpath=raster_inpath)

    if os.path.exists(f"data/adm2/{shapefile_name}.shp") is False:
        print("Downloading shapefile")
        adm_zip = fetch_geometry(url=adm2_url)
        shapefile = extract_geometry(zip=adm_zip, adm_level="adm2")
    
    if os.path.exists(f"data/rasters/masked/_masked{raster_name}") is False:
        print("Masking raster")
        mask_raster(raster_inpath=f"{raster_inpath}/{raster_name}",
                    raster_outpath=f"{raster_outpath}/masked_{raster_name}",
                    adm_level = adm_level)
    if os.path.exists(f"{zs_path}/zs_{raster_name}") is False:
        create_zonal_statistics(
            masked_rast=f"{masked_path}/masked_{raster_name}.tif",
            shp_path=f"{shp_path}",
            zs_path=f"{zs_path}/zs_{raster_name}.csv"
                                )


if __name__ == "__main__":
    main_flow()
