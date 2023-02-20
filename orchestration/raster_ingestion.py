import io
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import rasterio as rio
import requests
from prefect import flow, task

rast_url = "https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/cmip5/2061-2080/temp/CHELSA_tas_mon_ACCESS1-0_rcp85_r1i1p1_g025.nc_1_2061-2080_V1.2.tif"
adm2_url = "https://data.humdata.org/dataset/b20cd345-93fb-43bd-9c6e-7bc7d87b63eb/resource/30b6979a-d3f3-4982-971f-dc53f076bc52/download/wca_admbnda_adm2_ocha.zip"


@task(retries=3, log_prints=True)
def fetch_raster(url:str):
    """Download CHELSA raster and print descriptive statistics

    Args:
        url (str): URL for raster of interest

    Returns:
        _type_: rasterio object
    """
    with rio.open(url) as rast:
        print(type(rast))
        print(f"Number of bands: {rast.count}")
        print(f"Raster profile: {rast.profile}")
        print(f"Bounds: {rast.bounds}")
    return rast


@task(log_prints=True)
def fetch_geometry(url:str) -> ZipFile:
    """Retrieves and extracts shapefile from Humanitarian Data Exchange
    Shapefile is saved locally

    Args:
        url (str): The URL to download the shapefile

    Returns:
        save_path (str): Location of shapefile in local directory
    """
    response = requests.get(url, allow_redirects=True, stream=True)
    response.raise_for_status()

    return ZipFile(io.BytesIO(response.content))
    

def extract_geometry():
    adm2 = fetch_geometry(url=adm2_url)
    adm2.extractall("data/adm2.zip")
    
    

@task()
def mask_raster():
    # with rasterio.open("tests/data/RGB.byte.tif") as src:
    # out_image, out_transform = rasterio.mask.mask(src, shapes, crop=True)
    # out_meta = src.meta
    ...


@flow()
def main_flow():
    fetch_raster(rast_url)
    fetch_vector(adm2_url)


if __name__ == "__main__":
    main_flow()
