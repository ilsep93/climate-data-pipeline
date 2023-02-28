import io
from zipfile import ZipFile

import fiona
import geopandas as gpd
import pandas as pd
import rasterio
import requests
from make_gcp_blocks import gcp_zs_bucket
from prefect import task
from rasterio import mask
from rasterstats import zonal_stats


@task(log_prints=True, retries=0)
def write_local_raster(url:str, out_path: str) -> None:
    """Download CHELSA raster and print descriptive statistics

    Args:
        url (str): URL for raster of interest
        out_path (str): Save location of raw raster
    """
    with rasterio.open(url, "r") as rast:
        profile = rast.profile
        print(f"Number of bands: {rast.count}")
        print(f"Raster profile: {rast.profile}")
        print(f"Bounds: {rast.bounds}")
        print(f"Dimensions: {rast.shape}")

        raster = rast.read()

    with rasterio.open(f"{out_path}", "w", **profile) as dest:
        dest.write(raster)
        
    
@task(log_prints=True, retries=3)
def write_local_geometry(url:str, adm_level:str) -> None:
    """Retrieves shapefile from Humanitarian Data Exchange
    and writes local shapefile

    Args:
        url (str): The URL to download the shapefile
        adm_level (str): Administrative division level
    """
    response = requests.get(url, allow_redirects=True, stream=True)
    response.raise_for_status()
    data = ZipFile(io.BytesIO(response.content))
    data.extractall(f"data/{adm_level}")


@task()
def mask_raster(raw_path: str, masked_path: str, shp_path: str):
    """Mask raster with shapefile
    Note mask raster works best with features from fiona

    Args:
        raw_path (str): Path to raw raster
        masked_path (str): Save location for masked raster
        shp_path (str): Path to shapefile
    """
    
    with fiona.open(f"{shp_path}") as shapefile:
        shapes = [feature["geometry"] for feature in shapefile]

    with rasterio.open(raw_path, "r") as raster:
        profile = raster.profile
        out_image, out_transform = mask.mask(raster, shapes, crop=True)
    
    with rasterio.open(masked_path, "w", **profile) as dest:
        dest.write(out_image)


@task(log_prints=True)
def write_zonal_statistics(masked_rast: str, shp_path: str, zs_path: str) -> None:
    """Write zonal statistics to local directory

    Args:
        masked_rast (str): Path to masked raster (only area that intersects with shapefile)
        shp_path (str): Path to shapefile
        zs_path (str): Path to write zonal statistics
    """
    shapefile = gpd.read_file(f"{shp_path}")
    
    with rasterio.open(f"{masked_rast}", "r") as src:
        array = src.read(1)
        affine = src.transform
        nodata = src.nodata

        stats = zonal_stats(shapefile,
                            array,
                            nodata=nodata,
                            affine=affine,
                            stats="min mean max median",
                            geojson_out=False)

        df = pd.DataFrame(stats)
        full_df = shapefile.join(df, how="left")
        full_df.drop(['geometry', 'Shape_Leng', "Shape_Area", 'validOn', 'validTo'], axis=1, inplace=True)
        full_df.to_csv(f"{zs_path}", index=False)
