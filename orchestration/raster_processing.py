import io
import os
from zipfile import ZipFile

import fiona
import geopandas as gpd
import pandas as pd
import rasterio
import requests
from prefect import flow, task
from rasterio import mask
from rasterstats import zonal_stats


@task(log_prints=True, retries=0)
def write_local_raster(
    url:str,
    out_path: str, 
    month:int
    
    ) -> None:
    """Download CHELSA raster and print descriptive statistics

    Args:
        url (str): URL for raster of interest
        out_path (str): Save location of raw raster
    """
    rast_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/cmip5/2061-2080/temp/CHELSA_tas_mon_ACCESS1-0_rcp45_r1i1p1_g025.nc_{month}_2061-2080_V1.2.tif"
    raster_name = rast_url.split("/")[-1].replace(".tif", "")
    
    if os.path.exists(f"{out_path}/{raster_name}.tif") is False:
        print("Downloading raster")

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
def write_local_geometry(
    out_path: str,
    url:str,
    ) -> None:
    """Retrieves shapefile from Humanitarian Data Exchange
    and writes local shapefile

    Args:
        out_path (str): Save location of shapefile
        url (str): The URL to download the shapefile
        adm_level (str): Administrative division level
    """
    shapefile_name = url.split("/")[-1].replace(".zip", "")

    if os.path.exists(f"{out_path}/{shapefile_name}.shp") is False:
        print("Downloading shapefile")
        response = requests.get(url, allow_redirects=True, stream=True)
        response.raise_for_status()
        data = ZipFile(io.BytesIO(response.content))
        data.extractall(f"{out_path}")


@task()
def mask_raster(
    in_path: str,
    out_path: str,
    shp_path: str
    ) -> None:
    """Mask raster with shapefile
    Note mask raster works best with features from fiona

    Args:
        raw_path (str): Path to raw raster
        masked_path (str): Save location for masked raster
        shp_path (str): Path to shapefile
    """
    if os.path.exists(f"{in_path}.tif") is False:
        print("Masking raster")
    with fiona.open(f"{shp_path}") as shapefile:
        shapes = [feature["geometry"] for feature in shapefile]

    with rasterio.open(in_path, "r") as raster:
        profile = raster.profile
        out_image, out_transform = mask.mask(raster, shapes, crop=True)
    
    with rasterio.open(out_path, "w", **profile) as dest:
        dest.write(out_image)


@task(log_prints=True)
def write_zonal_statistics(
    in_path: str,
    out_path: str,
    shp_path: str,
    ) -> None:
    """Write zonal statistics to local directory

    Args:
        masked_rast (str): Path to masked raster (only area that intersects with shapefile)
        shp_path (str): Path to shapefile
        zs_path (str): Path to write zonal statistics
    """
    if os.path.exists(f"{in_path}.csv") is False:

        shapefile = gpd.read_file(f"{shp_path}")
        
        with rasterio.open(f"{in_path}", "r") as src:
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
            full_df.to_csv(f"{out_path}", index=False)

@flow()
def raster_processing_flow(month: int) -> None:
    rast_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/cmip5/2061-2080/temp/CHELSA_tas_mon_ACCESS1-0_rcp45_r1i1p1_g025.nc_{month}_2061-2080_V1.2.tif"
    raster_name = rast_url.split("/")[-1].replace(".tif", "")
    adm2_url = "https://data.humdata.org/dataset/b20cd345-93fb-43bd-9c6e-7bc7d87b63eb/resource/30b6979a-d3f3-4982-971f-dc53f076bc52/download/wca_admbnda_adm2_ocha.zip"
    shapefile_name = adm2_url.split("/")[-1].replace(".zip", "")


    write_local_geometry(
        out_path="data/adm2/",
        url=adm2_url)
    
    write_local_raster(
        url=rast_url,
        out_path=f"data/rasters/raw/{raster_name}.tif",
        month=month
        )

    mask_raster(
        in_path=f"data/rasters/raw/{raster_name}.tif",
        out_path=f"data/rasters/masked/masked_{raster_name}.tif",
        shp_path=f"data/adm2/{shapefile_name}.shp",
                )
    
    write_zonal_statistics(
        in_path=f"data/rasters/masked/masked_{raster_name}.tif",
        out_path=f"data/zonal_statistics/zs_{raster_name}.csv",
        shp_path=f"data/adm2/{shapefile_name}.shp",
    )

@flow()
def raster_processing_parent_flow(months: list[int]):
    for month in months:
        raster_processing_flow(month)


if __name__ == "__main__":
    months = [1,2,3]
    raster_processing_parent_flow(months)


