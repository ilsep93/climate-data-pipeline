import glob
import logging
import os
import re
from pathlib import Path
from typing import Tuple, Union

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio import mask
from rasterio.profiles import Profile
from rasterstats import zonal_stats

logger = logging.getLogger(__name__)
logging.basicConfig(filename='processing_logger.log', encoding='utf-8', level=logging.DEBUG)


def raster_description(profile: Profile):
    """Print description of raster

    Args:
        rast (rasterio): Raster profile
    """
    logger.info(f"CRS: {profile['crs']}")
    logger.info(f"Band Count: {profile['count']}")
    logger.info(f"Affine: {profile['transform']}")


def read_raster(location: Union[str, Path]) -> Tuple[np.ndarray, Profile]:
    """Read a raster from a URL or path provided as a string

    Args:
        url (str): Link used to download .tif file

    Returns:
        raster: np.ndarray with raster values
        profile: rasterio raster profile
    """

    location = _check_tif_extension(location)
    with rasterio.open(location, "r") as rast:
            raster = rast.read()
            profile = rast.profile
    return raster, profile


def _check_tif_extension(location: Union[str, Path]) -> Union[str, Path]:
    """Adds .tif to raster location if it is not available

    Args:
        location (Union[str, Path]): The location of the raster

    Returns:
        Union[str, Path]: The modified location of the raster (if applicable)
    """
    if isinstance(location, Path) and not location.suffix == ".tif":
        location = location.with_suffix(".tif")
    
    if isinstance(location, str) and not location.endswith(".tif"):
        location = os.path.join(location + ".tif")
    
    return location
    

def write_local_raster(raster: np.ndarray, profile: Profile, out_path: Path) -> None:
    """Write .tif file to specified location

    Args:
        raster (np.ndarray): Raster object, given as a numpy ndarray
        profile (Profile): The raster's profile
        out_path (Path): The location where the raster will be saved
    """
    out_path = _check_tif_extension(location=out_path)
    with rasterio.open(out_path, "w", **profile) as dest:
        dest.write(raster)


def get_shapefile(shp_path: Path, cols_to_drop: list[str], lower_case: bool = True) -> gpd.GeoDataFrame:
    """Get a clean version of the shapefile for 

    Args:
        shp_path (Path): _description_

    Returns:
        gpd.GeoDataFrame: _description_
    """
    shapefile = gpd.read_file(shp_path)
    clean_shapefile = _drop_shapefile_cols(shapefile=shapefile)
    clean_shapefile = _lower_case_cols(clean_shapefile)

    return clean_shapefile


def _drop_shapefile_cols(shapefile: gpd.GeoDataFrame,
                         cols_to_drop: List[str] = ['OBJECTID_1', 'Shape_Leng', 'Shape_Area', 'validOn', 'validTo', 'last_modif', 'source', 'date']
                         ) -> gpd.GeoDataFrame:
    
    clean_shapefile = shapefile.drop(columns=cols_to_drop)
    return clean_shapefile


def _lower_case_cols(df: Union[pd.DataFrame, gpd.GeoDataFrame]) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Returns version of dataframe with lower case columns

    Args:
        df (Union[pd.DataFrame, gpd.GeoDataFrame]): Dataframe to be modified

    Returns:
        Union[pd.DataFrame, gpd.GeoDataFrame]: Modified dataframe
    """
    df.columns= df.columns.str.lower()
    return df
    

def mask_raster_with_shp(raster_location: Path, gdf: gpd.GeoDataFrame, nodata: int = 0) -> Tuple[np.ndarray, Profile]:
    """Masks raster with geodataframe

    Args:
        raster_location (Path): Location of raster file. Note that rasterio mask function expects a dataset connection
        gdf (gpd.GeoDataFrame): Geodataframe that will be used to mask raster
        nodata (int, optional): Raster value that symbolizes no data. Defaults to 0.

    Returns:
        Tuple[np.ndarray, Profile]: Masked raster and masked raster profile
    """
    # with fiona.open(f"{shp_path}") as shapefile:
    #         shapes = [feature["geometry"] for feature in shapefile]
        
    masked_raster, _ = mask.mask(raster, shapefile, crop=True)
    
    return masked_raster
    

def kelvin_to_celcius(
        col: int
) -> float:
    """Converts Kelvin into Celcius

    Args:
        col (int): Numeric value to convert to Celcius
    """
    return col - 273.15

def write_zonal_statistics(
    self,
    shp_path: str = "data/adm2/wca_admbnda_adm2_ocha.shp",
    ) -> None:
    """Write zonal statistics to local directory

    Args:
        shp_path (str): Path to shapefile
    """

    shapefile = gpd.read_file(f"{shp_path}")
    for file in os.listdir(self.masked_raster):
        if file.endswith(".tif"):
            with rasterio.open(f"{self.masked_raster}/{file}", "r") as src:
                array = src.read(1)
                affine = src.transform
                nodata = src.nodata

                zs = zonal_stats(shapefile,
                                    array,
                                    affine=affine,
                                    nodata=nodata,
                                    stats="min mean max median",
                                    geojson_out=False)

                #Attribute join between shapefile and zonal stats
                df = pd.DataFrame(zs)
                full_df = shapefile.join(df, how="left")
                full_df.drop(['geometry', 'Shape_Leng', "Shape_Area", 'validOn', 'validTo'], axis=1, inplace=True)
                
                #Convert from Kelvin to Celcius
                stats= ["min", "mean", "max", "median"]
                full_df[stats] = full_df[stats].apply(self.kelvin_to_celcius)
                
                #Export as CSV
                file = file.replace(".tif", ".csv")
                file = file.replace("msk_", "zs_")
                full_df.to_csv(f"{self.zonal_statistics}/{file}", index=False)

def climatology_yearly_table_generator(
    self,
):
    """Aggregates monthly climatology predictions into a yearly table.
    Processing steps: 
    * Add month int month column (1-12)
    * Sort values by administrative identifier and month
    """
    if not os.path.exists(f"{self.time_series}/{self.climatology}_yearly.csv"):
        zs_files = glob.glob(os.path.join(self.zonal_statistics, '*.csv'))
        
        li = []
        logger.info(f"Creating a yearly dataset for {self.climatology}")

        for file in zs_files:
            with open(f"{file}", 'r') as f:
                month = re.search('_\d{1,2}', file).group(0)
                month = month.replace("_", "")
                df = pd.read_csv(f, index_col=None, header=0)
                df['month'] = int(month)
                li.append(df)

            data = pd.concat(li, axis=0, ignore_index=True)
            data.sort_values(by=["OBJECTID_1", "month"], inplace=True)
            data.to_csv(f"{self.time_series}/{self.climatology}_yearly.csv", index=False)
            
    else:
        logger.info(f"Yearly time appended dataset exists for {self.climatology}")
