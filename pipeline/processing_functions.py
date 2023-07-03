import glob
import logging
import os
import re
from pathlib import Path
from typing import Tuple, Union

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


def _check_tif_extension(location: Union[str, Path]) -> Path:
    """Adds .tif to raster location if it is not available

    Args:
        location (Union[str, Path]): The location of the raster

    Returns:
        Union[str, Path]: The modified location of the raster (if applicable)
    """
    if isinstance(location, str):
        location = Path(location)
    
    if not location.suffix == ".tif":
        location = location.with_suffix(".tif")

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


def get_shapefile(shp_path: Path,
                  cols_to_drop: list[str] = ['OBJECTID_1', 'Shape_Leng', 'Shape_Area', 'validOn', 'validTo', 'last_modif', 'source', 'date'],
                  lower_case: bool = True
                  ) -> gpd.GeoDataFrame:
    """Get a clean version of the shapefile for 

    Args:
        shp_path (Path): _description_

    Returns:
        gpd.GeoDataFrame: _description_
    """
    shapefile = gpd.read_file(shp_path)
    clean_shapefile = _drop_shapefile_cols(shapefile=shapefile, cols_to_drop=cols_to_drop)
    if lower_case:
        clean_shapefile = _lower_case_cols(clean_shapefile)
    else:
        clean_shapefile = shapefile

    return gpd.GeoDataFrame(clean_shapefile)


def _drop_shapefile_cols(shapefile: gpd.GeoDataFrame,
                         cols_to_drop: list[str],
                         ) -> gpd.GeoDataFrame:
    
    clean_shapefile = shapefile.drop(columns=cols_to_drop)
    return gpd.GeoDataFrame(clean_shapefile)


def _lower_case_cols(df: Union[pd.DataFrame, gpd.GeoDataFrame]) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Returns version of dataframe with lower case columns

    Args:
        df (Union[pd.DataFrame, gpd.GeoDataFrame]): Dataframe to be modified

    Returns:
        Union[pd.DataFrame, gpd.GeoDataFrame]: Modified dataframe
    """
    df.columns= df.columns.str.lower()
    return df
    

def mask_raster_with_shp(raster_location: Path, gdf: gpd.GeoDataFrame) -> Tuple[np.ndarray, Profile]:
    """Masks raster with geodataframe

    Args:
        raster_location (Path): Location of raster file. Note that rasterio mask function expects a dataset connection
        gdf (gpd.GeoDataFrame): Geodataframe that will be used to mask raster
        nodata (int, optional): Raster value that symbolizes no data. Defaults to 0.

    Returns:
        Tuple[np.ndarray, Profile]: Masked raster and masked raster profile
    """

    raster_location = _check_tif_extension(location=raster_location)

    with rasterio.open(raster_location, "r") as src:
        profile = src.profile
        gdf = _check_crs(raster=src, vector=gdf)

        masked_raster, _ = mask.mask(dataset=src, shapes=gdf.geometry, crop=True)
    
    return masked_raster, profile

def _check_crs(raster: rasterio.DatasetReader, vector: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Transform CRS of vector if CRS does not match raster

    Args:
        raster (rasterio.DatasetReader): Raster dataset reader
        vector (gpd.GeoDataFrame): Geodataframe to be mofidied if needed

    Returns:
        gpd.GeoDataFrame: _description_
    """
    if raster.crs != vector.crs:
        vector = vector.to_crs(raster.crs)
    return vector

def kelvin_to_celcius(
        df: pd.DataFrame
) -> pd.DataFrame:
    """Converts Kelvin into Celcius

    Args:
        col (int): Numeric value to convert to Celcius
    """
    #Convert from Kelvin to Celcius
    stats= ["min", "mean", "max", "median"]
    df[stats] = df[stats].apply(kelvin_to_celcius)
    
    return col - 273.15

def attribute_join(shapefile: gpd.GeoDataFrame,
                   df: pd.DataFrame,
                   ) -> pd.DataFrame:
    
    joined_df = df.join(shapefile)
     
    return joined_df


def calculate_zonal_statistics(raster_location: Path,
                               shapefile: gpd.GeoDataFrame,
                               provided_stats: str = "min mean max"
                               ) -> pd.DataFrame:
    """Calculates zonal statistics based on provided list of desired statistics

    Args:
        raster_location (Path): Path to raster. By providing path, zonal_stats function can access the profile directly
        shapefile (gpd.GeoDataFrame): Shapefile that will be the unit of analysis for zonal stats
        provided_stats (str, optional): Statistics to calculate. Defaults to "min mean max".

        results = zonal_stats(shapefile,
    Returns:
        pd.DataFrame: Tabular results, where each row is a geometry in the shapefile
    """
    raster_location = _check_tif_extension(raster_location)
    results = zonal_stats(vectors=shapefile.geometry,
                            raster=raster_location,
                            nodata=-999,
                            stats=provided_stats)
    

    return df


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
