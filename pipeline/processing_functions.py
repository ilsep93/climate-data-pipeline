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
from climatology import ChelsaProduct, Month, TemperatureProduct
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
    """Get a clean version of the shapefile

    Args:
        shp_path (Path): Path to .shp

    Returns:
        gpd.GeoDataFrame: Shapefile without columns to drop, with columns in lowercase
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
        Union[pd.DataFrame, gpd.GeoDataFrame]: Modified dataframe with lower case columns
    """
    df.columns= df.columns.str.lower()
    return df


def _add_month_to_df(month: Month,
                     df: Union[gpd.GeoDataFrame, pd.DataFrame]
                     ) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    df["month"] = month.value
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
        vector (gpd.GeoDataFrame): Geodataframe to be modified if needed

    Returns:
        gpd.GeoDataFrame: Modified geodataframe
    """

    if raster.crs != vector.crs:
        reprojected_vector = vector.to_crs(raster.crs)
        return gpd.GeoDataFrame(reprojected_vector)
    else:
        return vector
    

def attribute_join(shapefile: gpd.GeoDataFrame,
                   df: pd.DataFrame,
                   ) -> pd.DataFrame:
    
    joined_df = df.join(shapefile)
     
    return joined_df


def calculate_zonal_statistics(raster_location: Path,
                               shapefile: gpd.GeoDataFrame,
                               month: Month,
                               provided_stats: str = "min mean max",
                               ) -> pd.DataFrame:
    """Calculates zonal statistics based on provided list of desired statistics

    Args:
        raster_location (Path): Path to raster. By providing path, zonal_stats function can access the profile directly
        shapefile (gpd.GeoDataFrame): Shapefile that will be the unit of analysis for zonal stats
        month (Month): Scenario's month
        provided_stats (str, optional): Statistics to calculate. Defaults to "min mean max".

    Returns:
        pd.DataFrame: Tabular results, where each row is a geometry in the shapefile
    """
    raster_location = _check_tif_extension(raster_location)
    results = zonal_stats(vectors=shapefile.geometry,
                            raster=raster_location,
                            nodata=-999,
                            stats=provided_stats)
    
    stats_list= provided_stats.split(" ")
    for stat in stats_list:
        column_name = f"{stat}_raw_value"
        shapefile[column_name] = [result[stat] for result in results]
    
    shapefile.drop(columns=['geometry'], inplace=True)
    shapefile_with_month = _add_month_to_df(month=month, df=shapefile)

    return shapefile_with_month

def _monthly_temperature_conversion(temperature: float) -> float:
    """Monthly climatologies are in C/10 units
    https://chelsa-climate.org/wp-admin/download-page/CHELSA_tech_specification.pdf (pg.36)

    Args:
        temperature (float): Numeric value to convert to Celcius
    """
    return temperature / 10


def _check_temperature_converter(product:ChelsaProduct,
                                 df: pd.DataFrame,
                                 provided_stats: str = "min mean max"
                                 ) -> pd.DataFrame:
    """Checks if product is a temperature product. If so, new column is created with celsius values.

    Args:
        product (ChelsaProduct): Instance of any ChelsaProduct
        df (pd.DataFrame): Dataframe that with values that will be checked and converted to C
        provided_stats (str, optional): Columns with C/10 temperatures to be converted to C. Defaults to "min mean max".

    Returns:
        pd.DataFrame: _description_
    """
    
    if type(product).__name__ in [tp.name for tp in TemperatureProduct]:
        cols_to_convert_celsius = provided_stats.split(" ")
        for stat in cols_to_convert_celsius:
            raw_column_name = f"{stat}_raw_value"
            celsius_column_name = f"{stat}_celsius_value"
            df[celsius_column_name] = df[raw_column_name].apply(_monthly_temperature_conversion)
    
    return df


def yearly_table_generator(product: ChelsaProduct, zonal_dir: Path, sort_values: list[str]) -> pd.DataFrame:
    """Iterates through CSV files in zonal directory and appends them together to create a yearly table,
    with one row per month.

    Args:
        product (ChelsaProduct): Type of CHELSA product. Used to determine raw value conversion
        zonal_dir (Path): Path where list of zonal stat files can be found
        sort_values (list[str]): Columns used to sort the yearly dataframe

    Returns:
        pd.DataFrame: Yearly table
    """
        
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
