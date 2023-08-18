import os
from pathlib import Path
from typing import Literal, Tuple, Union

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from climatology import ChelsaProduct, Month, Scenario, TemperatureProduct
from config import read_config
from rasterio import mask
from rasterio.profiles import Profile
from rasterstats import zonal_stats

config = read_config("config.json")


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


def _add_product_identifiers(
    chelsa_product: ChelsaProduct,
    place_id: str,
    df: Union[gpd.GeoDataFrame, pd.DataFrame],
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """Add product, scenario, month, and id columns to df

    Args:
        product (ChelsaProduct): _description_
        place_id (str): Column that uniquely identifies each row in the df. If none exists, one is created
        df (Union[gpd.GeoDataFrame, pd.DataFrame]): Dataframe that will receive product identifier columns

    Returns:
        Union[gpd.GeoDataFrame, pd.DataFrame]: Dataframe with product identifiers
    """
    if any(~df.duplicated(place_id)):
        place_id, df = _create_unique_place_id(df=df)

    df["product"] = chelsa_product.product.value
    df["month"] = str(chelsa_product.month.value)
    df["scenario"] = chelsa_product.scenario.value
    df["id"] = (
        df["product"]
        + "_"
        + df["scenario"]
        + "_"
        + (df["month"])
        + "_"
        + df[place_id].astype(str)
    )

    if place_id in df:
        df = df.drop(place_id, axis=1)

    return df


def _create_unique_place_id(
    df: Union[gpd.GeoDataFrame, pd.DataFrame]
) -> Tuple[str, Union[gpd.GeoDataFrame, pd.DataFrame]]:
    """Creates unique id based on iso2_code and number of geoms per iso2_code

    Args:
        df (Union[gpd.GeoDataFrame, pd.DataFrame]): Dataframe that needs unique id

    Returns:
        Tuple[str, Union[gpd.GeoDataFrame, pd.DataFrame]]: Dataframe with unique id
    """
    place_id_col = "place_id"

    df[place_id_col] = df.groupby("iso2_code").cumcount() + 1
    df[place_id_col] = df["iso2_code"] + df[place_id_col].astype(str)

    return place_id_col, df


def crop_raster_with_geometry(
    raster_location: Path, gdf: gpd.GeoDataFrame
) -> Tuple[np.ndarray, Profile]:
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
        gdf = _check_crs(dataset_reader=src, vector=gdf)

        cropped_raster, cropped_transform = mask.mask(
            dataset=src, shapes=gdf.geometry, crop=True
        )

        cropped_profile: Profile = src.profile.copy()

        cropped_profile.update(
            {
                "width": cropped_raster.shape[2],
                "height": cropped_raster.shape[1],
                "transform": cropped_transform,
            }
        )

    return cropped_raster, cropped_profile


def _check_crs(
    dataset_reader: rasterio.DatasetReader, vector: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Transform CRS of vector if CRS does not match dataset_reader

    Args:
        dataset_reader (rasterio.DatasetReader): dataset_reader dataset reader
        vector (gpd.GeoDataFrame): Geodataframe to be modified if needed

    Returns:
        gpd.GeoDataFrame: Modified geodataframe
    """

    if dataset_reader.crs != vector.crs:
        reprojected_vector = vector.to_crs(dataset_reader.crs)
        return gpd.GeoDataFrame(reprojected_vector)
    else:
        return vector


def calculate_zonal_statistics(
    raster_location: Path,
    geometry: gpd.GeoDataFrame,
    chelsa_product: ChelsaProduct,
    place_id: str,
    provided_stats: Literal["mean median min max"] = config.zonal_stats_aggregates,
) -> pd.DataFrame:
    """Calculates zonal statistics based on provided list of desired statistics

    Args:
        raster_location (Path): Path to raster. By providing path, zonal_stats function can access the profile directly
        geometry (gpd.GeoDataFrame): geometry that will be the unit of analysis for zonal stats
        month (Month): Scenario's month
        provided_stats (str, optional): Statistics to calculate. Defaults to "min mean max".

    Returns:
        pd.DataFrame: Tabular results, where each row is a geometry in the geometry
    """
    raster_location = _check_tif_extension(raster_location)
    results = zonal_stats(
        vectors=geometry.geometry,
        raster=raster_location,
        nodata=-999,
        stats=provided_stats,
    )

    stats_list = provided_stats.split(" ")
    for stat in stats_list:
        column_name = f"{stat}_raw"
        geometry[column_name] = [result[stat] for result in results]

    geometry.drop(columns=["geometry"], inplace=True)
    geometry_with_ids = _add_product_identifiers(
        chelsa_product=chelsa_product, place_id=place_id, df=geometry
    )

    return geometry_with_ids


def _monthly_temperature_conversion(temperature: float) -> float:
    """Monthly climatologies are in C/10 units
    https://chelsa-climate.org/wp-admin/download-page/CHELSA_tech_specification.pdf (pg.36)

    Args:
        temperature (float): Numeric value to convert to Celcius
    """
    return temperature / 10


def _check_temperature_converter(
    product: ChelsaProduct,
    df: pd.DataFrame,
    provided_stats: str = config.zonal_stats_aggregates,
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
            df[celsius_column_name] = df[raw_column_name].apply(
                _monthly_temperature_conversion
            )

    return df


def yearly_table_generator(
    product: ChelsaProduct, zonal_dir: Path, sort_values: list[str]
) -> pd.DataFrame:
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
    yearly_table = pd.DataFrame()
    for file in os.listdir(zonal_dir):
        if file.endswith(".csv"):
            with open(f"{os.path.join(zonal_dir, file)}", "r") as f:
                df = pd.read_csv(f, index_col=None, header=0, encoding="utf-8")
                li.append(df)
                yearly_table = pd.concat(li, axis=0, ignore_index=True)

    yearly_table = _check_temperature_converter(product=product, df=yearly_table)
    yearly_table.sort_values(by=sort_values, inplace=True)

    return yearly_table
