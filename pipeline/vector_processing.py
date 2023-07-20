import logging
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)

COLUMN_MAPPING = {
    'admin0pcod': 'iso2_code',
    'admin0name': 'adm0_name',
    'admin1name': 'adm1_name',
    'admin2name': 'adm2_name',
    'admin1pcod': 'adm1_id',
    'admin2pcod': 'adm2_id',
}

def get_geometry(geom_path: Path,
                 column_mapping: dict,
                 lower_case: bool = True,
                 cols_to_drop: Optional[list[str]] = ['OBJECTID_1', 'Shape_Leng', 'Shape_Area', 'validOn', 'validTo', 'last_modif', 'source', 'date'],
                  ) -> gpd.GeoDataFrame:
    """Reads geometry, drops unnecessary columns, 
    transforms to lower case (optional), and renames columns
    to align with database standards.

    Args:
        geom_path (Path): Path to .shp
        cols_to_drop (List[str]): Columns to drop from the shapefile
        lower_case (bool): Convert column names to lowercase if True

    Returns:
        gpd.GeoDataFrame: Shapefile without specified columns, with optional lowercase column names
    """
    geometry = gpd.read_file(geom_path)
    geometry.drop(columns=cols_to_drop, inplace=True)
    if lower_case:
        geometry.columns = map(str.lower, geometry.columns)
    mapped_geoms = _rename_geometry(geom=geometry, column_mapping=column_mapping)

    logger.info(f"Geometry columns: {mapped_geoms.columns}")
  
    return mapped_geoms



def _rename_geometry(geom: gpd.GeoDataFrame, column_mapping: dict) -> gpd.GeoDataFrame:
    """Rename geometries based on column mapping to align with database standards.

    Args:
        geom (gpd.GeoDataFrame): Geometry to rename
        column_mapping (dict): Dictionary, where key is target name and value is current name.

    Returns:
        gpd.GeoDataFrame: Renamed geodataframe
    """
    geom.rename(columns=column_mapping, inplace=True)
    assert isinstance(geom, gpd.GeoDataFrame)
    return geom


def attribute_join(shapefile: gpd.GeoDataFrame,
                   df: pd.DataFrame,
                   ) -> pd.DataFrame:
    
    joined_df = df.join(shapefile)
     
    return joined_df