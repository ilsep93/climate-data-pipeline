from pathlib import Path

import geopandas as gpd
import pandas as pd



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
    """Get a clean version of the shapefile

    Args:
        geom_path (Path): Path to .shp
        cols_to_drop (List[str]): Columns to drop from the shapefile
        lower_case (bool): Convert column names to lowercase if True

    Returns:
        gpd.GeoDataFrame: Shapefile without specified columns, with optional lowercase column names
    """
    shapefile = gpd.read_file(shp_path)
    clean_shapefile = shapefile.drop(columns=cols_to_drop)
    if lower_case:
        clean_shapefile.columns = map(str.lower, clean_shapefile.columns)
    mapped_geoms = _rename_geometry(geom=geometry, column_mapping=column_mapping)
  
def _rename_geometry(geom: gpd.GeoDataFrame, column_mapping: dict) -> gpd.GeoDataFrame:
    """Rename geometries based on column mapping to align with database standards.

    return clean_shapefile
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