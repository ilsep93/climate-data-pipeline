from pathlib import Path

import geopandas as gpd


def get_shapefile(shp_path: Path,
                  cols_to_drop: list[str] = ['OBJECTID_1', 'Shape_Leng', 'Shape_Area', 'validOn', 'validTo', 'last_modif', 'source', 'date'],
                  lower_case: bool = True
                  ) -> gpd.GeoDataFrame:
    """Get a clean version of the shapefile

    Args:
        shp_path (Path): Path to .shp
        cols_to_drop (List[str]): Columns to drop from the shapefile
        lower_case (bool): Convert column names to lowercase if True

    Returns:
        gpd.GeoDataFrame: Shapefile without specified columns, with optional lowercase column names
    """
    shapefile = gpd.read_file(shp_path)
    clean_shapefile = shapefile.drop(columns=cols_to_drop)
    if lower_case:
        clean_shapefile.columns = map(str.lower, clean_shapefile.columns)

    return clean_shapefile


def attribute_join(shapefile: gpd.GeoDataFrame,
                   df: pd.DataFrame,
                   ) -> pd.DataFrame:
    
    joined_df = df.join(shapefile)
     
    return joined_df