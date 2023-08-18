from pathlib import Path

from climatology import ChelsaProduct, Month, Scenario
from config import read_config
from functions import calculate_zonal_statistics
from vector_processing import COLUMN_MAPPING, get_geometry

config = read_config("config.json")


def process_zonal_statistics(
    raster_location: Path,
    out_path: Path,
    chelsa_product: ChelsaProduct,
    place_id: str,
    geom_path: Path = config.geom_path,
) -> None:
    """Processes zonal statistics for a CHELSA product

    Args:
        raster_location (Path): Location of raster that will be used for zonal statistics
        out_path (Path): Location where tabular zonal statistics will be saved
        chelsa_product (ChelsaProduct): Used to insert product identifiers to zonal statistics
        place_id (str): Column that contains a unique ID per geometry
        geom_path (Path, optional): Path to geometry used for zonal statistics. Defaults to config.geom_path.
    """
    geometry = get_geometry(geom_path=geom_path, column_mapping=COLUMN_MAPPING)
    zonal_stats = calculate_zonal_statistics(
        raster_location=raster_location,
        geometry=geometry,
        chelsa_product=chelsa_product,
        place_id=place_id,
    )

    zonal_stats.to_csv(out_path, encoding="utf-8", index=False)
