from pathlib import Path

from climatology import ChelsaProduct, Month, Scenario
from config import read_config
from functions import calculate_zonal_statistics
from vector_processing import COLUMN_MAPPING, get_geometry

config = read_config("config.json")

def process_zonal_statistics(
        raster_location: Path,
        out_path: Path,
        place_id: str,
        geom_path: Path = config.geom_path,
       
        ) -> None:

    geometry = get_geometry(geom_path=geom_path,
                            column_mapping=COLUMN_MAPPING)
    zonal_stats = calculate_zonal_statistics(raster_location=raster_location,
                                             geometry=geometry,
                                             product=product,
                                             scenario=scenario,
                                             month=month,
                                             place_id=place_id)
    
    zonal_stats.to_csv(out_path, encoding='utf-8', index=False)