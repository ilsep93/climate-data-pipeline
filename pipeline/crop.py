from pathlib import Path

from config import read_config
from functions import crop_raster_with_geometry, write_local_raster
from vector_processing import COLUMN_MAPPING, get_geometry

config = read_config("config.json")


def process_masked_raster(
        raw_raster_location: Path,
        masked_out_path: Path,
        geom_path: Path = config.geom_path,
        ) -> None:

    geometry = get_geometry(geom_path=geom_path,
                             column_mapping=COLUMN_MAPPING)
    masked_raster, profile = crop_raster_with_geometry(raster_location=raw_raster_location,
                                         gdf=geometry,
                                         )
    write_local_raster(raster=masked_raster, profile=profile, out_path=masked_out_path)
