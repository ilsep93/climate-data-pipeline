import json
from pathlib import Path

from climatology import Month, Product, Scenario
from pydantic import BaseSettings, ValidationError


class CMIPConfig(BaseSettings):
    """Config objects that should be specified in config.json"""

    root_dir: Path
    geom_path: Path
    adm_unique_id: str
    zonal_stats_aggregates: str
    raw_raster_dir: str
    cropped_raster_dir: str
    zonal_stats_dir: str
    yearly_aggregate_dir: str
    product: Product
    scenario: Scenario
    month: Month


def read_config(config_file: str) -> CMIPConfig:
    try:
        with open(config_file) as file:
            config = json.load(file)
            return CMIPConfig(**config)
    except ValidationError as e:
        raise ValueError("Invalid config: " + str(e))
