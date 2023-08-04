import json
from dataclasses import dataclass
from pathlib import Path

from climatology import Month, Product, Scenario


@dataclass
class CMIPConfig:
    """Config objects that should be specified in config.json"""
    root_dir: Path
    geom_path: Path
    zonal_stats_aggregates: str
    raw_raster_dir: str
    cropped_raster_dir: str
    zonal_stats_dir: str
    yearly_aggregate_dir: str
    product: Product
    scenario: Scenario
    month: Month


def read_config(config_file: str) -> CMIPConfig:
    with open(config_file) as file:
        config = json.load(file)
        return CMIPConfig(**config)