
from pathlib import Path

from climatology import ChelsaProduct, Month, Scenario
from functions import read_raster, write_local_raster


def process_raw_raster(
        product: ChelsaProduct,
        scenario: Scenario,
        month: Month,
        raw_out_path: Path) -> None:
    """Downloads CHELSA raster given a URL

    Args:
        product (ChelsaProduct): Product to be downloaded
        scenario (Scenario): Scenario for product
        month (Month): Month for scenario
        raw_out_path (Path): Location for saved raster
    """
    
    url = product.get_url(scenario=scenario, month=month)
    raster, profile = read_raster(location=url)
    write_local_raster(raster=raster, profile=profile, out_path=raw_out_path)

