import logging
import os
import sys
from pathlib import Path

from climatology import ChelsaProduct, Month, Scenario, get_climatology
from climatology_processing import (get_shapefile, mask_raster_with_shp,
                                    raster_description, read_raster,
                                    write_local_raster)
from processing_steps import RasterProcessingStep, get_processing_steps

sys.path.append("../utils/")
from timestamp import timestamp

logger = logging.getLogger(__name__)
logging.basicConfig(filename='processing_logger.log', encoding='utf-8', level=logging.DEBUG)

def process_raw_raster(
        product: ChelsaProduct,
        scenario: Scenario,
        month: Month,
        raw_out_path: Path) -> None:
    
    url = product.get_url(scenario=scenario, month=month)
    raster, profile = read_raster(location=url)
    raster_description(profile=profile)

    write_local_raster(raster=raster, profile=profile, out_path=raw_out_path)
    logger.info(f"Raster downloaded for {product.climatology} - {scenario}")


def raster_processing_flow(product: str, scenario: Scenario, month: Month):
    
    # Return concrete implementation of climatology object
    concrete_product = get_climatology(product=product)
    pathways = concrete_product.get_pathways(scenario=scenario)

    # Determine which processing steps are needed for product's scenario 
    processing_steps = get_processing_steps(product=concrete_product,
                                            scenario=scenario,
                                            month=month)
    
    # Raster downloading step
    if RasterProcessingStep.DOWNLOAD in processing_steps:
        logger.info(f"{timestamp()} : RasterProcessingStep.DOWNLOAD for {concrete_product}_{scenario}_{month}") 
                    
        process_raw_raster(product=concrete_product,
                           scenario=scenario,
                           month=month,
                           raw_out_path=Path(os.path.join(pathways[0], f"{scenario.value}_{month.value}")))
    
    


if __name__=="__main__":
    raster_processing_flow(product="temp", scenario=Scenario.ACCESS1_0_rcp45, month=Month.OCTOBER)