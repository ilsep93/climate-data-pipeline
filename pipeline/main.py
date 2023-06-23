import logging
import os
from pathlib import Path

from climatology import Month, Scenario, get_climatology
from climatology_processing import (raster_description, read_raster_from_url,
                                    write_local_raster)
from processing_steps import RasterProcessingStep, get_processing_steps

logger = logging.getLogger(__name__)
logging.basicConfig(filename='processing_logger.log', encoding='utf-8', level=logging.DEBUG)


def raster_processing_flow(product: str, scenario: Scenario, month: Month):
    
    # Return concrete implementation of climatology object
    concrete_product = get_climatology(product=product)
    pathways = concrete_product.get_pathways(scenario=scenario)

    # Determine which processing steps are needed for product's scenario 
    processing_steps = get_processing_steps(product=concrete_product,
                                            scenario=scenario,
                                            month=month)
    
    # Determine if downloads are needed
    if RasterProcessingStep.DOWNLOAD in processing_steps:
        url = concrete_product.get_url(scenario=scenario, month=month)
        raster, profile = read_raster_from_url(url=url)
        raster_description(profile=profile)

        raw_out_path = Path(os.path.join(pathways[0], f"{scenario.value}_{month.value}"))
        write_local_raster(raster=raster, profile=profile, out_path=raw_out_path)
        logger.info(f"Raster downloaded for {concrete_product.climatology} - {scenario}")

if __name__=="__main__":
    raster_processing_flow(product="temp", scenario=Scenario.ACCESS1_0_rcp45, month=Month.OCTOBER)