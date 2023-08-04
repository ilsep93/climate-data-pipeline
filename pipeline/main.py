import logging
from pathlib import Path

from climatology import (ChelsaProduct, Month, Product, Scenario,
                         get_climatology)
from config import read_config
from processing_functions import (calculate_zonal_statistics,
                                  crop_raster_with_geometry, read_raster,
                                  write_local_raster, yearly_table_generator)
from processing_steps import RasterProcessingStep, get_processing_steps
from vector_processing import COLUMN_MAPPING, get_geometry

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler("pipeline_dev.log", encoding="utf-8")
file_handler.setLevel(logging.WARNING)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


config = read_config("config.json")

# TODO: add overwrite that will replace the existing file if needed

def raster_processing_flow(product: str, scenario: Scenario, month: Month):
    
    # Return concrete implementation of climatology object
    concrete_product = get_climatology(product=product)
    concrete_product.set_pathways_as_attributes(scenario=scenario, month=month)

    # Determine which processing steps are needed for product's scenario 
    processing_steps = get_processing_steps(product=concrete_product,
                                            scenario=scenario,
                                            month=month)
    
    logger.info(f"Processing steps: {processing_steps} for {product}_{scenario.name}_{month.name}")

    execute_processing_steps(processing_steps=processing_steps,
                             chelsa_product=chelsa_product,
                             scenario=scenario,
                             month=month)

def raster_processing_parent_flow(product: str, scenario: Scenario):
    # All months for a given product's scenario
    available_months = [month for month in Month]
    
    for month in available_months:
        raster_processing_flow(product=product,
                               scenario=scenario,
                               month=month)

    logging.shutdown()

if __name__=="__main__":
    raster_processing_flow(product=Product[config.product],
                                  scenario=Scenario[config.scenario],
                                  month=Month[config.month])