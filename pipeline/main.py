import logging

from climatology import Month, Product, Scenario, get_climatology
from config import read_config
from log import setup_logger
from processing_steps import execute_processing_steps, get_processing_steps

config = read_config("config.json")
logger = setup_logger()

# TODO: add overwrite that will replace the existing file if needed
def raster_processing_flow(product: str, scenario: Scenario, month: Month):
    """_summary_

    Args:
        product (str): _description_
        scenario (Scenario): _description_
        month (Month): _description_
    """
    
    # Return concrete implementation of climatology object
    chelsa_product = get_climatology(product=product)
    chelsa_product.set_pathways_as_attributes(scenario=scenario, month=month)

    # Determine which processing steps are needed for product's scenario 
    processing_steps = get_processing_steps(product=chelsa_product,
                                            scenario=scenario,
                                            month=month)
    processing_step_names = [step.name for step in processing_steps]
    
    logger.info(f"Processing steps: {processing_step_names} for {chelsa_product.product.name}_{scenario.name}_{month.name}")

    execute_processing_steps(processing_steps=processing_steps,
                             chelsa_product=chelsa_product,
                             scenario=scenario,
                             month=month)

def raster_processing_parent_flow(product: str, scenario: Scenario):
    """All months for a given product's scenario"""
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