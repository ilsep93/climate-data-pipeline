import logging

from climatology import Month, Product, Scenario, get_climatology
from config import read_config
from log import setup_logger
from processing_steps import execute_processing_steps, get_processing_steps

config = read_config("config.json")
logger = setup_logger()


def run_single_month(product: Product, scenario: Scenario, month: Month):
    """Runs pipeline given a product, scenario, and month.
    Steps include downloading, cropping, zonal statistics, uploading to the database.
    If all 12 months for a scenario are available, a yearly aggregate is generated.

    Args:
        product (str): CHELSA product
        scenario (Scenario): CMIP scenario
        month (Month): Month (provided as a name, rather than integer)
    """

    # Return concrete implementation of climatology object
    chelsa_product = get_climatology(product=product, scenario=scenario, month=month)

    # Determine which processing steps are needed for product's scenario
    processing_steps = get_processing_steps(
        product=chelsa_product, scenario=scenario, month=month
    )
    processing_step_names = [step.name for step in processing_steps]

    logger.info(
        f"Processing steps: {processing_step_names} for {chelsa_product.product.name}_{scenario.name}_{month.name}"
    )

    execute_processing_steps(
        processing_steps=processing_steps,
        chelsa_product=chelsa_product,
        scenario=scenario,
        month=month,
    )


def run_all_months(product: Product, scenario: Scenario):
    """All months for a given product's scenario"""
    available_months = [month for month in Month]

    for month in available_months:
        run_single_month(product=product, scenario=scenario, month=month)

    logging.shutdown()


if __name__ == "__main__":
    run_single_month(
        product=Product[config.product],
        scenario=Scenario[config.scenario],
        month=Month[config.month],
    )
