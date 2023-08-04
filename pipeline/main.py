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
def process_yearly_table(product: ChelsaProduct,
                         zonal_dir: Path,
                         out_path: Path,
                         sort_values: list[str]):
    
    yearly_table = yearly_table_generator(product=product,
                                          zonal_dir=zonal_dir,
                                          sort_values=sort_values)
    
    yearly_table.to_csv(out_path, encoding='utf-8', index=False)
    

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

    if RasterProcessingStep.DOWNLOAD in processing_steps:
        logger.info(f"RasterProcessingStep.DOWNLOAD for {product}_{scenario.name}_{month.name}")
        
        process_raw_raster(product=concrete_product,
                           scenario=scenario,
                           month=month,
                           raw_out_path=concrete_product.raw_raster_path)
    
    if RasterProcessingStep.MASK in processing_steps:
        logger.info(f"RasterProcessingStep.MASK for {product}_{scenario.name}_{month.name}")

        process_masked_raster(raw_raster_location=concrete_product.raw_raster_path,
                              masked_out_path=concrete_product.cropped_raster_path)
    
    if RasterProcessingStep.ZONAL_STATISTICS in processing_steps:
        logger.info(f"RasterProcessingStep.ZONAL_STATISTICS for {product}_{scenario.name}_{month.name}")

        process_zonal_statistics(raster_location=concrete_product.cropped_raster_path,
                                 out_path=concrete_product.zonal_file_path,
                                 product=concrete_product,
                                 scenario=scenario,
                                 month=month,
                                 place_id="adm2_id")
    
    if RasterProcessingStep.YEARLY_TABLE in processing_steps:
        logger.info(f"RasterProcessingStep.YEARLY_TABLE for {product}_{scenario.name}_{month.name}")
        
        process_yearly_table(product = concrete_product,
                             zonal_dir=concrete_product.zonal_stats_dir,
                             out_path=concrete_product.yearly_aggregate_path,
                             sort_values=["admin2pcod", "month"])
    
    if len(processing_steps) == 0:
        logger.info(f"All available steps already completed for {product}_{scenario.name}_{month.name}")


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