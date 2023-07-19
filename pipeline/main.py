import logging
import os
import sys
from pathlib import Path

import pandas as pd
from climatology import ChelsaProduct, Month, Scenario, get_climatology
from processing_functions import (calculate_zonal_statistics,
                                  crop_raster_with_geometry, read_raster,
                                  write_local_raster, yearly_table_generator)
from processing_steps import RasterProcessingStep, get_processing_steps
from vector_processing import get_geometry

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


ROOT_DIR = Path(__file__).parent.parent

def process_raw_raster(
        product: ChelsaProduct,
        scenario: Scenario,
        month: Month,
        raw_out_path: Path) -> None:
    
    url = product.get_url(scenario=scenario, month=month)
    raster, profile = read_raster(location=url)
    write_local_raster(raster=raster, profile=profile, out_path=raw_out_path)


def process_masked_raster(
        raw_raster_location: Path,
        masked_out_path: Path,
        geom_path: Path = Path(f"{ROOT_DIR}/data/adm2/wca_admbnda_adm2_ocha.shp"),
        ) -> None:

    shapefile = get_geometry(geom_path=geom_path)
    masked_raster, profile = crop_raster_with_geometry(raster_location=raw_raster_location,
                                         gdf=shapefile,
                                         )
    write_local_raster(raster=masked_raster, profile=profile, out_path=masked_out_path)

def process_zonal_statistics(
        raster_location: Path,
        out_path: Path,
        product: ChelsaProduct,
        scenario: Scenario,
        month: Month,
        geom_path: Path = Path(f"{ROOT_DIR}/data/adm2/wca_admbnda_adm2_ocha.shp"),
       
        ) -> None:

    shapefile = get_geometry(geom_path=geom_path)
    zonal_stats = calculate_zonal_statistics(raster_location=raster_location,
                                             shapefile=shapefile,
                                             product=product,
                                             scenario=scenario,
                                             month=month)
    
    zonal_stats.to_csv(out_path, encoding='utf-8', index=False)


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
    pathways = concrete_product.get_pathways(scenario=scenario)

    # Determine which processing steps are needed for product's scenario 
    processing_steps = get_processing_steps(product=concrete_product,
                                            scenario=scenario,
                                            month=month)
    
    logger.info(f"Processing steps: {processing_steps} for {product}_{scenario.name}_{month.name}")

    if RasterProcessingStep.DOWNLOAD in processing_steps:
        logger.info(f"RasterProcessingStep.DOWNLOAD for {product}_{scenario.name}_{month.name}")

        raster_raw_location = Path(os.path.join(ROOT_DIR, pathways[0], f"{scenario.value}_{month.value}"))       
        
        process_raw_raster(product=concrete_product,
                           scenario=scenario,
                           month=month,
                           raw_out_path=raster_raw_location)
    
    if RasterProcessingStep.MASK in processing_steps:
        logger.info(f"RasterProcessingStep.MASK for {product}_{scenario.name}_{month.name}")

        raw_raster_location = Path(os.path.join(ROOT_DIR, pathways[0], f"{scenario.value}_{month.value}"))
        masked_out_path = Path(os.path.join(ROOT_DIR, pathways[1], f"{scenario.value}_{month.value}"))
        
        process_masked_raster(raw_raster_location=raw_raster_location,
                              masked_out_path=masked_out_path)
    
    if RasterProcessingStep.ZONAL_STATISTICS in processing_steps:
        logger.info(f"RasterProcessingStep.ZONAL_STATISTICS for {product}_{scenario.name}_{month.name}")

        masked_out_path = Path(os.path.join(ROOT_DIR, pathways[1], f"{scenario.value}_{month.value}"))
        zonal_out_path = Path(os.path.join(ROOT_DIR, pathways[2], f"{scenario.value}_{month.value}.csv"))

        process_zonal_statistics(raster_location=masked_out_path,
                                 out_path=zonal_out_path,
                                 month=month)
    
    if RasterProcessingStep.YEARLY_TABLE in processing_steps:
        logger.info(f"RasterProcessingStep.YEARLY_TABLE for {product}_{scenario.name}_{month.name}")
        
        yearly_table_out_path = Path(os.path.join(ROOT_DIR, pathways[-1], f"{scenario.value}_yearly.csv"))

        process_yearly_table(product = concrete_product,
                             zonal_dir=pathways[2],
                             out_path=yearly_table_out_path,
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
    raster_processing_parent_flow(product="temp",
                                  scenario=Scenario.ACCESS1_0_rcp45)                                  scenario=Scenario.BNU_ESM_rcp26)