import os
from enum import Enum, auto
from pathlib import Path

from climatology import ChelsaProduct, Month, Scenario
from crop import process_masked_raster
from download import process_raw_raster
from log import setup_logger
from yearly_table import process_yearly_table
from zonal_stats import process_zonal_statistics

logger = setup_logger()
class RasterProcessingStep(Enum):
    DOWNLOAD = auto()
    MASK = auto()
    ZONAL_STATISTICS = auto()
    YEARLY_TABLE = auto()


def get_processing_steps(product: ChelsaProduct, scenario: Scenario, month: Month) -> list[RasterProcessingStep]:
    """Determine which processing steps are needed for a given month of the product's scenario.
    Each pipeline run is for a specific product, month, and scenario pair.
    
    TODO: Log pipeline runs on postgres database.
    """

    product.set_pathways_as_attributes(scenario=scenario, month=month)

    processing_steps = []
    
    if not os.path.exists(product.raw_raster_path):
        processing_steps.append(RasterProcessingStep.DOWNLOAD)

    if not os.path.exists(product.cropped_raster_path):
        processing_steps.append(RasterProcessingStep.MASK)
    
    if not os.path.exists(product.zonal_file_path):
        processing_steps.append(RasterProcessingStep.ZONAL_STATISTICS)

    all_months_available = _check_monthly_zonal_stats_complete(zonal_path=Path(product.zonal_stats_dir))
    if all_months_available and not os.path.exists(product.yearly_aggregate_path):
        processing_steps.append(RasterProcessingStep.YEARLY_TABLE)
    
    return processing_steps


def _check_monthly_zonal_stats_complete(zonal_path: Path):
    available_files = [path for path in os.listdir(zonal_path) if path.endswith(".csv")]
    if len(available_files) == 12:
        return True
    else:
        return False


def execute_processing_steps(processing_steps: list[RasterProcessingStep],
                             chelsa_product: ChelsaProduct,
                             scenario: Scenario,
                             month: Month) -> None:
    """Execute downloads, cropping, zonal statistics, and yearly table depending on processing steps

    Args:
        processing_steps (list[RasterProcessingStep]): List of available processing steps for that product, scenario, month
        chelsa_product (ChelsaProduct): Chelsa product to be processed
        scenario (Scenario): Scenario to be processed
        month (Month): Month to be processed
    """

    if RasterProcessingStep.DOWNLOAD in processing_steps:
        logger.info(f"RasterProcessingStep.DOWNLOAD for {chelsa_product}_{scenario.name}_{month.name}")
        
        process_raw_raster(product=chelsa_product,
                        scenario=scenario,
                        month=month,
                        raw_out_path=chelsa_product.raw_raster_path)
        
    if RasterProcessingStep.MASK in processing_steps:
        logger.info(f"RasterProcessingStep.MASK for {chelsa_product}_{scenario.name}_{month.name}")

        process_masked_raster(raw_raster_location=chelsa_product.raw_raster_path,
                            masked_out_path=chelsa_product.cropped_raster_path)
    
    if RasterProcessingStep.ZONAL_STATISTICS in processing_steps:
        logger.info(f"RasterProcessingStep.ZONAL_STATISTICS for {chelsa_product}_{scenario.name}_{month.name}")

        process_zonal_statistics(raster_location=chelsa_product.cropped_raster_path,
                                out_path=chelsa_product.zonal_file_path,
                                product=chelsa_product,
                                scenario=scenario,
                                month=month,
                                place_id="adm2_id")
    
    if RasterProcessingStep.YEARLY_TABLE in processing_steps:
        logger.info(f"RasterProcessingStep.YEARLY_TABLE for {chelsa_product}_{scenario.name}_{month.name}")
        
        process_yearly_table(product = chelsa_product,
                            zonal_dir=chelsa_product.zonal_stats_dir,
                            out_path=chelsa_product.yearly_aggregate_path,
                            sort_values=["admin2pcod", "month"])
    
    if len(processing_steps) == 0:
        logger.info(f"All available steps already completed for {chelsa_product}_{scenario.name}_{month.name}")

