import os
from enum import Enum, auto
from pathlib import Path

from climatology import ChelsaProduct
from config import read_config
from crop import process_masked_raster
from download import process_raw_raster
from log import setup_logger
from upload import _check_if_table_exists, upload_to_db
from yearly_table import process_yearly_table
from zonal_stats import process_zonal_statistics

logger = setup_logger()

config = read_config("config.json")


class RasterProcessingStep(Enum):
    DOWNLOAD = auto()
    MASK = auto()
    ZONAL_STATISTICS = auto()
    YEARLY_TABLE = auto()
    UPLOAD = auto()


def get_processing_steps(chelsa_product: ChelsaProduct) -> list[RasterProcessingStep]:
    """Determine which processing steps are needed for a given month of the product's scenario.
    Each pipeline run is for a specific product, month, and scenario pair.

    TODO: Log pipeline runs on postgres database.
    """

    processing_steps = []

    if not os.path.exists(product.raw_raster_path):
        processing_steps.append(RasterProcessingStep.DOWNLOAD)

    if not os.path.exists(product.cropped_raster_path):
        processing_steps.append(RasterProcessingStep.MASK)

    if not os.path.exists(product.zonal_file_path):
        processing_steps.append(RasterProcessingStep.ZONAL_STATISTICS)

    all_months_available = _check_monthly_zonal_stats_complete(
        zonal_path=Path(product.zonal_stats_dir)
    )
    if all_months_available and not os.path.exists(product.yearly_aggregate_path):
        processing_steps.append(RasterProcessingStep.YEARLY_TABLE)

    # table_exists = _check_if_table_exists(table_name=f"{scenario}_{month}")
    # if not table_exists:
    processing_steps.append(RasterProcessingStep.UPLOAD)

    return processing_steps


def _check_monthly_zonal_stats_complete(zonal_path: Path) -> bool:
    """Return True if all 12 months for a product's scenario are available

    Args:
        zonal_path (Path): Directory of scenario's zonal statistics

    Returns:
        bool: True if 12 files are available
    """
    available_files = [path for path in os.listdir(zonal_path) if path.endswith(".csv")]
    if len(available_files) == 12:
        return True
    else:
        return False


def execute_processing_steps(
    processing_steps: list[RasterProcessingStep], chelsa_product: ChelsaProduct
) -> None:
    """Execute downloads, cropping, zonal statistics, and yearly table depending on processing steps

    Args:
        processing_steps (list[RasterProcessingStep]): List of available processing steps for that product, scenario, month
        chelsa_product (ChelsaProduct): Chelsa product to be processed
        scenario (Scenario): Scenario to be processed
        month (Month): Month to be processed
    """
    if RasterProcessingStep.DOWNLOAD in processing_steps:
        process_raw_raster(
            product=chelsa_product,
            scenario=chelsa_product.scenario,
            month=chelsa_product.month,
            raw_out_path=chelsa_product.raw_raster_path,
        )

    if RasterProcessingStep.MASK in processing_steps:
        process_masked_raster(
            raw_raster_location=chelsa_product.raw_raster_path,
            masked_out_path=chelsa_product.cropped_raster_path,
        )

    if RasterProcessingStep.ZONAL_STATISTICS in processing_steps:
        process_zonal_statistics(
            raster_location=chelsa_product.cropped_raster_path,
            out_path=chelsa_product.zonal_file_path,
            product=chelsa_product,
            scenario=chelsa_product.scenario,
            month=chelsa_product.month,
            place_id=config.adm_unique_id,
        )

    if RasterProcessingStep.YEARLY_TABLE in processing_steps:
        process_yearly_table(
            product=chelsa_product,
            zonal_dir=chelsa_product.zonal_stats_dir,
            out_path=chelsa_product.yearly_aggregate_path,
            sort_values=[config.adm_unique_id, "month"],
        )

    if RasterProcessingStep.UPLOAD in processing_steps:
        upload_to_db(
            df_path=chelsa_product.zonal_file_path,
            table_name=chelsa_product.product.value,
        )

    if len(processing_steps) == 0:
        logger.info(
            f"All available steps already completed for {chelsa_product}_{chelsa_product.scenario.name}_{chelsa_product.month.name}"
        )
