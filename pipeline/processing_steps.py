import os
from enum import Enum, auto
from pathlib import Path

from climatology import ChelsaProduct, Month, Scenario


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
