import os
from enum import Enum, auto
from pathlib import Path

from climatology import ChelsaProduct, Month, Scenario


class RasterProcessingStep(Enum):
    DOWNLOAD = auto()
    MASK = auto()
    ZONAL_STATISTICS = auto()
    YEARLY_TABLE = auto()


# TODO: create dictionary of processing steps, with product, scenario, and month as keys


def get_processing_steps(product: ChelsaProduct, scenario: Scenario, month: Month) -> list[RasterProcessingStep]:
    """Determine which processing steps are needed for a given month of the product's scenario.
    Each pipeline run is for a specific product, month, and scenario pair.
    
    TODO: Log pipeline runs on postgres database.
    """

    all_pathways = product.get_pathways(scenario=scenario)

    raw_path = [str(path) for path in all_pathways if "raw" in path]
    masked_path = [str(path) for path in all_pathways if "mask" in path]
    zonal_dir = [str(path) for path in all_pathways if "zonal" in path]
    time_series_path = [str(path) for path in all_pathways if "time" in path]

    processing_steps = []
    
    raw_file_path = Path(os.path.join(raw_path[0], f"{scenario.value}_{month.value}.tif"))
    if not os.path.exists(raw_file_path):
        processing_steps.append(RasterProcessingStep.DOWNLOAD)

    masked_file_path = Path(os.path.join(masked_path[0], f"{scenario.value}_{month.value}.tif"))
    if not os.path.exists(masked_file_path):
        processing_steps.append(RasterProcessingStep.MASK)
    
    zonal_file_path = Path(os.path.join(zonal_dir[0], f"{scenario.value}_{month.value}.csv"))
    if not os.path.exists(zonal_file_path):
        processing_steps.append(RasterProcessingStep.ZONAL_STATISTICS)

    time_series_path = Path(os.path.join(time_series_path[0], f"{scenario.value}_yearly.csv"))
    all_months_available = _check_monthly_zonal_stats_complete(zonal_path=Path(zonal_dir[0]))
    if all_months_available and not os.path.exists(time_series_path):
        processing_steps.append(RasterProcessingStep.YEARLY_TABLE)
    
    return processing_steps

def _check_monthly_zonal_stats_complete(zonal_path: Path):
    available_files = [path for path in os.listdir(zonal_path) if path.endswith(".csv")]
    if len(available_files) == 12:
        return True
    else:
        return False
