import os
from abc import ABC
from enum import Enum, auto
from pathlib import Path

from config import read_config

config = read_config("config.json")

class Product(Enum):
    TEMP = "temp"
    BIO = "bio"
    PREC = "prec"
    TMAX = "tmax"
    TMIN = "tmin"


class Scenario(Enum):
    ACCESS1_0_rcp45 = "ACCESS1-0_rcp45"
    ACCESS1_0_rcp85 = "ACCESS1-0_rcp85"
    BNU_ESM_rcp26 = "BNU-ESM_rcp26"
    CCSM4_rcp60 = "CCSM4_rcp60"
    BNU_ESM_rcp45 = "BNU-ESM_rcp45"


class Phase(Enum):
    CMIP5 = "cmip5"
    CMIP6 = "cmip6"


class Month(Enum):
    JANUARY = 1
    FEBRUARY = 2
    MARCH = 3
    APRIL = 4
    MAY = 5
    JUNE = 6
    JULY = 7
    AUGUST = 8
    SEPTEMBER = 9
    OCTOBER = 10
    NOVEMBER = 11
    DECEMBER = 12

class ChelsaProduct(ABC):
    """Abstract class for all CHELSA Product products"""

    phase: Phase = Phase.CMIP5
    time_period: str = "2061-2080"
    base_url: str
    product: Product
    scenarios: list[Scenario]
    months: list[Month]


    def get_url(self, scenario: Scenario, month: Month) -> str:
        """Constructs a URL based on a given scenario and month for a given product.

        Returns:
            download_url: URL that can be used to download raster .tif file
        """
        if scenario not in self.scenarios:
            raise ValueError(f"This product is not available. \
                         Options include {self.scenarios}")
        
        if month not in self.months:
            raise ValueError(f"This month is not available. \
                         Options include {self.months}")

        download_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/{self.phase.value}/ \
                        {self.time_period}/{self.product.value}/CHELSA_{self.base_url}_mon_{scenario.value} \
                        _r1i1p1_g025.nc_{month.value}_{self.time_period}_V1.2.tif"
        return download_url

    
    def set_pathways_as_attributes(self, scenario: Scenario, month: Month):
        base_path = Path(f"{config.root_dir}/{self.phase.value}/{self.product.value}/{scenario.value}/")

        self.raw_raster_dir = Path(f"{base_path}/{config.raw_raster_dir}/")
        self.cropped_raster_dir = Path(f"{base_path}/{config.cropped_raster_dir}/")
        self.zonal_stats_dir = Path(f"{base_path}/{config.zonal_stats_dir}/")
        self.yearly_aggregate_dir = Path(f"{base_path}/{config.yearly_aggregate_dir}/")
        
        self.raw_raster_path = Path(f"{self.raw_raster_dir}/{scenario.value}_{month.value}.tif")
        self.cropped_raster_path = Path(f"{self.cropped_raster_dir}/{scenario.value}_{month.value}.tif")
        self.zonal_file_path = Path(f"{self.zonal_stats_dir}/{scenario.value}_{month.value}.csv")
        self.yearly_aggregate_path = Path(f"{self.yearly_aggregate_dir}/{scenario.value}_{month.value}_yearly.csv")

        directories = [self.raw_raster_dir, self.cropped_raster_dir, self.zonal_stats_dir, self.yearly_aggregate_dir]

        self._create_directories(pathways=directories)

    def _create_directories(self, pathways: list) -> None:
        """Create local directories to save downloaded and processed data"""


        for path in pathways:
            if not os.path.exists(path):
                os.makedirs(path, exist_ok=True)

    
class Temperature(ChelsaProduct):
    """Concrete implementation of temperature ChelsaProduct"""

    product = Product.TEMP
    scenarios = [Scenario.ACCESS1_0_rcp45,
                 Scenario.ACCESS1_0_rcp85,
                 Scenario.BNU_ESM_rcp26,
                 Scenario.BNU_ESM_rcp45,
                 Scenario.CCSM4_rcp60,
                 ]
    months = [month for month in Month]
    base_url = "tas"


class Bio(ChelsaProduct):
    """Concrete implementation of bio ChelsaProduct"""

    product = Product.BIO
    scenarios = [Scenario.ACCESS1_0_rcp45,
                 Scenario.ACCESS1_0_rcp85,
                 Scenario.BNU_ESM_rcp26,
                 Scenario.BNU_ESM_rcp45,
                 Scenario.CCSM4_rcp60,
                 ]
    months = [month for month in Month]
    base_url = "bio"


class Precipitation(ChelsaProduct):
    """Concrete implementation of precipitation ChelsaProduct"""

    product = Product.PREC
    scenarios = [Scenario.ACCESS1_0_rcp45,
                 Scenario.ACCESS1_0_rcp85,
                 Scenario.BNU_ESM_rcp26,
                 Scenario.BNU_ESM_rcp45,
                 Scenario.CCSM4_rcp60,
                 ]
    months = [month for month in Month]
    base_url = "pr"


class MaximumTemperature(ChelsaProduct):
    """Concrete implementation of maxiumum temperature ChelsaProduct"""
    
    product = Product.TMAX
    scenarios = [Scenario.ACCESS1_0_rcp45,
                 Scenario.ACCESS1_0_rcp85,
                 Scenario.BNU_ESM_rcp26,
                 Scenario.BNU_ESM_rcp45,
                 Scenario.CCSM4_rcp60,
                 ]
    months = [month for month in Month]
    base_url = "tasmax"


class MinimumTemperature(ChelsaProduct):
    """Concrete implementation of minimum temperature ChelsaProduct"""
    
    product = Product.TMIN
    scenarios = [Scenario.ACCESS1_0_rcp45,
                 Scenario.ACCESS1_0_rcp85,
                 Scenario.BNU_ESM_rcp26,
                 Scenario.BNU_ESM_rcp45,
                 Scenario.CCSM4_rcp60,
                 ]
    months = [month for month in Month]
    base_url = "tasmin"


class TemperatureProduct(Enum):
    """Convenience class to identify temperature products"""
    Temperature = auto()
    MinimumTemperature = auto()
    MaximumTemperature = auto()


def get_climatology(product: str) -> ChelsaProduct:
    """Returns concrete implementation based on user provided product

    Args:
        product (str): Requested CHELSA product as a string

    Returns:
        ChelsaProduct: Concrete implementation of CHELSA product
    """
    available_products = [product.value for product in Product]
    if product not in available_products:
        raise ValueError(f"This product is not available. \
                         Options include {available_products}")

    factories = {
        "temp": Temperature(),
        "bio": Bio(),
        "prec": Precipitation(),
        "tmax": MaximumTemperature(),
        "tmin": MinimumTemperature(),
    }

    return factories[product]