import os
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path

import numpy as np


class Climatology(Enum):
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
    """Abstract class for all CHELSA climatology products"""

    phase: Phase = Phase.CMIP5
    time_period: str = "2061-2080"
    base_url: str
    climatology: Climatology
    scenarios: list[Scenario]
    months: list[Month]

    @abstractmethod
    def get_urls(self) -> list:
        """Constructs a list of urls based on a base url and available scenarios.
        Expect 12 URLs (one per month) for each available scenario.

        Returns:
            list: List of URLs that can be used to download raster .tif file
        """


    def extract_experiment_from_url(self, url: str) -> None:
        """Extracts the experiment name from a provided URL. Expect the same pattern for each product

        Args:
            url (str): URL used to download climatology from https://chelsa-climate.org/future/
        """

    def get_pathways(self, scenario: Scenario) -> list:
        if scenario not in self.scenarios:
            raise ValueError(f"Scenario not available. \
                             Options include {self.scenarios}")
        pathways = []
        base_export_path = Path(f"data/{self.phase.value}/{self.climatology.value}/{scenario.value}")
        folders = ["raw", "masked", "zonal_statistics", "time_series"]

        for folder in folders:
            path = os.path.join(base_export_path, folder)
            pathways.append(path)

        return pathways
    

    def create_directories(self, scenario: Scenario) -> None:
        """Create local directories to save downloaded and processed data"""
        
        pathways = self.get_pathways(scenario)
        for path in pathways:
            if not os.path.exists(path):
                os.makedirs(path)

    
class Temperature(ChelsaProduct):
    """Concrete implementation of temperature ChelsaProduct"""

    climatology = Climatology.TEMP
    scenarios = [Scenario.ACCESS1_0_rcp45,
                 Scenario.ACCESS1_0_rcp85,
                 Scenario.BNU_ESM_rcp26,
                 Scenario.BNU_ESM_rcp45,
                 Scenario.CCSM4_rcp60,
                 ]

    def get_urls(self, scenario: Scenario) -> list:
        if scenario not in self.scenarios:
            raise ValueError(f"This product is not available. \
                         Options include {self.scenarios}")
        
        months = range(1, 13)
        base_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/{self.phase.value}/{self.time_period}/{self.climatology.value}"
        
        download_urls = [f"{base_url}/CHELSA_tas_mon_{scenario.value}_r1i1p1_g025.nc_{month}_{self.time_period}_V1.2.tif" for month in months]
        return download_urls
    months = [month for month in Month]


class Bio(ChelsaProduct):
    """Concrete implementation of bio ChelsaProduct"""

    climatology = Climatology.BIO
    scenarios = [Scenario.ACCESS1_0_rcp45,
                 Scenario.ACCESS1_0_rcp85,
                 Scenario.BNU_ESM_rcp26,
                 Scenario.BNU_ESM_rcp45,
                 Scenario.CCSM4_rcp60,
                 ]

    def get_urls(self, scenario: Scenario) -> list:
        if scenario not in self.scenarios:
            raise ValueError(f"This product is not available. \
                         Options include {self.scenarios}")
        months = range(1, 13)
        base_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/{self.phase.value}/{self.time_period}/{self.climatology.value}"
        
        download_urls = [f"{base_url}/CHELSA_bio_mon_{scenario.value}_r1i1p1_g025.nc_{month}_{self.time_period}_V1.2.tif" for scenario, month in zip(self.scenarios, months)]
        return download_urls
    months = [month for month in Month]


class Precipitation(ChelsaProduct):
    """Concrete implementation of precipitation ChelsaProduct"""

    climatology = Climatology.PREC
    scenarios = [Scenario.ACCESS1_0_rcp45,
                 Scenario.ACCESS1_0_rcp85,
                 Scenario.BNU_ESM_rcp26,
                 Scenario.BNU_ESM_rcp45,
                 Scenario.CCSM4_rcp60,
                 ]
    months = [month for month in Month]

    def get_urls(self, scenario: Scenario) -> list:
        if scenario not in self.scenarios:
            raise ValueError(f"This product is not available. \
                         Options include {self.scenarios}")
        months = range(1, 13)
        base_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/{self.phase.value}/{self.time_period}/{self.climatology.value}"
        
        download_urls = [f"{base_url}/CHELSA_pr_mon_{scenario.value}_r1i1p1_g025.nc_{month}_{self.time_period}_V1.2.tif" for scenario, month in zip(self.scenarios, months)]
        return download_urls
    

class MaximumTemperature(ChelsaProduct):
    """Concrete implementation of maxiumum temperature ChelsaProduct"""
    
    climatology = Climatology.TMAX
    scenarios = [Scenario.ACCESS1_0_rcp45,
                 Scenario.ACCESS1_0_rcp85,
                 Scenario.BNU_ESM_rcp26,
                 Scenario.BNU_ESM_rcp45,
                 Scenario.CCSM4_rcp60,
                 ]
    months = [month for month in Month]

    def get_urls(self, scenario: Scenario) -> list:
        if scenario not in self.scenarios:
            raise ValueError(f"This product is not available. \
                         Options include {self.scenarios}")
        months = range(1, 13)
        base_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/{self.phase.value}/{self.time_period}/{self.climatology.value}"
        
        download_urls = [f"{base_url}/CHELSA_tasmax_mon_{scenario.value}_r1i1p1_g025.nc_{month}_{self.time_period}_V1.2.tif" for scenario, month in zip(self.scenarios, months)]
        return download_urls

class MinimumTemperature(ChelsaProduct):
    """Concrete implementation of minimum temperature ChelsaProduct"""
    
    climatology = Climatology.TMIN
    scenarios = [Scenario.ACCESS1_0_rcp45,
                 Scenario.ACCESS1_0_rcp85,
                 Scenario.BNU_ESM_rcp26,
                 Scenario.BNU_ESM_rcp45,
                 Scenario.CCSM4_rcp60,
                 ]

    def get_urls(self, scenario: Scenario) -> list:
        if scenario not in self.scenarios:
            raise ValueError(f"This product is not available. \
                         Options include {self.scenarios}")
        months = range(1, 13)
        base_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/{self.phase.value}/{self.time_period}/{self.climatology.value}"
        
        download_urls = [f"{base_url}/CHELSA_tasmin_mon_{scenario.value}_r1i1p1_g025.nc_{month}_{self.time_period}_V1.2.tif" for scenario, month in zip(self.scenarios, months)]
        return download_urls
    months = [month for month in Month]


def get_climatology(product: str):
    """Returns concrete implementation based on user provided product

    Args:
        product (str): Requested CHELSA product as a string

    Returns:
        _type_: Concrete implementation of CHELSA product
    """
    available_products = [product.value for product in Climatology]
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