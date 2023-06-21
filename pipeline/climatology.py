import os
import re
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum


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

    
    def _climatology_pathways(self, climatology_url):   
        self._url_to_climatology(climatology_url)

        self.raw_raster = f"data/{self.climatology}/raw"
        self.masked_raster = f"data/{self.climatology}/masked"
        self.zonal_statistics = f"data/{self.climatology}/zonal_statistics"
        self.time_series = f"data/{self.climatology}/time_series"

        for path in (self.raw_raster,
                     self.masked_raster,
                     self.zonal_statistics,
                     self.time_series
                     ):
            if not os.path.exists(path):
                os.makedirs(path)    """Abstract class for all CHELSA climatology products"""
class ChelsaProduct(ABC):
    """Abstract class for all CHELSA climatology products"""

    phase: Phase = Phase.CMIP5
    time_period: str = "2061-2080"
    base_url: str
    climatology: Climatology
    scenarios: list[Scenario]

    @abstractmethod
    def url_constructor(self) -> list:
        """Constructs a list of urls based on a base url and available scenarios.
        Expect 12 URLs (one per month) for each available scenario.

        Returns:
            list: List of URLs that can be used to download raster .tif file
        """


class Temperature(ChelsaProduct):
    """Concrete implementation of temperature ChelsaProduct"""

    climatology = Climatology.TEMP
    scenarios = [Scenario.ACCESS1_0_rcp45,
                 Scenario.ACCESS1_0_rcp85,
                 Scenario.BNU_ESM_rcp26,
                 Scenario.BNU_ESM_rcp45,
                 Scenario.CCSM4_rcp60,
                 ]

    def url_constructor(self) -> list:
        months = range(1, 12)
        base_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/{self.phase.value}/{self.time_period}/{self.climatology.value}"
        
        download_urls = [f"{base_url}/CHELSA_tas_mon_{scenario.value}_r1i1p1_g025.nc_{month}_{self.time_period}_V1.2.tif" for scenario, month in zip(self.scenarios, months)]
        return download_urls


class Bio(ChelsaProduct):
    """Concrete implementation of bio ChelsaProduct"""

    climatology = Climatology.BIO
    scenarios = [Scenario.ACCESS1_0_rcp45,
                 Scenario.ACCESS1_0_rcp85,
                 Scenario.BNU_ESM_rcp26,
                 Scenario.BNU_ESM_rcp45,
                 Scenario.CCSM4_rcp60,
                 ]

    def url_constructor(self) -> list:
        months = range(1, 12)
        base_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/{self.phase.value}/{self.time_period}/{self.climatology.value}"
        
        download_urls = [f"{base_url}/CHELSA_bio_mon_{scenario.value}_r1i1p1_g025.nc_{month}_{self.time_period}_V1.2.tif" for scenario, month in zip(self.scenarios, months)]
        return download_urls


class Precipitation(ChelsaProduct):
    """Concrete implementation of precipitation ChelsaProduct"""

    climatology = Climatology.PREC
    scenarios = [Scenario.ACCESS1_0_rcp45,
                 Scenario.ACCESS1_0_rcp85,
                 Scenario.BNU_ESM_rcp26,
                 Scenario.BNU_ESM_rcp45,
                 Scenario.CCSM4_rcp60,
                 ]

    def url_constructor(self) -> list:
        months = range(1, 12)
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

    def url_constructor(self) -> list:
        months = range(1, 12)
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

    def url_constructor(self) -> list:
        months = range(1, 12)
        base_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/{self.phase.value}/{self.time_period}/{self.climatology.value}"
        
        download_urls = [f"{base_url}/CHELSA_tasmin_mon_{scenario.value}_r1i1p1_g025.nc_{month}_{self.time_period}_V1.2.tif" for scenario, month in zip(self.scenarios, months)]
        return download_urls

