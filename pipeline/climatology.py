import os
import re
from dataclasses import dataclass
from enum import Enum


class Climatology(Enum):
    TEMP = "temp"
    BIO = "bio"
    PREC = "prec"
    TMAX = "tmax"
    TMIN = "tmin"

    def _url_to_climatology(self, climatology_url):
        self.climatology = re.search(string=climatology_url, pattern="CHELSA_tas_mon_(.*_rcp\d\d)").group(1)

class Scenario(Enum):
    ACCESS1_0_rcp45 = "ACCESS1-0_rcp45"
    ACCESS1_0_rcp85 = "ACCESS1-0_rcp85"
    BNU_ESM_rcp26 = "BNU-ESM_rcp26"
    CCSM4_rcp60 = "CCSM4_rcp60"
    BNU_ESM_rcp45 = "BNU-ESM_rcp45"


    
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
                os.makedirs(path)