import re
from dataclasses import dataclass


@dataclass
class Climatology():

    def _url_to_climatology(self, climatology_url):
        self.climatology = re.search(string=climatology_url, pattern="(rcp\d\d+)").group(0)
    
    def _climatology_pathways(self, climatology_url):   
        self._url_to_climatology(climatology_url)
        
        self.raw_raster = f"data/rasters/{self.climatology}/raw"
        self.masked_raster = f"data/rasters/{self.climatology}/masked"
        self.zonal_statistics = f"data/zonal_statistics/{self.climatology}/"
