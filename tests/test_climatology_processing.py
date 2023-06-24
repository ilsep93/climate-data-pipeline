import random
import sys

import rasterio

sys.path.insert(0, "pipeline")
from pipeline.climatology import Climatology, get_climatology
from pipeline.climatology_processing import (read_raster_from_url,
                                             write_local_raster)


class TestClimatologyProcessing:
    def test_read_raster(self, climatology_url):
        raster, profile = read_raster(location=climatology_url)

        assert isinstance(raster, np.ndarray)
        assert isinstance(profile, Profile)
    
    
    def test_write_local_raster(self):
        ...
