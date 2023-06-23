import random
import sys

import rasterio

sys.path.insert(0, "pipeline")
from pipeline.climatology import Climatology, get_climatology
from pipeline.climatology_processing import (read_raster_from_url,
                                             write_local_raster)


class TestClimatologyProcessing:
    def test_read_raster_from_url(self):
        random_product = random.choice(list(Climatology))
        random_climatology = get_climatology(random_product.value)
    
    def test_write_local_raster(self):
        ...
