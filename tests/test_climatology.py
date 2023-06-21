
import os
import random
import sys

import numpy as np
import pytest
import rasterio

sys.path.insert(0, "pipeline")
from pipeline.climatology import (Bio, MaximumTemperature, MinimumTemperature,
                                  Precipitation, Temperature, get_climatology)


class TestClimatology:
    def test_product_instance(self):
        temp = get_climatology("temp")
        bio = get_climatology("bio")
        tmax = get_climatology("tmax")
        tmin = get_climatology("tmin")
        prec = get_climatology("prec")

        assert isinstance(temp, Temperature)
        assert isinstance(bio, Bio)
        assert isinstance(tmax, MaximumTemperature)
        assert isinstance(tmin, MinimumTemperature)
        assert isinstance(prec, Precipitation)
    
    def test_product_not_available_requested(self):
        with pytest.raises(ValueError):
            get_climatology("DOES NOT EXIST")
    
    def test_number_urls_constructed(self):
        """Expect 12 urls per available product"""

        temp = get_climatology("temp")
        available_products = len(temp.scenarios)
        urls = temp.url_constructor()

        assert len(urls) == available_products * 12
    
    def test_valid_urls_constructed(self):
        temp = get_climatology("temp")
        urls = temp.url_constructor()
        random_url = random.choice(urls)

        with rasterio.open(random_url, "r") as rast:
           raster = rast.read()
           assert isinstance(raster, np.ndarray)
