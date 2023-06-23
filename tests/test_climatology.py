
import os
import random
import sys

import numpy as np
import pytest
import rasterio

sys.path.insert(0, "pipeline")
from pipeline.climatology import (Bio, MaximumTemperature, MinimumTemperature,
                                  Precipitation, Scenario, Temperature,
                                  get_climatology)


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
        """Expect 12 urls per scenario"""

        temp = get_climatology("temp")
        urls = temp.get_urls(scenario=Scenario.ACCESS1_0_rcp45)

        assert len(urls) == 12
    
    def test_valid_scenarios_for_url(self):
        temp = get_climatology("temp")
        with pytest.raises(ValueError): 
            temp.get_urls(scenario="DOES NOT EXIST")
    
    def test_valid_urls_constructed(self):
        temp = get_climatology("temp")
        urls = temp.get_urls(scenario=Scenario.ACCESS1_0_rcp45)
        random_url = random.choice(urls)

        with rasterio.open(random_url, "r") as rast:
           raster = rast.read()
           assert isinstance(raster, np.ndarray)

    def test_filepaths_created(self):
        temp = get_climatology("temp")
        pathways = temp.get_pathways(scenario = Scenario.ACCESS1_0_rcp45)
        temp.create_directories(scenario = Scenario.ACCESS1_0_rcp45)

        for path in pathways:
            assert os.path.exists(path)