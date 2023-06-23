
import os
import random
import sys

import numpy as np
import pytest
import rasterio

sys.path.insert(0, "pipeline")
from pipeline.climatology import (Bio, Climatology, MaximumTemperature,
                                  MinimumTemperature, Month, Precipitation,
                                  Scenario, Temperature, get_climatology)


@pytest.fixture(scope="session")
def random_climatology():
    random_product = random.choice(list(Climatology))
    random_climatology = get_climatology(random_product.value)

    return random_climatology

@pytest.fixture(scope="session")
def random_month():
    random_month = random.choice(list(Month))
    return random_month

@pytest.fixture(scope="session")
def random_scenario():
    random_scenario = random.choice(list(Scenario))
    return random_scenario

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
    
    def test_number_urls_constructed(self, random_month, random_scenario, random_climatology):
        url = random_climatology.get_url(month=random_month, scenario=random_scenario)

        assert type(url) == str
    
    def test_valid_scenarios_for_url(self, random_month, random_climatology):

        with pytest.raises(ValueError): 
            random_climatology.get_url(scenario="DOES NOT EXIST", month=random_month)

    def test_valid_month_for_url(self, random_scenario, random_climatology):
        with pytest.raises(ValueError): 
            random_climatology.get_url(scenario=random_scenario, month="DOES NOT EXIST")
    
    def test_valid_urls_constructed(self, random_month, random_scenario, random_climatology):
        url = random_climatology.get_url(scenario=random_scenario, month=random_month)

        with rasterio.open(url, "r") as rast:
           raster = rast.read()
           assert isinstance(raster, np.ndarray)

    def test_filepaths_created(self, random_scenario, random_climatology):
        pathways = random_climatology.get_pathways(scenario = random_scenario)
        for path in pathways:
            assert os.path.exists(path)