import random
import sys

import rasterio

sys.path.insert(0, "pipeline")
from pipeline.climatology_processing import (mask_raster_with_shp, read_raster,
                                             write_local_raster)
from tests.test_climatology import (random_climatology, random_month,
                                    random_scenario)


@pytest.fixture(scope="session")
def climatology_url(random_climatology, random_month, random_scenario):
    url = random_climatology.get_url(scenario=random_scenario, month=random_month)
    return url

@pytest.fixture(scope="session")
def sample_raster(climatology_url):
    raster, _ = read_raster(location=climatology_url)
    return raster

@pytest.fixture(scope="session")
def sample_profile(climatology_url):
    _ , profile = read_raster(location=climatology_url)
    return profile

@pytest.fixture(scope="session")
def sample_shapefile():
    ...

class TestClimatologyProcessing:
    def test_read_raster(self, climatology_url):
        raster, profile = read_raster(location=climatology_url)

        assert isinstance(raster, np.ndarray)
        assert isinstance(profile, Profile)
    
    
    def test_write_local_raster(self):
        ...
