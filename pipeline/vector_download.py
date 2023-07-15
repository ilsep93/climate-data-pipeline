import io
import json
import logging
import os
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import requests

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).parent.parent

def download_shapefile(out_path: Path, url:str,) -> None:
    """Retrieves shapefile from Humanitarian Data Exchange
    and writes local shapefile

    Args:
        out_path (str): Desired location of shapefile
        url (str): The URL to download the shapefile
    Returns:
        shapefile_location (str): Location of created shapefile

    """
    shapefile_name = url.split("/")[-1].replace(".zip", "")
    shapefile_location = Path(f"{out_path}/{shapefile_name}.shp")

    if not os.path.exists(shapefile_location):
        logger.info("Downloading shapefile")
        response = requests.get(url, allow_redirects=True, stream=True)
        response.raise_for_status()
        data = ZipFile(io.BytesIO(response.content))
        data.extractall(f"{out_path}")

        logger.info(f"Downloaded shapefile: {shapefile_location}")
    else:
        logger.info("Shapefile exists in provided location")


def download_geojson(url: str) -> None:

    response = requests.get(url, allow_redirects=True)
    response.raise_for_status()
    content = response.content
    json_data = json.loads(content)
    # JSON data to geojson
    # geojson = gpd.read_file(json_data.decode(), driver = 'GeoJSON')
    # geojson.to_file(f"{outpath}.geojson", driver='GeoJSON')
    return json_data


def shapefile_to_geojson(
        shp_path: Path,
        outpath: Path
        ) -> None:
    """Transforms shapefile to geojson

    Args:
        shp_path (str): Location of shapefile, without extension
        outpath (str): Desired location of geojson, without extension
    """
    if not os.path.exists(f"{outpath}.geojson"):
        shp_file = gpd.read_file(f"{shp_path}")
        shp_file.columns = [x.lower() for x in shp_file.columns]
        shp_file.to_file(Path(f"{outpath}.geojson"), driver='GeoJSON')
        logger.info(f"Saved GeoJSON to {outpath}.")
    
    else:
        logger.info("GeoJSON already exists in provided location.")

if __name__ == "__main__":
    shapefile_location = download_shapefile(
        url="https://data.humdata.org/dataset/b20cd345-93fb-43bd-9c6e-7bc7d87b63eb/resource/30b6979a-d3f3-4982-971f-dc53f076bc52/download/wca_admbnda_adm2_ocha.zip",
        out_path=Path(f"{ROOT_DIR}/data/adm2/")
        )
    shapefile_to_geojson(
        shp_path=Path(f"{ROOT_DIR}/data/adm2/"),
        outpath=Path(f"{ROOT_DIR}/dash/west_africa")
        )
    # download_geojson(
    #     url="https://data.humdata.org/dataset/b20cd345-93fb-43bd-9c6e-7bc7d87b63eb/resource/6166e76c-bc33-4c45-8031-649d76aa5644/download/wca_admbnda_adm2_ocha.json",
    #     outpath="../dash/west_africa.geojson")
