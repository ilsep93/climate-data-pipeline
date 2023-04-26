import io
import logging
import os
from zipfile import ZipFile

import requests

logger = logging.getLogger(__name__)
logging.basicConfig(filename='adm.log', encoding='utf-8', level=logging.DEBUG)


def write_local_geometry(
    out_path: str,
    url:str,
    ) -> None:
    """Retrieves shapefile from Humanitarian Data Exchange
    and writes local shapefile

    Args:
        out_path (str): Save location of shapefile
        url (str): The URL to download the shapefile
    """
    shapefile_name = url.split("/")[-1].replace(".zip", "")

    if os.path.exists(f"{out_path}/{shapefile_name}.shp") is False:
        logger.info("Downloading shapefile")
        response = requests.get(url, allow_redirects=True, stream=True)
        response.raise_for_status()
        data = ZipFile(io.BytesIO(response.content))
        data.extractall(f"{out_path}")