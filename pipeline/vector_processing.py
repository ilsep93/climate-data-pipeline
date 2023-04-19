import io
import os
from zipfile import ZipFile

import requests
from prefect import task


@task(log_prints=True, retries=3)
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
        print("Downloading shapefile")
        response = requests.get(url, allow_redirects=True, stream=True)
        response.raise_for_status()
        data = ZipFile(io.BytesIO(response.content))
        data.extractall(f"{out_path}")