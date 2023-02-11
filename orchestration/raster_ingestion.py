import io
from pathlib import Path
from zipfile import ZipFile
import rasterio as rio
import requests
from prefect import flow, task
from rasterio.plot import show

#url = "https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/chelsa_cmip5_ts/CHELSAcmip5ts_tasmax_CMCC-CM_rcp45_2030-2049_V1.1.nc"
url = "https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/cmip5/2061-2080/temp/CHELSA_tas_mon_ACCESS1-0_rcp85_r1i1p1_g025.nc_1_2061-2080_V1.2.tif"
adm2_url = "https://data.humdata.org/dataset/b20cd345-93fb-43bd-9c6e-7bc7d87b63eb/resource/30b6979a-d3f3-4982-971f-dc53f076bc52/download/wca_admbnda_adm2_ocha.zip"


@task(retries=3, log_prints=True)
def fetch_raster(url):
    # response = requests.get(url)
    # print(response)
    # print(type(response))
    with rio.open(url) as rast:
        print(rast.count)
        print(rast.profile)
        print(rast.bounds)
        #show(rast)
    return rast


@task(log_prints=True)
def fetch_vector(url:str) -> None:
    """Retrieves shapefile from Humanitarian Data Exchange

    Args:
        url (str): The URL to download the shapefile
    """
    # Create directory for shapefiles
    filename = url.split('/')[-1]
    filename = filename.replace(".zip", "")
    save_path=f"data/{filename}"
    Path(save_path).mkdir(parents=True, exist_ok=True)
    # Download, extract and save zipped contentes
    response = requests.get(url, allow_redirects=True, stream=True)
    response.raise_for_status()
    z = ZipFile(io.BytesIO(response.content))
    z.extractall(save_path)

@flow()
def main_flow():
    fetch_raster(url)
    fetch_raster(rast_url)
    fetch_vector(adm2_url)


if __name__ == "__main__":
    main_flow()
