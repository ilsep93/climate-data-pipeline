import rasterio as rio
from prefect import flow, task
from rasterio.plot import show

#url = "https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/chelsa_cmip5_ts/CHELSAcmip5ts_tasmax_CMCC-CM_rcp45_2030-2049_V1.1.nc"
url = "https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/cmip5/2061-2080/temp/CHELSA_tas_mon_ACCESS1-0_rcp85_r1i1p1_g025.nc_1_2061-2080_V1.2.tif"

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

@flow()
def main_flow():
    fetch_raster(url)


if __name__ == "__main__":
    main_flow()
