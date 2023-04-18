import os
import re
from dataclasses import dataclass

import fiona
import geopandas as gpd
import pandas as pd
import rasterio
from rasterio import mask
from rasterstats import zonal_stats

cmip5_temp = "https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/cmip5/2061-2080/temp"

climatology_base_url = [
    f"{cmip5_temp}/CHELSA_tas_mon_ACCESS1-0_rcp45_r1i1p1_g025.nc",
    f"{cmip5_temp}/CHELSA_tas_mon_ACCESS1-0_rcp85_r1i1p1_g025.nc",
    f"{cmip5_temp}/CHELSA_tas_mon_BNU-ESM_rcp26_r1i1p1_g025.nc",
]

@dataclass()
class Climatology:
    climatology_url: str

    def climatology_pathways(self):
        self.climatology = re.search(string=self.climatology_url, pattern="(rcp\d\d+)").group(0)
        self.raw_raster = f"data/rasters/{self.climatology}/raw"
        self.masked_raster = f"data/rasters/{self.climatology}/masked"
        self.zonal_statistics = f"data/zonal_statistics/{self.climatology}/"

    def write_local_raster(self) -> None:
        """Download CHELSA raster and print descriptive statistics

        Args:
            url (str): URL for raster of interest
        """
        
        self.climatology_pathways()
        climatology = self.climatology
        raw_path = self.raw_raster

        if os.path.exists(raw_path) is False:
            os.makedirs(raw_path)

        for month in range(1,13):
            rast_url = f"{self.climatology_url}_{month}_2061-2080_V1.2.tif"
            raster_name = rast_url.split("/")[-1].replace(".tif", "")
            
            if os.path.exists(f"{raw_path}/{raster_name}.tif") is False:
                print("Downloading raster")

                with rasterio.open(rast_url, "r") as rast:
                    profile = rast.profile
                    print(f"Number of bands: {rast.count}")
                    print(f"Raster profile: {rast.profile}")
                    print(f"Bounds: {rast.bounds}")
                    print(f"Dimensions: {rast.shape}")

                    raster = rast.read()

                with rasterio.open(f"{raw_path}/{climatology}_{month}.tif", "w", **profile) as dest:
                    dest.write(raster)

    @task(log_prints=True)
    def mask_raster(
            self,
            shp_path: str
        ) -> None:
        """Mask raster with shapefile
        Note mask raster works best with features from fiona

        Args:
            shp_path (str): Path to shapefile
        """
        self.climatology_pathways()
        raw_path = self.raw_raster
        masked_path = self.masked_raster

        if os.path.exists(masked_path) is False:
            os.makedirs(masked_path)

        if len(os.listdir(raw_path)) != 0:
            print("Masking rasters")
            with fiona.open(f"{shp_path}") as shapefile:
                shapes = [feature["geometry"] for feature in shapefile]
            
            for file in os.listdir(raw_path):
                if file.endswith(".tif"):
                    with rasterio.open(f"{raw_path}/{file}", "r") as raster:
                        profile = raster.profile
                        out_image, out_transform = mask.mask(raster, shapes, crop=True)
                    
                    with rasterio.open(f"{masked_path}/msk_{file}", "w", **profile) as dest:
                        dest.write(out_image)
        else:
            print("There are no raw rasters available")

    def kelvin_to_celcius(
            self,
            col
    ) -> None:
        return col - 273.15
    
    def write_zonal_statistics(
        self,
        shp_path: str = "data/adm2/wca_admbnda_adm2_ocha.shp",
        ) -> None:
        """Write zonal statistics to local directory

        Args:
            shp_path (str): Path to shapefile
        """
        self.climatology_pathways()
        zonal_path = self.zonal_statistics
        masked_path = self.masked_raster

        if os.path.exists(zonal_path) is False:
            os.makedirs(zonal_path)

        if len(os.listdir(masked_path)) != 0:
            print("Calculating zonal statistics")

            shapefile = gpd.read_file(f"{shp_path}")
            for file in os.listdir(masked_path):
                if file.endswith(".tif"):
                    with rasterio.open(f"{masked_path}/{file}", "r") as src:
                        array = src.read(1)
                        affine = src.transform
                        nodata = src.nodata

                        zs = zonal_stats(shapefile,
                                            array,
                                            nodata=nodata,
                                            affine=affine,
                                            stats="min mean max median",
                                            geojson_out=False)

                        #Attribute join between shapefile and zonal stats
                        df = pd.DataFrame(zs)
                        full_df = shapefile.join(df, how="left")
                        full_df.drop(['geometry', 'Shape_Leng', "Shape_Area", 'validOn', 'validTo'], axis=1, inplace=True)
                        
                        #Convert from Kelvin to Celcius
                        stats= ["min", "mean", "max", "median"]
                        full_df[stats] = full_df[stats].apply(self.kelvin_to_celcius)
                        
                        #Export as CSV
                        file = file.replace(".tif", ".csv")
                        full_df.to_csv(f"{zonal_path}/{file}", index=False)

def raster_processing_flow(
    climatologies: list
    ) -> None:

    for scenario in climatologies:
        sim = Climatology(climatology_url=scenario)
        sim.write_local_raster()
        sim.mask_raster()
        sim.write_zonal_statistics()


if __name__=="__main__":
    raster_processing_flow(climatology_base_url)


