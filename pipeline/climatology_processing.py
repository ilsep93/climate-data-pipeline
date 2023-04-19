import os
import re
from dataclasses import dataclass

import fiona
import geopandas as gpd
import pandas as pd
import rasterio
from climatology_urls import climatology_base_url
from rasterio import mask
from rasterstats import zonal_stats


@dataclass()
class ClimatologyProcessing:
    climatology_url: str

    def _climatology_pathways(self):
        self.climatology = re.search(string=self.climatology_url, pattern="(rcp\d\d+)").group(0)
        self.raw_raster = f"data/rasters/{self.climatology}/raw"
        self.masked_raster = f"data/rasters/{self.climatology}/masked"
        self.zonal_statistics = f"data/zonal_statistics/{self.climatology}/"

    def _write_local_raster(self) -> None:
        """Download CHELSA raster and print descriptive statistics

        Args:
            url (str): URL for raster of interest
        """
        
        #Update self based on URL
        self._climatology_pathways()

        if os.path.exists(self.raw_raster) is False:
            os.makedirs(self.raw_raster)
        
        if len(os.listdir(self.raw_raster)) != 12:
            print(f"Downloading raw rasters for {self.climatology}")
            for month in range(1,13):
                rast_url = f"{self.climatology_url}_{month}_2061-2080_V1.2.tif"
                raster_name = rast_url.split("/")[-1].replace(".tif", "")
                
                if os.path.exists(f"{self.raw_raster}/{raster_name}.tif") is False:
                    with rasterio.open(rast_url, "r") as rast:
                        profile = rast.profile
                        print(f"Number of bands: {rast.count}")
                        print(f"Raster profile: {rast.profile}")
                        print(f"Bounds: {rast.bounds}")
                        print(f"Dimensions: {rast.shape}")

                        raster = rast.read()

                    with rasterio.open(f"{self.raw_raster}/{self.climatology}_{month}.tif", "w", **profile) as dest:
                        dest.write(raster)
                        print(f"Raster masked for {self.climatology}_{month}")
        else:
            print(f"All raw rasters are available for {self.climatology}")


    def _mask_raster(
            self,
            shp_path: str = "data/adm2/wca_admbnda_adm2_ocha.shp",
        ) -> None:
        """Mask raster with shapefile
        Note mask raster works best with features from fiona

        Args:
            shp_path (str): Path to shapefile
        """

        if os.path.exists(self.masked_raster) is False:
            os.makedirs(self.masked_raster)

        if len(os.listdir(self.masked_raster)) != 12:
            print(f"Masking rasters for {self.climatology}")
            with fiona.open(f"{shp_path}") as shapefile:
                shapes = [feature["geometry"] for feature in shapefile]
            
            for file in os.listdir(self.raw_raster):
                if file.endswith(".tif"):
                    with rasterio.open(f"{self.raw_raster}/{file}", "r") as raster:
                        profile = raster.profile
                        out_image, out_transform = mask.mask(raster, shapes, crop=True)
                    
                    with rasterio.open(f"{self.masked_raster}/msk_{file}", "w", **profile) as dest:
                        dest.write(out_image)
        else:
            print(f"All masked rasters are available for {self.climatology}")

    def kelvin_to_celcius(
            self,
            col
    ) -> None:
        return col - 273.15
    
    def _write_zonal_statistics(
        self,
        shp_path: str = "data/adm2/wca_admbnda_adm2_ocha.shp",
        ) -> None:
        """Write zonal statistics to local directory

        Args:
            shp_path (str): Path to shapefile
        """

        if os.path.exists(self.zonal_statistics) is False:
            os.makedirs(self.zonal_statistics)

        if len(os.listdir(self.zonal_statistics)) != 12:
            print(f"Calculating zonal statistics for {self.climatology}")

            shapefile = gpd.read_file(f"{shp_path}")
            for file in os.listdir(self.masked_raster):
                if file.endswith(".tif"):
                    with rasterio.open(f"{self.masked_raster}/{file}", "r") as src:
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
                        file = file.replace("msk_", "zs_")
                        full_df.to_csv(f"{self.zonal_statistics}/{file}", index=False)
        else:
            print(f"All zonal statistics are available for {self.climatology}")

def raster_processing_flow(
    climatologies: list
    ) -> None:

    for scenario in climatologies:
        cmip_temp = ClimatologyProcessing(climatology_url=scenario)
        cmip_temp._write_local_raster()
        cmip_temp._mask_raster()
        cmip_temp._write_zonal_statistics()


if __name__=="__main__":
    raster_processing_flow(climatology_base_url)


