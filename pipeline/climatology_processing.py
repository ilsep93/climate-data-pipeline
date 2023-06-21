import glob
import logging
import os
import re
from dataclasses import dataclass

import fiona
import geopandas as gpd
import pandas as pd
import rasterio
from climatology import Climatology
from rasterio import mask
from rasterstats import zonal_stats

logger = logging.getLogger(__name__)
logging.basicConfig(filename='processing_logger.log', encoding='utf-8', level=logging.DEBUG)

@dataclass()
class ClimatologyProcessing(Climatology):
    climatology_url: str

    @staticmethod
    def raster_description(rast):
        """Print description of raster

        Args:
            rast (rasterio): Raster connection via rasterio
        """
        print(f"Number of bands: {rast.count}")
        print(f"Raster profile: {rast.profile}")
        print(f"Bounds: {rast.bounds}")
        print(f"Dimensions: {rast.shape}")

    
    def _write_local_raster(self) -> None:
        """Download CHELSA raster and save locally
        """
        
        #Update self based on URL
        self._climatology_pathways(self.climatology_url)
        
        if len(os.listdir(self.raw_raster)) != 12:
            logger.info(f"Downloading raw rasters for {self.climatology}")
            
            for month in range(1,13):
                rast_url = f"{self.climatology_url}_{month}_2061-2080_V1.2.tif"

                with rasterio.open(rast_url, "r") as rast:
                    profile = rast.profile
                    self.raster_description(rast)
                    raster = rast.read()

                with rasterio.open(f"{self.raw_raster}/{self.climatology}_{month}.tif", "w", **profile) as dest:
                    dest.write(raster)
                    logger.info(f"Raster masked for {self.climatology}_{month}")
        else:
            logger.info(f"All raw rasters are available for {self.climatology}")


    def _mask_raster(
            self,
            shp_path: str = "data/adm2/wca_admbnda_adm2_ocha.shp",
        ) -> None:
        """Mask raster with shapefile
        Note mask raster works best with features from fiona

        Args:
            shp_path (str): Path to shapefile. Default is West Africa shapefile
        """

        if len(os.listdir(self.masked_raster)) != 12:
            logger.info(f"Masking rasters for {self.climatology}")
            
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
            logger.info(f"All masked rasters are available for {self.climatology}")

    @staticmethod
    def kelvin_to_celcius(
            col: int
    ) -> float:
        """Converts Kelvin into Celcius

        Args:
            col (int): Numeric value to convert to Celcius
        """
        return col - 273.15
    
    def _write_zonal_statistics(
        self,
        shp_path: str = "data/adm2/wca_admbnda_adm2_ocha.shp",
        ) -> None:
        """Write zonal statistics to local directory

        Args:
            shp_path (str): Path to shapefile
        """

        if len(os.listdir(self.zonal_statistics)) != 12:
            logger.info(f"Calculating zonal statistics for {self.climatology}")

            shapefile = gpd.read_file(f"{shp_path}")
            for file in os.listdir(self.masked_raster):
                if file.endswith(".tif"):
                    with rasterio.open(f"{self.masked_raster}/{file}", "r") as src:
                        array = src.read(1)
                        affine = src.transform
                        nodata = src.nodata

                        zs = zonal_stats(shapefile,
                                            array,
                                            affine=affine,
                                            nodata=nodata,
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
            logger.info(f"All zonal statistics are available for {self.climatology}")

    def climatology_yearly_table_generator(
        self,
    ):
        """Aggregates monthly climatology predictions into a yearly table.
        Processing steps: 
        * Add month int month column (1-12)
        * Sort values by administrative identifier and month
        """
        if not os.path.exists(f"{self.time_series}/{self.climatology}_yearly.csv"):
            zs_files = glob.glob(os.path.join(self.zonal_statistics, '*.csv'))
            
            li = []
            logger.info(f"Creating a yearly dataset for {self.climatology}")

            for file in zs_files:
                with open(f"{file}", 'r') as f:
                    month = re.search('_\d{1,2}', file).group(0)
                    month = month.replace("_", "")
                    df = pd.read_csv(f, index_col=None, header=0)
                    df['month'] = int(month)
                    li.append(df)

                data = pd.concat(li, axis=0, ignore_index=True)
                data.sort_values(by=["OBJECTID_1", "month"], inplace=True)
                data.to_csv(f"{self.time_series}/{self.climatology}_yearly.csv", index=False)
                
        else:
            logger.info(f"Yearly time appended dataset exists for {self.climatology}")

def raster_processing_flow(
    climatologies: list
    ) -> None:
    for url in climatologies:
        cmip_temp = ClimatologyProcessing(climatology_url=url)
        cmip_temp._climatology_pathways(climatology_url=url)
        cmip_temp._write_local_raster()
        cmip_temp._mask_raster()
        cmip_temp._write_zonal_statistics()
        cmip_temp.climatology_yearly_table_generator()


if __name__=="__main__":
    raster_processing_flow(climatology_base_urls)


