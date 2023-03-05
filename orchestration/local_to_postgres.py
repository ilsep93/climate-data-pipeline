import os
from pathlib import Path

import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from utils import (mask_raster, write_local_geometry, write_local_raster,
                   write_zonal_statistics)


@task()
def web_to_postgres(zs_path: str, raster_name:str) -> None:
    username=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASS"),  # plain (unescaped) text
    host=os.getenv("POSTGRES_HOST"),
    db=os.getenv("POSTGRES_DB")
    port=os.getenv("LOCAL_PORT")
    table=raster_name

    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    df = pd.read_csv(zs_path)

    if engine.dialect.has_table(engine, table):  # If table don't exist, Create.
        print("table exists")
        # metadata = MetaData(engine)
        # # Create a table with the appropriate Columns
        # Table(Variable_tableName, metadata,
        #     Column('Id', Integer, primary_key=True, nullable=False), 
        #     Column('Date', Date), Column('Country', String),
        #     Column('Brand', String), Column('Price', Float),
        # # Implement the creation
        # metadata.create_all()

    df.head(n=0).to_sql(name=table, con=engine, if_exists='replace')
    df.to_sql(name=table, con=engine, if_exists='replace')


@flow(log_prints=True)
def postgres_ingestion(month: int):
    rast_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/cmip5/2061-2080/temp/CHELSA_tas_mon_ACCESS1-0_rcp45_r1i1p1_g025.nc_{month}_2061-2080_V1.2.tif"
    raster_name = rast_url.split("/")[-1].replace(".tif", "")

    adm2_url = "https://data.humdata.org/dataset/b20cd345-93fb-43bd-9c6e-7bc7d87b63eb/resource/30b6979a-d3f3-4982-971f-dc53f076bc52/download/wca_admbnda_adm2_ocha.zip"
    shapefile_name = adm2_url.split("/")[-1].replace(".zip", "")

    adm_level = "adm2"
    shp_path = f"data/{adm_level}/{shapefile_name}.shp"
    raw_path = "data/rasters/raw"
    masked_path = "data/rasters/masked"
    zs_path = "data/zonal_statistics"

    if os.path.exists(f"{raw_path}/{raster_name}.tif") is False:
        print("Downloading raster")
        write_local_raster(
            url=rast_url,
            out_path=f"{raw_path}/{raster_name}.tif"
            )

    if os.path.exists(f"data/adm2/{shapefile_name}.shp") is False:
        print("Downloading shapefile")
        write_local_geometry(url=adm2_url,
            adm_level=adm_level
            )
    
    if os.path.exists(f"{masked_path}/masked_{raster_name}.tif") is False:
        print("Masking raster")
        mask_raster(
            raw_path=f"{raw_path}/{raster_name}.tif",
            shp_path=shp_path,
            masked_path=f"{masked_path}/masked_{raster_name}.tif"
                    )

    if os.path.exists(f"{zs_path}/zs_{raster_name}.csv") is False:
        write_zonal_statistics(
            masked_rast=f"{masked_path}/masked_{raster_name}.tif",
            shp_path=f"{shp_path}",
            zs_path=f"{zs_path}/zs_{raster_name}.csv")
        
        web_to_postgres(zs_path=f"{zs_path}/zs_{raster_name}.csv",
                        raster_name=raster_name)
        
@flow()
def postgres_ingestion_parent_flow(months: list[int] = [1, 2]):
    for month in months:
       postgres_ingestion(month)

if __name__=="main":
    months = [5,6]
    postgres_ingestion_parent_flow(months)