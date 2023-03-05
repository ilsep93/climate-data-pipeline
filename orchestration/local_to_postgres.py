import os

import pandas as pd
from dotenv import load_dotenv
from prefect import flow, task
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

load_dotenv("docker/.env")

@task(log_prints=True)
def local_to_postgres(
    in_path: str,
    rast_url: str,
    month:int
    ) -> None:

    rast_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/cmip5/2061-2080/temp/CHELSA_tas_mon_ACCESS1-0_rcp45_r1i1p1_g025.nc_{month}_2061-2080_V1.2.tif"
    raster_name = rast_url.split("/")[-1].replace(".tif", "")

    username=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASS"),  # plain (unescaped) text
    host=os.getenv("POSTGRES_HOST"),
    db=os.getenv("POSTGRES_DB")
    port=os.getenv("LOCAL_PORT")
    table=raster_name

    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    

    if engine.dialect.has_table(engine, table):  # If table don't exist, Create.
        print("table exists")
        pass
    else:
    try:
        print(engine)
        engine.connect()
        print("Connection established!")
        df = pd.read_csv(f"{in_path}.csv")
        df.head(n=0).to_sql(name=table, con=engine, if_exists='replace')
        df.to_sql(name=table, con=engine, if_exists='replace')

@flow()
def local_to_postgres_parent_flow(months: list[int]):
    rast_url = f"https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V1/cmip5/2061-2080/temp/CHELSA_tas_mon_ACCESS1-0_rcp45_r1i1p1_g025.nc_{month}_2061-2080_V1.2.tif"
    raster_name = rast_url.split("/")[-1].replace(".tif", "")
    except(OperationalError):
        print("Could not connect to postgres")
        pass


    for month in months:
       local_to_postgres(
           in_path=f"data/zonal_statistics/zs_{raster_name}.csv",
           rast_url=rast_url,
           month=month)

if __name__=="main":
    months = [1,2,3]
    local_to_postgres_parent_flow(months)