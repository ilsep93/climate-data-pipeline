import os

import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine


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

    for month in months:
       local_to_postgres(
           in_path=f"data/zonal_statistics/zs_{raster_name}.csv",
           rast_url=rast_url,
           month=month)

if __name__=="main":
    months = [1,2,3]
    local_to_postgres_parent_flow(months)