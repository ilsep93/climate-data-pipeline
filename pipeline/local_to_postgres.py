import os
import re

import pandas as pd
from dotenv import load_dotenv
from prefect import flow, task
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

load_dotenv("docker/.env")

@task(log_prints=True)
def local_to_postgres(
    in_path: str,
    docker_run: bool,
    ) -> None:

    table_name = re.search('_\d_2061-2080_V1',in_path).group(0)

    username=os.getenv("POSTGRES_USER")
    password=os.getenv("POSTGRES_PASSWORD")
    host=os.getenv("POSTGRES_HOST")
    db=os.getenv("POSTGRES_DB")
    port=os.getenv("LOCAL_PORT")
    table=table_name

    if docker_run:
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    else:
        engine = create_engine(f'postgresql://{username}:{password}@localhost:4000/{db}')

    try:
        print(engine)
        engine.connect()
        print("Connection established!")

        df = pd.read_csv(f"{in_path}", encoding= 'unicode_escape')
        df.head(n=0).to_sql(name=table, con=engine, if_exists='replace')
        df.to_sql(name=table, con=engine, if_exists='replace')

    except(OperationalError):
        print("Could not connect to postgres")
        pass


@flow()
def local_to_postgres_flow() -> None:
    for file in os.listdir("data/zonal_statistics/"):
       if file.endswith(".csv"):
        print(f"Uploading: {file}")
        local_to_postgres(
            in_path=f"data/zonal_statistics/{file}",
            docker_run=False)

if __name__ == "__main__":
    local_to_postgres_flow()