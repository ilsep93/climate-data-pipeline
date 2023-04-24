import glob
import os
import re
from dataclasses import dataclass

import pandas as pd
from climatology import Climatology
from climatology_urls import climatology_base_urls
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

load_dotenv("docker/.env")

@dataclass
class ClimatologyUploads(Climatology):
    climatology_url: str
    
    def climatology_yearly_table_generator(
        self,
    ):
        self._climatology_pathways(self.climatology_url)

        if not os.path.exists(f"{self.time_series}/{self.climatology}_yearly.csv"):
            zs_files = glob.glob(os.path.join(self.zonal_statistics, '*.csv'))
            
            li = []
            print(f"Creating a yearly dataset for {self.climatology}")
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
            print(f"Yearly time appended dataset exists for {self.climatology}")

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
def local_to_postgres_flow(
        climatologies: list
    ) -> None:

    for url in climatologies:
        cmip_temp = ClimatologyUploads(climatology_url=url)
        cmip_temp.climatology_yearly_table_generator()

    # for file in os.listdir("data/zonal_statistics/"):
    #    if file.endswith(".csv"):
    #     print(f"Uploading: {file}")
    #     local_to_postgres(
    #         in_path=f"data/zonal_statistics/{file}",
    #         docker_run=False)

if __name__ == "__main__":
    local_to_postgres_flow(climatologies=climatology_base_urls)
    # upload = ClimatologyUploads(climatology_url=climatology_base_urls[0])
