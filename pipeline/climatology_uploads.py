import glob
import os
import re
from dataclasses import dataclass

import pandas as pd
from climatology import Climatology
from climatology_urls import climatology_base_urls
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateSchema

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

    @staticmethod
    def _get_engine(docker_run: bool):
        
        username=os.getenv("POSTGRES_USER")
        password=os.getenv("POSTGRES_PASSWORD")
        host=os.getenv("POSTGRES_HOST")
        db=os.getenv("POSTGRES_DB")
        port=os.getenv("LOCAL_PORT")

        try:

            if docker_run:
                engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}')
            else:
                engine = create_engine(f'postgresql://{username}:{password}@localhost:4000/{db}')

            return engine

        except(OperationalError):
            print("Could not connect to postgres")
            pass
    
    def db_validator(
            self,
    ) -> None:
        
        self.schema = "climatology"
        engine = self._get_engine(docker_run=False)
        inspector = inspect(engine)

        if self.schema not in inspector.get_schema_names():
            CreateSchema(self.schema)
        
        #TODO: inspector for specific schema (default is public)
        # if self.climatology not in inspector.get_table_names():
        #     self.upload_to_db(engine=engine)


    def upload_to_db(
        self,
        engine
        ) -> None:

        table = self.climatology.lower()
        primary_key = "objectid_1"

        with Session(engine):
            df = pd.read_csv(f"{self.time_series}/{self.climatology}_yearly.csv", encoding= 'unicode_escape')
            df.columns= df.columns.str.lower()

            df.head(n=0).to_sql(name=table, con=engine, schema=self.schema, if_exists='replace', index=False)
            df.to_sql(name=table, con=engine, if_exists='replace', index=False)

            engine.execute(f'ALTER TABLE {self.schema}."{table}" ADD PRIMARY KEY ({primary_key})')
            print(f"Uploaded '{self.climatology}' to the DB")

def local_to_postgres_flow(
        climatologies: list
    ) -> None:

    for url in climatologies:
        cmip_temp = ClimatologyUploads(climatology_url=url)
        cmip_temp.climatology_yearly_table_generator()
        cmip_temp.db_validator()
        engine = cmip_temp._get_engine(docker_run=False)
        cmip_temp.upload_to_db(engine=engine)

if __name__ == "__main__":
    local_to_postgres_flow(climatologies=climatology_base_urls)