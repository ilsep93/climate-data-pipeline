import glob
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from climatology import Climatology
from climatology_urls import climatology_base_urls
from dotenv import load_dotenv
from session import get_session
from sqlalchemy import Engine, create_engine, inspect
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
from sqlalchemy.types import DateTime, Float, Integer, String

load_dotenv("docker/.env")

logger = logging.getLogger(__name__)
logging.basicConfig(filename='uploads_logger.log', encoding='utf-8', level=logging.DEBUG)

@dataclass
class ClimatologyUploads(Climatology):
    climatology_url: str
    
    def climatology_yearly_table_generator(
        self,
    ):
        self._climatology_pathways(self.climatology_url)
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
    def upload_to_db(
        self,
        engine: Engine
        ) -> None:
        """Uploads yearly aggregated table to postgres db
        Processing steps:
        * Reduce number of columns to adm names and projected values
        * Convert int to datetime
        * Add climatology column based on climatology name

        Args:
            engine (Engine): Engine connection to postgres db
        """

        table = self.climatology.lower()

            df = pd.read_csv(f"{self.time_series}/{self.climatology}_yearly.csv", encoding= 'unicode_escape')

            df.columns= df.columns.str.lower() # lowercase columns
            keep_cols = ['admin0name', 'admin1name', 'admin2name', 'min', 'max', 'mean', 'median', 'month']
            df = df[keep_cols] #subset to set of cols

            df["month"] = df["month"].apply(lambda x: datetime.strptime(str(x), "%m")) #transform from int to date

            df["climatology"] = self.climatology


            df.to_sql(name=table,
        with get_session() as Session:
            with Session() as session:
                df.to_sql(name=self.climatology.lower(),
                      schema=self.schema,
                      con=session.get_bind(),
                      if_exists='replace',
                      index=False,
                      dtype={
                        "admin0name": String,
                        "admin1name": String,
                        "admin2name": String,
                        "min": Float,
                        "max": Float,
                        "mean": Float,
                        "median": Float,
                        "month": DateTime
                        })

            logger.info(f"Uploaded '{self.climatology}' to the DB")

def local_to_postgres_flow(
        climatologies: list
    ) -> None:

    for url in climatologies:
        cmip_temp = ClimatologyUploads(climatology_url=url)
        cmip_temp.climatology_yearly_table_generator()
        cmip_temp.upload_to_db(engine=engine)

if __name__ == "__main__":
    local_to_postgres_flow(climatologies=climatology_base_urls)