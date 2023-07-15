import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
from climatology import Month, Scenario
from dotenv import load_dotenv
from sqlalchemy.types import DateTime, Float, String

#sys.path.append("db/")
from db.base_table import BaseTable
from db.session import get_session

load_dotenv("docker/.env")

logger = logging.getLogger(__name__)
logging.basicConfig(filename='uploads_logger.log', encoding='utf-8', level=logging.DEBUG)


load_dotenv("docker/.env")

logger = logging.getLogger(__name__)

def upload_to_db(df_path: Path, table_name: str, schema: str) -> None:


            logger.info(f"Uploaded '{self.climatology}' to the DB")

def local_to_postgres_flow(
        climatologies: list
    ) -> None:

    for url in climatologies:
        cmip_temp = ClimatologyUploads(climatology_url=url)
        cmip_temp.upload_to_db()

if __name__ == "__main__":
    local_to_postgres_flow(climatologies=climatology_base_urls)