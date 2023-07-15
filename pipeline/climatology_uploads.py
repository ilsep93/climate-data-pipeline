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
    with get_session() as Session:
        with Session() as session:
            try:
                for _, row in df.iterrows():
                    record  = BaseTable(**row, uploaded_at=datetime.now())
                    record.__tablename__ = table_name
                    record.__table_args__= schema
                    session.add(record)
                session.commit()
            except:
                session.rollback()



if __name__ == "__main__":
