import logging
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
from dotenv import load_dotenv
from session import get_session
from sqlalchemy.engine.reflection import Inspector
from tables import get_table

load_dotenv("docker/.env")

table_names = Literal["temp", "tmin", "tmax", "prec"]

logger = logging.getLogger(__name__)


def _check_if_table_exists(table_name: Literal[table_names]) -> bool:
    with get_session() as Session:
        with Session() as session:
            inspector = Inspector.from_engine(session.bind)
            if inspector.has_table(table_name):
                return True
            else:
                return False


def upload_to_db(df_path: Path, table_name: Literal[table_names]) -> None:
    chelsa_table = get_table(table_name=table_name)
    df = pd.read_csv(df_path, encoding="unicode_escape")

    with get_session() as Session:
        with Session() as session:
            try:
                for _, row in df.iterrows():
                    record = chelsa_table(**row, uploaded_at=datetime.now())
                    session.add(record)
                session.commit()
            except:
                session.rollback()
                logger.exception(f"Unable to add record to database.")
