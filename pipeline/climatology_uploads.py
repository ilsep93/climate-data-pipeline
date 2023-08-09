import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from db.base_table import TableMixin
from db.session import get_session

load_dotenv("docker/.env")

logger = logging.getLogger(__name__)


def upload_to_db(df_path: Path, table_name: str, schema: str) -> None:

    df = pd.read_csv(df_path, encoding= 'unicode_escape')

    with get_session() as Session:
        with Session() as session:
            try:
                for _, row in df.iterrows():
                    record  = TableMixin(**row, uploaded_at=datetime.now())
                    record.__tablename__ = table_name
                    record.__table_args__= schema
                    session.add(record)
                session.commit()
            except:
                session.rollback()
                logger.exception(f"Unable to add record to database.")



if __name__ == "__main__":
    upload_to_db(
        df_path=Path("data/cmip5/temp/ACCESS1-0_rcp45/time_series/ACCESS1-0_rcp45_yearly.csv"),
        table_name="test_table",
        schema="test_schema"
    )