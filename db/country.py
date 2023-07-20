from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from session import get_session
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base

load_dotenv("docker/.env")

Base = declarative_base()

class Country(Base):
    __tablename__ = "country"
    __table_args__ = {"schema": "public"}

    iso3_code = Column(String(3), primary_key=True, nullable=False)
    iso2_code = Column(String(2), nullable=False)
    adm0_name = Column(String, nullable=False)


def delete_table():
    with get_session() as Session:
        with Session() as session:
            Base.metadata.drop_all(bind=session.connection())


def add_countries_to_db(countries_file: Path):
    with get_session() as Session:
        with Session() as session:
            Base.metadata.create_all(bind=session.connection())
            with open(countries_file) as file:
                countries = pd.read_csv(file, keep_default_na=False)
                for _, row in countries.iterrows():
                    try:
                        country = Country(**row)
                        session.add(country)
                        session.commit()
                    except:
                        session.rollback()
                    


if __name__ == "__main__":
    add_countries_to_db(Path("db/countries.csv"))