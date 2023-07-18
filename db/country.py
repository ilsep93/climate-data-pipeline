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
    name = Column(String, nullable=False)
