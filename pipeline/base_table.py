from sqlalchemy import Column, DateTime, Float, Integer, String
from sqlalchemy.orm import declared_attr


class BaseTable:
    id = Column(Integer, primary_key=True)
    iso2_code = Column(String(2))
    adm0_name = Column(String(128))
    adm1_name = Column(String(128))
    adm2_name = Column(String(128))
    adm1_id = Column(String(128))
    adm2_id = Column(String(128))
    product = Column(String(10), nullable=False)
    scenario = Column(String(40), nullable=False)
    month = Column(String, nullable=False)
    mean_raw = Column(Float)
    min_raw = Column(Float)
    max_raw = Column(Float)
    uploaded_at = Column(DateTime, nullable=False)
