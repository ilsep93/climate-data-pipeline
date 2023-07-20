from country import Country
from sqlalchemy import Column, DateTime, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import declared_attr, relationship

Base = declarative_base()

class TableMixin(Base):
    id = Column(Integer, primary_key=True)
    iso3_code = relationship('Country')
    adm0_name = Column(String(128))
    adm1_name = Column(String(128))
    adm2_name = Column(String(128))
    adm1_id = Column(String(128))
    adm2_id = Column(String(128))
    product = Column(String(10), nullable=False)
    scenario = Column(String(40), nullable=False)
    month = Column(Integer, nullable=False)
    mean_raw = Column(Float)
    min_raw = Column(Float)
    max_raw = Column(Float)
    uploaded_at = Column(DateTime, nullable=False)

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    
    @declared_attr
    def __table_args__(cls):
        return cls.metadata.schema