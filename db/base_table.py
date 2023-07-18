from country import Country
from sqlalchemy import Column, DateTime, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import declared_attr, relationship

Base = declarative_base()

class TableMixin(Base):
    id = Column(Integer, primary_key=True)
    iso3_code = relationship('Country')
    place_name = Column(String(128), nullable=False)
    place_id = Column(String(128))
    month = Column(Integer, nullable=False)
    scenario = Column(String(20), nullable=False)
    mean = Column(Float, nullable=False)
    min = Column(Float)
    max = Column(Float)
    uploaded_at = Column(DateTime, nullable=False)

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    
    @declared_attr
    def __table_args__(cls):
        return cls.metadata.schema