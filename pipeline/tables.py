from base_table import BaseTable
from climatology import Product
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class TemperatureTable(Base, BaseTable):
    __tablename__ = "temp"


class MinimumTemperatureTable(Base, BaseTable):
    __tablename__ = "tmin"


class MaximumTemperatureTable(Base, BaseTable):
    __tablename__ = "tmax"


class PrecipitationTable(Base, BaseTable):
    __tablename__ = "prec"


class BioTable(Base, BaseTable):
    __tablename__ = "prec"


def get_table(table_name: str):
    factories = {
        "temp": TemperatureTable,
        "bio": BioTable,
        "prec": PrecipitationTable,
        "tmax": MaximumTemperatureTable,
        "tmin": MinimumTemperatureTable,
    }

    return factories[table_name]