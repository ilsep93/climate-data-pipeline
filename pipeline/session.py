import os
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv("docker/.env")


@contextmanager
def get_session():
    """Yield database session"""
    username = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("LOCAL_HOST")
    db = os.getenv("POSTGRES_DB")
    port = os.getenv("LOCAL_PORT")

    engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{db}")
    session_maker = sessionmaker(bind=engine)

    yield session_maker
