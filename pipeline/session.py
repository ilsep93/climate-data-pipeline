import os
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv("docker/.env")

@contextmanager
def get_session(schema: str):
    """Yield database session"""
    username=os.getenv("POSTGRES_USER")
    password=os.getenv("POSTGRES_PASSWORD")
    host="localhost"
    db=os.getenv("POSTGRES_DB")
    port=os.getenv("LOCAL_PORT")
    schema_name=schema

    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}?search_path={schema_name}')
    session_maker = sessionmaker(bind=engine)

    yield session_maker