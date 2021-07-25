from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from .queries import *


def get_db() -> Engine:
    engine = create_engine("postgresql://postgres:patata@localhost/covid-data")

    return engine
