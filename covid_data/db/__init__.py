import os

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from .queries import create_case, create_country, create_county, create_province


def get_db() -> Engine:
    engine = create_engine(
        (
            f"postgresql://{os.environ.get('POSTGRES_USER', '')}"
            f":{os.environ.get('POSTGRES_PASS', '')}@"
            f"{os.environ.get('POSTGRES_HOST', 'localhost')}"
            f":{os.environ.get('POSTGRES_PORT', '5432')}/"
            f"{os.environ.get('POSTGRES_DB', '')}"
        )
    )

    return engine


__all__ = [
    "get_db",
    "create_case",
    "create_country",
    "create_province",
    "create_county",
]
