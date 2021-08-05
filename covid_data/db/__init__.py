import os

from psycopg2 import connect
from psycopg2._psycopg import connection  # pylint: disable=no-name-in-module

from .queries import (create_case, create_country, create_county,
                      create_province)


def get_db() -> connection:
    return connect(
        (
            f"postgresql://{os.environ.get('POSTGRES_USER', '')}"
            f":{os.environ.get('POSTGRES_PASS', '')}@"
            f"{os.environ.get('POSTGRES_HOST', 'localhost')}"
            f":{os.environ.get('POSTGRES_PORT', '5432')}/"
            f"{os.environ.get('POSTGRES_DB', '')}"
        )
    )


def close_db(conn: connection):
    return lambda *args, **kwargs: conn.close()


__all__ = [
    "get_db",
    "create_case",
    "create_country",
    "create_province",
    "create_county",
]
