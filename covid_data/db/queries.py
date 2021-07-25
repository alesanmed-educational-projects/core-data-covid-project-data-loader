from sqlalchemy.engine.base import Connection
from covid_data.db import get_db


def country_exists(country: str) -> bool:
    with get_db().connect() as conn:
        conn: Connection

        result = conn.execute(
            "SELECT COUNT(*) FROM countries WHERE name = ':name'", {"name": country}
        )

        return list(result)[0][0] > 0


if __name__ == "__main__":
    country_exists("Spain")
