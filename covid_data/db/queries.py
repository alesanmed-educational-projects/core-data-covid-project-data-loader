from sqlalchemy.engine.base import Connection, Engine


def country_exists(country: str, engine: Engine) -> bool:
    with engine.connect() as conn:
        conn: Connection

        result = conn.execute(
            "SELECT COUNT(*) FROM countries WHERE name = ':name'", {"name": country}
        )

        return list(result)[0][0] > 0


if __name__ == "__main__":
    country_exists("Spain")
