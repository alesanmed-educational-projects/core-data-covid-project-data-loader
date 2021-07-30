from sqlalchemy.engine import Connection
from sqlalchemy.engine.base import Engine


def place_exists(place: str, engine: Engine) -> str:
    with engine.connect() as conn:
        conn: Connection

        result = list(
            conn.exec_driver_sql(
                "SELECT id FROM countries WHERE name = %(name)s", {"name": place}
            )
        )

        if not len(result):
            return None
        else:
            return result[0][0]


def create_country(country: dict, engine: Engine) -> str:
    with engine.connect() as conn:
        conn: Connection

        result = conn.exec_driver_sql(
            (
                "INSERT INTO countries "
                "VALUES ("
                "DEFAULT, "
                "%(name)s, "
                "%(alpha2)s, "
                "%(alpha3)s, "
                "ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 4326)"
                ") "
                "RETURNING id"
            ),
            country,
        )

        return list(result)[0][0]


def create_province(province: dict, engine: Engine) -> str:
    with engine.connect() as conn:
        conn: Connection

        result = conn.exec_driver_sql(
            (
                "INSERT INTO provinces VALUES ("
                "DEFAULT, "
                "%(name)s, "
                "ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 4326), "
                "%(code)s, "
                "%(country_id)s"
                ") RETURNING id"
            ),
            province,
        )

        return list(result)[0][0]


def create_county(county: dict, engine: Engine) -> str:
    with engine.connect() as conn:
        conn: Connection

        result = conn.exec_driver_sql(
            (
                "INSERT INTO provinces VALUES ("
                "DEFAULT, "
                "%(name)s, "
                "ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 4326), "
                "%(code)s, "
                "%(province_id)s"
                ") RETURNING id"
            ),
            county,
        )

        return list(result)[0][0]


def create_case(case: dict, engine: Engine) -> bool:
    with engine.connect() as conn:
        conn: Connection

        conn.exec_driver_sql(
            (
                "INSERT INTO cases VALUES (DEFAULT, %(type)s, %(amount)s, %(date)s, %(country_id)s, "
                "%(province_id)s, %(county_id)s) "
                "ON CONFLICT ("
                "type, "
                "date, "
                "country_id, "
                "type, "
                "date, "
                "country_id, "
                "COALESCE(province_id, -1), COALESCE(county_id, -1)"
                ") DO UPDATE SET amount=%(amount)s"
            ),
            case,
        )

        return True


if __name__ == "__main__":
    from covid_data.db import get_db

    print(place_exists("Spain", get_db()))
