from psycopg2._psycopg import connection, cursor  # pylint: disable=no-name-in-module

from covid_data.db import get_db


def fix_wrong_locations(engine: connection):
    countries_to_fix = [
        {
            "query": "alpha2='CA'",
            "update": "location=ST_SetSRID(ST_MakePoint(-106.346771, 56.130366), 4326)",
        }
    ]

    provinces_to_fix = [
        {
            "query": "name='Canarias, Spain'",
            "update": "name='Canary Islands', code='CN'",
        },
        {"query": "name='Navarre'", "update": "code='NC'"},
        {
            "query": "code='MC' AND country_id=242",
            "update": "location=ST_SetSRID(ST_MakePoint(-1.13004, 37.98704), 4326)",
        },
        {
            "query": "code='RI' AND country_id=242",
            "update": "location=ST_SetSRID(ST_MakePoint(-2.46302, 36.94508), 4326)",
        },
        {
            "query": "code='MD' AND country_id=242",
            "update": "location=ST_SetSRID(ST_MakePoint(-3.70256, 40.4165), 4326)",
        },
        {
            "query": "code='ML' AND country_id=242",
            "update": "location=ST_SetSRID(ST_MakePoint(-2.93833, 35.29369), 4326)",
        },
        {
            "query": "code='CE' AND country_id=242",
            "update": "location=ST_SetSRID(ST_MakePoint(-5.32042, 35.88919), 4326)",
        },
    ]

    with engine.cursor() as cur:
        cur: cursor

        for country_to_fix in countries_to_fix:
            cur.execute(
                f"UPDATE countries SET {country_to_fix['update']} WHERE {country_to_fix['query']}"  # nosec
            )

        for province_to_fix in provinces_to_fix:
            cur.execute(
                f"UPDATE provinces SET {province_to_fix['update']} WHERE {province_to_fix['query']}"  # nosec
            )


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    engine = get_db()

    try:
        fix_wrong_locations(engine)
    finally:
        engine.close()
