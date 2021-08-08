from typing import Iterable, List, Union

from psycopg2 import sql
from psycopg2._psycopg import connection, cursor  # pylint: disable=no-name-in-module

from covid_data.types import (
    CaseType,
    OnConflictStrategy,
    PlaceProperty,
    PlaceTable,
    PlaceType,
)


def place_exists(
    place: str, engine: connection, table: PlaceTable = PlaceTable.COUNTRY
) -> Union[str, None]:
    with engine.cursor() as cur:
        cur: cursor

        cur.execute(
            sql.SQL("SELECT id FROM {0} WHERE name=%s").format(
                sql.Identifier(table.value)
            ),
            (place,),
        )

        result = cur.fetchone() or []

        if not len(result):
            return None
        else:
            return result[0]


def get_place_by_property(
    value: str, property: PlaceProperty, engine: connection, place_type: PlaceType
) -> dict:
    from_table = None

    if place_type == PlaceType.COUNTRY:
        from_table = "countries"
    elif place_type == PlaceType.PROVINCE:
        from_table = "provinces"
    elif place_type == PlaceType.COUNTY:
        from_table = "counties"
    else:
        raise ValueError(f"Place type {place_type} not supported")

    with engine.cursor() as cur:
        cur: cursor

        cur.execute(
            sql.SQL("SELECT * FROM {0} WHERE {1}=%s").format(
                sql.Identifier(from_table), sql.Identifier(property.value)
            ),
            (value,),
        )

        result = cur.fetchone() or []

        return row_to_dict(result, from_table, engine)[0]


def get_country_by_alpha2(country: str, engine: connection) -> dict:
    return get_place_by_property(
        country, PlaceProperty.ALPHA_2_CODE, engine, PlaceType.COUNTRY
    )


def get_province_by_alpha2(province: str, engine: connection) -> dict:
    return get_place_by_property(
        province, PlaceProperty.ALPHA_2_CODE, engine, PlaceType.PROVINCE
    )


def get_county_by_alpha2(county: str, engine: connection) -> dict:
    return get_place_by_property(
        county, PlaceProperty.ALPHA_2_CODE, engine, PlaceType.COUNTY
    )


def get_country_by_id(country_id: str, engine: connection) -> dict:
    return get_place_by_property(
        country_id, PlaceProperty.ID, engine, PlaceType.COUNTRY
    )


def get_province_by_id(province_id: str, engine: connection) -> dict:
    return get_place_by_property(
        province_id, PlaceProperty.ID, engine, PlaceType.PROVINCE
    )


def get_county_by_id(county_id: str, engine: connection) -> dict:
    return get_place_by_property(county_id, PlaceProperty.ID, engine, PlaceType.COUNTY)


def row_to_dict(rows: Iterable, table: str, engine: connection) -> list[dict]:
    with engine.cursor() as cur:
        cur: cursor

        cur.execute(
            (
                "SELECT column_name "
                'FROM information_schema."columns" '
                "WHERE table_name=%s"
                "ORDER BY ordinal_position"
            ),
            (table,),
        )

        column_names = cur.fetchall()

        column_names = [column[0] for column in column_names]

        res: list[dict] = []

        for row in ensure_array(rows):
            mapped_row = {}
            for i, column_name in enumerate(column_names):
                mapped_row[column_name] = row[i]
            res.append(mapped_row)

        return res


def ensure_array(element) -> List:
    if not len(element):
        return []

    return element if isinstance(element[0], Iterable) else [element]


def create_country(country: dict, engine: connection) -> str:
    with engine.cursor() as cur:
        cur: cursor

        cur.execute(
            (
                "INSERT INTO countries "
                "VALUES ("
                "DEFAULT, "
                "%(name)s, "
                "%(alpha2)s, "
                "%(alpha3)s, "
                "ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 4326), "
                "NULL"
                ") "
                "RETURNING id"
            ),
            country,
        )

        result = cur.fetchone()

        engine.commit()

        return result[0]


def create_province(province: dict, engine: connection) -> str:
    with engine.cursor() as cur:
        cur: cursor

        cur.execute(
            (
                "INSERT INTO provinces VALUES ("
                "DEFAULT, "
                "%(name)s, "
                "ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 4326), "
                "NULL, "
                "%(code)s, "
                "%(country_id)s"
                ") RETURNING id"
            ),
            province,
        )

        result = cur.fetchone()

        engine.commit()

        return result[0]


def create_county(county: dict, engine: connection) -> str:
    with engine.cursor() as cur:
        cur: cursor

        cur.execute(
            (
                "INSERT INTO provinces VALUES ("
                "DEFAULT, "
                "%(name)s, "
                "ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 4326), "
                "NULL, "
                "%(code)s, "
                "%(province_id)s"
                ") RETURNING id"
            ),
            county,
        )

        result = cur.fetchone()

        engine.commit()

        return result[0]


def get_cases_by_country(
    country_id: int, engine: connection, case_type: CaseType = None
) -> list:
    with engine.cursor() as cur:
        cur: cursor

        params = [country_id]

        query = sql.SQL("SELECT * FROM cases WHERE country_id=%s")

        if case_type:
            query += sql.SQL("AND type=%s")
            params.append(case_type.value)

        cur.execute(query, tuple(params))

        return row_to_dict(cur.fetchall(), "cases", engine)


def get_cases_by_province(
    country_id: int, province_id: int, engine: connection, case_type: CaseType = None
) -> list:
    with engine.cursor() as cur:
        cur: cursor

        params = [country_id, province_id]

        query = sql.SQL("SELECT * FROM cases WHERE country_id=%s AND province_id=%s")

        if case_type:
            query += sql.SQL("AND type=%s")
            params.append(case_type.value)

        cur.execute(query, tuple(params))

        return row_to_dict(cur.fetchall(), "cases", engine)


def create_case(
    case: dict,
    engine: connection,
    conflict_strategy: OnConflictStrategy = OnConflictStrategy.REPLACE,
) -> bool:
    with engine.cursor() as cur:
        cur: cursor

        query = (
            "INSERT INTO cases VALUES (DEFAULT, %(type)s, %(amount)s, %(date)s, %(country_id)s, "
            "%(province_id)s, %(county_id)s) "
            "ON CONFLICT ("
            "type, "
            "date, "
            "country_id, "
            "COALESCE(province_id, -1), COALESCE(county_id, -1)"
            ") DO UPDATE SET "
        )

        if conflict_strategy is OnConflictStrategy.ADD:
            query += "amount=cases.amount + %(amount)s"
        else:
            query += "amount=%(amount)s"

        cur.execute(query, case)

        engine.commit()

        return True


if __name__ == "__main__":
    from dotenv import load_dotenv

    from covid_data.db import get_db

    load_dotenv()

    print(place_exists("Spain", get_db()))
