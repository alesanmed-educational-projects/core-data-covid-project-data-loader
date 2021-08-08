import datetime
import json
import logging
import os

import requests
from psycopg2._psycopg import connection  # pylint: disable=no-name-in-module

from covid_data.db import close_db, get_db
from covid_data.db.queries import OnConflictStrategy, create_case
from covid_data.errors import DateFetchException
from covid_data.logger import init_logger
from covid_data.types import CaseType, PlaceType
from covid_data.utils.places import create_country, create_province

logger = logging.getLogger("covid-data")

START_DATE = datetime.datetime(2020, 3, 2)

ALT_NAMES = {"Guyane": "French Guiana"}
ALT_TYPES = {
    "Guadeloupe": PlaceType.STATE,
    "Martinique": PlaceType.STATE,
    "French Guiana": PlaceType.STATE,
    "Île-de-France": PlaceType.STATE,
    "Centre-Val de Loire": PlaceType.STATE,
    "Grand Est": PlaceType.STATE,
    "Pays de la Loire": PlaceType.STATE,
}


def scrap_cases(engine: connection, start_date: datetime.datetime = START_DATE) -> None:
    URL = "https://dashboard.covid19.data.gouv.fr/data/date-{}.json"

    now = datetime.datetime.now()

    days_from_start_to_now = (now - start_date).days + 1

    for i in range(days_from_start_to_now):
        logger.info(f"Processing day {i+1}/{days_from_start_to_now}")
        curr_date = start_date + datetime.timedelta(days=i)

        response = requests.get(URL.format(curr_date.strftime("%Y-%m-%d")))

        if response.status_code > 399:
            logger.error(f"Error fetching data for date {curr_date}")
            logger.error(response)

            raise DateFetchException()

        data = json.loads(response.text)

        for i, piece in enumerate(data):
            logger.info(f"Processing location {i+1}/{len(data)}")
            code = piece["code"]

            if code == "WORLD":
                continue
            if code == "FRA":
                created_place = create_country(code, engine)
            else:
                nom = ALT_NAMES.get(piece["nom"], piece["nom"])
                created_place = create_province(
                    nom,
                    engine,
                    None,
                    f"{nom}, France",
                    ALT_TYPES.get(nom, PlaceType.CITY),
                )

            if "testsPositifs" not in piece and "casConfirmes" not in piece:
                positive_cases = 0
            else:
                positive_cases = (
                    piece["testsPositifs"]
                    if "testsPositifs" in piece
                    else piece["casConfirmes"]
                )

            confirmed_data = {
                "type": CaseType.CONFIRMED.value,
                "amount": positive_cases,
                "date": curr_date,
                "country_id": created_place.country_id,
                "province_id": created_place.province_id,
                "county_id": None,
            }

            create_case(confirmed_data, engine, OnConflictStrategy.REPLACE)

            if "deces" in piece:
                dead_data = {
                    "type": CaseType.DEAD.value,
                    "amount": piece["deces"],
                    "date": curr_date,
                    "country_id": created_place.country_id,
                    "province_id": created_place.province_id,
                    "county_id": None,
                }

                create_case(dead_data, engine, OnConflictStrategy.REPLACE)

            if "gueris" in piece:
                recovered_data = {
                    "type": CaseType.RECOVERED.value,
                    "amount": piece["gueris"],
                    "date": curr_date,
                    "country_id": created_place.country_id,
                    "province_id": created_place.province_id,
                    "county_id": None,
                }

                create_case(recovered_data, engine, OnConflictStrategy.REPLACE)


def scrap():
    pass


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    init_logger(
        os.path.join(os.path.dirname(__file__), "../../logs/covid_data.log"),
        logging.INFO,
    )
    engine = get_db()

    try:
        scrap_cases(engine, START_DATE + datetime.timedelta(days=350))
    except Exception as e:
        raise e
    finally:
        close_db(engine)
