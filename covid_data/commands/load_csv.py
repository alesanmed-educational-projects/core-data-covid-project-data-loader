import logging
import os
from contextlib import ExitStack
from datetime import datetime
from logging import getLogger

import pandas as pd

from covid_data.db import close_db, get_db
from covid_data.db.queries import (
    create_case,
    get_cases_by_country,
    get_cases_by_province,
)
from covid_data.errors import (
    PlaceInfoFetchException,
    PlaceInfoNotCompleteException,
    PlaceNameNotProvidedException,
    PlaceNotMatchedException,
)
from covid_data.types import CaseType, OnConflictStrategy
from covid_data.utils.places import CreatedPlace, create_country, create_province

logger = getLogger("covid-data")


def insert_data(df: pd.DataFrame, case_type: CaseType, optimize: bool = True) -> None:
    with ExitStack() as stack:
        engine = get_db()

        stack.push(close_db(engine))

        num_rows = df.shape[0]

        for index, row in enumerate(df.itertuples(index=False)):
            logger.info(f"Processing row {index + 1}/{num_rows}")
            state: str = row[df.columns.get_loc("Province/State")]
            country: str = row[df.columns.get_loc("Country/Region")]
            lat: float = row.Lat
            lng: float = row.Long
            created_country = CreatedPlace()
            created_province = CreatedPlace()

            if (pd.isna(lat) or pd.isna(lng)) or (lat == 0 or lng == 0):
                logger.warning(f"Skipping line {index + 2} due to missing location")
                continue

            err = False
            try:
                if not pd.isna(state):
                    created_province = create_province(state.replace("*", ""), engine)

                if not pd.isna(country):
                    created_country = create_country(country.replace("*", ""), engine)
            except PlaceInfoFetchException:
                err = True
                logger.error(f"Skipping line {index + 2}")
            except (PlaceInfoNotCompleteException, PlaceNotMatchedException):
                err = True
                logger.error(
                    f"Skipping line {index + 2} due to incomplete information in fetching"
                )
            except PlaceNameNotProvidedException:
                err = True
                logger.error(
                    f"Skipping line {index + 2} because no place name could be extracted"
                )
            except (TypeError, KeyError) as e:
                err = True
                logger.error(e)
                logger.error(
                    f"Skipping line {index + 2} due to missing information in fetching"
                )

            if err:
                continue

            cols = df.columns.drop(["Province/State", "Country/Region", "Lat", "Long"])

            num_columns = len(cols)

            saved_cases = []

            if created_province.province_id and created_country.country_id:
                saved_cases = get_cases_by_province(
                    int(created_country.country_id),
                    int(created_province.province_id),
                    engine,
                    case_type,
                )
            elif created_country.country_id:
                saved_cases = get_cases_by_country(
                    int(created_country.country_id), engine, case_type
                )

            if len(saved_cases) >= num_columns:
                logger.debug(f"Skipping line {index + 2} for optimizations")
                continue

            for i, date_str in enumerate(cols):
                logger.debug(f"Processing case {i+1}/{num_columns}")
                date_padded: str

                date_padded = "/".join([part.zfill(2) for part in date_str.split("/")])

                date = datetime.strptime(date_padded, "%m/%d/%y")

                case = {
                    "type": case_type.value,
                    "amount": row[df.columns.get_loc(date_str)],
                    "date": date,
                    "country_id": created_country.country_id,
                    "province_id": created_province.province_id,
                    "county_id": None,
                }

                create_case(case, engine, OnConflictStrategy.REPLACE)


if __name__ == "__main__":
    from dotenv import load_dotenv

    from covid_data.logger import init_logger

    load_dotenv()

    init_logger(
        os.path.join(os.path.dirname(__file__), "../../logs/covid_data.log"),
        logging.INFO,
    )

    for info in [
        (
            os.path.join(os.path.dirname(__file__), "../../data/confirmed_global.csv"),
            CaseType.CONFIRMED,
        ),
        (
            os.path.join(os.path.dirname(__file__), "../../data/deaths_global.csv"),
            CaseType.DEAD,
        ),
        (
            os.path.join(os.path.dirname(__file__), "../../data/recovered_global.csv"),
            CaseType.RECOVERED,
        ),
    ]:
        logger.info(f"Inserting {info[1]} cases")

        path: str = info[0]

        df = pd.read_csv(path)

        insert_data(df, info[1])
