import dataclasses
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from logging import getLogger

import pandas as pd
import requests
from sqlalchemy.engine.base import Engine

from covid_data.db import get_db, queries
from covid_data.db.queries import create_case, place_exists
from covid_data.utils import COMPONENTS_MAPPING

URL = f"https://api.opencagedata.com/geocode/v1/json?key={os.environ.get('CAGEDATA_API_KEY')}&no_annotations=1"
logger = getLogger("covid_data")


class PlaceNameNotProvidedException(Exception):
    pass


class PlaceInfoFetchException(Exception):
    pass


class PlaceInfoNotCompleteException(Exception):
    pass


class PlaceType:
    COUNTRY: str = "country"
    STATE: str = "state"
    CITY: str = "city"
    TERRITORY: str = "territory"


class CaseType:
    CONFIRMED: str = "confirmed"
    DEAD: str = "dead"
    RECOVERED: str = "recovered"


@dataclass
class Point:
    lat: float = None
    lng: float = None


@dataclass
class PlaceInfo:
    alpha2: str
    alpha3: str
    category: str
    type: str
    continent: str
    country: str
    country_code: str
    location: Point = None
    city: str = None
    county: str = None
    county_code: str = None
    political_union: str = None
    state: str = None
    state_code: str = None

    def __init__(self, **kwargs):
        names = set([f.name for f in dataclasses.fields(self)])
        for k, v in kwargs.items():
            if k in names:
                setattr(self, k, v)


def get_place_info(place_name: str) -> PlaceInfo:
    if not place_name:
        raise PlaceNameNotProvidedException()

    response = requests.get(URL, {"q": place_name})

    if response.status_code > 399:
        logger.error(
            f"Error while retrieving place info {place_name}. Error {response.status_code}"
        )
        logger.error(response.json())

        return None

    response_body = response.json()

    place = response_body["results"][0]

    components: dict = place["components"]

    components["alpha2"] = components.pop("ISO_3166-1_alpha-2")
    components["alpha3"] = components.pop("ISO_3166-1_alpha-3")
    components["type"] = components.pop("_type")
    components["category"] = components.pop("_category")

    if components["type"] == PlaceType.TERRITORY:
        components["type"] = PlaceType.STATE
        components["state"] = components.pop("territory")

    res: PlaceInfo

    res = PlaceInfo(**components)

    if res.type in COMPONENTS_MAPPING:
        res.type = COMPONENTS_MAPPING[res.type]

    if not hasattr(res, f"{res.type}") or getattr(res, f"{res.type}") is None:
        setattr(res, f"{res.type}", place_name)

    res.location = Point(**place["geometry"])

    return res


def extract_location(place_info: PlaceInfo, place_type: str) -> Point:
    res = Point()
    if place_info.type != place_type:
        correct_place_info = get_place_info(
            getattr(place_info, f"{place_type.replace(PlaceType.CITY, 'county')}_code")
            or getattr(place_info, f"{place_type.replace(PlaceType.CITY, 'county')}")
            or getattr(place_info, f"{place_type}_code")
            or getattr(place_info, f"{place_type}")
            or getattr(place_info, f"{place_info.type}")
        )
        if correct_place_info is None:
            raise PlaceInfoFetchException()

        res.lat = correct_place_info.location.lat
        res.lng = correct_place_info.location.lng
    else:
        res.lat = place_info.location.lat
        res.lng = place_info.location.lng

    return res


def create_country(country: str, engine: Engine, place_info: PlaceInfo = None) -> str:
    if country_id := place_exists(country, engine):
        return country_id

    if not place_info:
        place_info = get_place_info(country)
        if place_info is None:
            raise PlaceInfoFetchException()

    if not hasattr(place_info, "country"):
        place_info.country = country

    point = extract_location(place_info, PlaceType.COUNTRY)

    country_data = {
        "name": place_info.country,
        "alpha2": place_info.alpha2,
        "alpha3": place_info.alpha3,
        "lat": point.lat,
        "lng": point.lng,
    }

    return queries.create_country(country_data, engine)


def create_province(province: str, engine: Engine, place_info: PlaceInfo = None) -> str:
    if province_id := place_exists(province, engine):
        return province_id

    if not place_info:
        place_info = get_place_info(province)
        if place_info is None:
            raise PlaceInfoFetchException()

    if not hasattr(place_info, "country"):
        raise PlaceInfoNotCompleteException()

    country_id = create_country(place_info.country, engine, place_info)

    point = extract_location(place_info, PlaceType.STATE)

    province_data = {
        "name": place_info.state or getattr(place_info, f"{place_info.type}"),
        "code": place_info.state_code,
        "country_id": country_id,
        "lat": point.lat,
        "lng": point.lng,
    }

    return queries.create_province(province_data, engine)


def create_county(county: str, engine: Engine, place_info: PlaceInfo = None) -> str:
    if county_id := place_exists(county, engine):
        return county_id

    if not place_info:
        place_info = get_place_info(county)
        if place_info is None:
            raise PlaceInfoFetchException()

    province_id = create_province(place_info, engine)

    point = extract_location(place_info, PlaceType.CITY)

    county_data = {
        "name": place_info.county,
        "code": place_info.county_code or "",
        "lat": point.lat,
        "lng": point.lng,
        "province_id": province_id,
    }

    return queries.create_county(county_data, engine)


def insert_data(df: pd.DataFrame, case_type: CaseType) -> bool:
    engine = get_db()

    num_rows = df.shape[0]

    for index, row in df.iterrows():
        logger.info(f"Processing row {index + 1}/{num_rows}")
        state = row["Province/State"]
        country = row["Country/Region"]
        lat = row["Lat"]
        lng = row["Long"]
        province_id = None
        country_id = None

        if (pd.isna(lat) or pd.isna(lng)) or (float(lat) == 0 or float(lng) == 0):
            logger.warning(f"Skipping line {index + 2} due to missing location")
            continue

        try:
            if not pd.isna(state):
                province_id = create_province(state, engine)

            if not pd.isna(country):
                country_id = create_country(country, engine)
        except PlaceInfoFetchException:
            logger.error(f"Skipping line {index + 2}")
        except PlaceInfoNotCompleteException:
            logger.error(
                f"Skipping line {index + 2} due to incomplete information in fetching"
            )
        except PlaceNameNotProvidedException:
            logger.error(
                f"Skipping line {index + 2} because no place name could be extracted"
            )
        except (TypeError, KeyError):
            logger.error(
                f"Skipping line {index + 2} due to missing information in fetching"
            )

        row = row.drop(["Province/State", "Country/Region", "Lat", "Long"])

        num_columns = row.shape[0]

        for i, date_str in enumerate(row.index):
            logger.debug(f"Processing case {i+1}/{num_columns}")
            date_padded: str

            date_padded = "/".join([part.zfill(2) for part in date_str.split("/")])

            date = datetime.strptime(date_padded, "%m/%d/%y")

            case = {
                "type": case_type,
                "amount": row[date_str],
                "date": date,
                "country_id": country_id,
                "province_id": province_id,
                "county_id": None,
            }

            create_case(case, engine)


if __name__ == "__main__":
    from dotenv import load_dotenv

    from covid_data.logger import init_logger

    load_dotenv()

    init_logger(
        os.path.join(os.path.dirname(__file__), "../../logs/covid_data.log"),
        logging.INFO,
    )

    df = pd.read_csv(
        os.path.join(os.path.dirname(__file__), "../../data/recovered_global.csv")
    )

    insert_data(df, CaseType.RECOVERED)
