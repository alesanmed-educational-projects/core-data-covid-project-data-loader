import os
from difflib import SequenceMatcher
from logging import getLogger
from typing import List, Union

import requests
import unidecode
from psycopg2._psycopg import connection  # pylint: disable=no-name-in-module

from covid_data.db import queries
from covid_data.errors import (
    PlaceInfoFetchException,
    PlaceInfoNotCompleteException,
    PlaceNameNotProvidedException,
    PlaceNotMatchedException,
)
from covid_data.types import CreatedPlace, PlaceInfo, PlaceTable, PlaceType, Point
from covid_data.utils import COMPONENTS_MAPPING

URL = f"https://api.opencagedata.com/geocode/v1/json?key={os.environ.get('CAGEDATA_API_KEY')}&no_annotations=1"
logger = getLogger("covid_data")


def sanitize_place(place: str) -> str:
    return unidecode.unidecode(place).replace("-", " ").title()


def str_similarity(first: str, other: str) -> float:
    return SequenceMatcher(None, first, other).ratio()


def get_place_info(place_name: str) -> Union[PlaceInfo, None]:
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

    places: List = response_body["results"]
    place = None
    max_similarity = 0

    for candidate in places:
        components = candidate["components"]
        type = components["_type"]

        if type not in components:
            type = COMPONENTS_MAPPING[type]

        name = components[type]

        if (
            similarity := str_similarity(place_name, sanitize_place(name))
        ) > max_similarity:
            place = candidate
            max_similarity = similarity

        if max_similarity == 1.0:
            break

    if place is None:
        raise PlaceNotMatchedException()

    components: dict = place["components"]

    components["alpha2"] = components.pop("ISO_3166-1_alpha-2")
    components["alpha3"] = components.pop("ISO_3166-1_alpha-3")
    components["type"] = components.pop("_type")
    components["category"] = components.pop("_category")

    if components["type"] == PlaceType.TERRITORY.value:
        components["type"] = PlaceType.STATE.value
        components["state"] = components.pop("territory")

    res: PlaceInfo

    res = PlaceInfo(**components)

    if res.type in COMPONENTS_MAPPING:
        res.type = COMPONENTS_MAPPING[res.type]

    if not hasattr(res, f"{res.type}") or getattr(res, f"{res.type}") is None:
        setattr(res, f"{res.type}", place_name)

    res.location = Point(**place["geometry"])

    return res


def extract_location(place_info: PlaceInfo, place_type: PlaceType) -> Point:
    res = Point()
    if place_info.type != place_type.value:
        correct_place_info = get_place_info(
            getattr(
                place_info,
                place_type.value.replace(PlaceType.CITY.value, "county"),
            )
            or getattr(place_info, f"{place_type.value}")
        )
        if correct_place_info is None:
            raise PlaceInfoFetchException()

        if correct_place_info.location is not None:
            res.lat = correct_place_info.location.lat
            res.lng = correct_place_info.location.lng
    elif place_info.location is not None:
        res.lat = place_info.location.lat
        res.lng = place_info.location.lng

    return res


def create_country(
    country: str, engine: connection, place_info: Union[PlaceInfo, None] = None
) -> CreatedPlace:
    sanitized_country = sanitize_place(country)

    if country_id := queries.place_exists(
        sanitized_country, engine, PlaceTable.COUNTRY
    ):
        return CreatedPlace(country_id)

    if (
        place_info is not None
        and place_info.country_code.lower() != place_info.alpha2.lower()
    ):
        place_info = None

    if not place_info:
        place_info = get_place_info(country)
        if place_info is None:
            raise PlaceInfoFetchException()

    if not hasattr(place_info, "country"):
        place_info.country = country

    # Double check just in case the country name passed as parameter
    # is in a non-standard format/language and the database returns no
    # rows
    if country_id := queries.place_exists(place_info.country, engine):
        return CreatedPlace(country_id)

    point = extract_location(place_info, PlaceType.COUNTRY)

    country_data = {
        "name": sanitized_country,
        "alpha2": place_info.alpha2,
        "alpha3": place_info.alpha3,
        "lat": point.lat,
        "lng": point.lng,
    }

    country_id = queries.create_country(country_data, engine)

    return CreatedPlace(country_id)


def create_province(
    province: str,
    engine: connection,
    place_info: Union[PlaceInfo, None] = None,
    province_query: str = None,
) -> CreatedPlace:
    sanitized_province = sanitize_place(province)

    if province_id := queries.place_exists(
        sanitized_province, engine, PlaceTable.PROVINCE
    ):
        province_data = queries.get_province_by_id(province_id, engine)
        country_id = province_data["country_id"]
        return CreatedPlace(country_id, province_id)

    if not place_info:
        place_info = get_place_info(province_query if province_query else province)
        if place_info is None:
            raise PlaceInfoFetchException()

    if not hasattr(place_info, "country"):
        raise PlaceInfoNotCompleteException()

    # Double check just in case the province name passed as parameter
    # is in a non-standard format/language and the database returns no
    # rows
    if province_id := queries.place_exists(
        place_info.state or getattr(place_info, f"{place_info.type}"),
        engine,
        PlaceTable.PROVINCE,
    ):
        province_data = queries.get_province_by_id(province_id, engine)
        country_id = province_data["country_id"]
        return CreatedPlace(country_id, province_id)

    created_place = create_country(place_info.country, engine, place_info)

    point = extract_location(place_info, PlaceType.STATE)

    province_data = {
        "name": sanitized_province,
        "code": place_info.state_code,
        "country_id": created_place.country_id,
        "lat": point.lat,
        "lng": point.lng,
    }

    province_id = queries.create_province(province_data, engine)

    return CreatedPlace(created_place.country_id, province_id)


def create_county(
    county: str, engine: connection, place_info: Union[PlaceInfo, None] = None
) -> CreatedPlace:
    sanitized_county = sanitize_place(county)

    if county_id := queries.place_exists(sanitized_county, engine, PlaceTable.COUNTY):
        county_data = queries.get_county_by_id(county_id, engine)
        province_id = county_data["province_id"]
        province = queries.get_province_by_id(province_id, engine)
        country_id = province["country_id"]
        return CreatedPlace(country_id, province_id, county_id)

    if not place_info:
        place_info = get_place_info(county)
        if place_info is None:
            raise PlaceInfoFetchException()

    # Double check just in case the county name passed as parameter
    # is in a non-standard format/language and the database returns no
    # rows
    if place_info.county is not None and (
        county_id := queries.place_exists(
            place_info.county, engine, queries.PlaceTable.COUNTY
        )
    ):
        county_data = queries.get_county_by_id(county_id, engine)
        province_id = county_data["province_id"]
        province = queries.get_province_by_id(province_id, engine)
        country_id = province["country_id"]
        return CreatedPlace(country_id, province_id, county_id)

    if place_info.state_code is None:
        raise PlaceInfoNotCompleteException()

    created_place = create_province(place_info.state_code, engine)

    point = extract_location(place_info, PlaceType.CITY)

    county_data = {
        "name": sanitized_county,
        "code": place_info.county_code or "",
        "lat": point.lat,
        "lng": point.lng,
        "province_id": created_place.province_id,
    }

    county_id = queries.create_county(county_data, engine)

    return CreatedPlace(created_place.country_id, created_place.province_id, county_id)
