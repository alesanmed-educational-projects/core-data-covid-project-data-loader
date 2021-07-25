import os
from logging import getLogger
from typing import NamedTuple

import pandas as pd
import requests

from covid_data.db.queries import country_exists

URL = f"https://api.opencagedata.com/geocode/v1/json?key={os.environ.get('CAGEDATA_API_KEY')}&no_annotations=1"
logger = getLogger('covid_data')

class Point(NamedTuple):
    lat: float
    lng: float

class PlaceInfo(NamedTuple):
    alpha2Code: str
    alpha3Code: str
    type: str
    sub_code: str
    location: Point


def get_place_info(place: str) -> PlaceInfo:
    response = requests.get(URL, {
        "q": place
    })

    if response.status_code > 399:
        logger.error(f"Error while retrieving place info {place}. Error {response.status_code}")
        logger.error(response.json())

        return None
    
    response_body = response.json()

def insert_data(df: pd.DataFrame) -> bool:
    for index, row in df.iterrows():
        state = row["Province/State"]
        country = row["Province/State"]
        latlng = (row["Lat"], row["Long"])

        if not country_exists(country):
            pass
