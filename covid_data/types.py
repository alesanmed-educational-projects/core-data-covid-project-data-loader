import dataclasses
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Union


class PlaceType(Enum):
    COUNTRY = "country"
    PROVINCE = "province"
    COUNTY = "county"
    STATE = "state"
    CITY = "city"
    TERRITORY = "territory"


class CaseType(Enum):
    CONFIRMED = "confirmed"
    DEAD = "dead"
    RECOVERED = "recovered"


@dataclass
class Point:
    lat: Union[float, None] = None
    lng: Union[float, None] = None


@dataclass
class PlaceInfo:
    alpha2: str
    alpha3: str
    category: str
    type: str
    continent: str
    country: str
    country_code: str
    location: Union[Point, None] = None
    city: Union[str, None] = None
    county: Union[str, None] = None
    county_code: Union[str, None] = None
    political_union: Union[str, None] = None
    state: Union[str, None] = None
    state_code: Union[str, None] = None

    def __init__(self, **kwargs):
        names = set([f.name for f in dataclasses.fields(self)])
        for k, v in kwargs.items():
            if k in names:
                setattr(self, k, v)


@dataclass
class CreatedPlace:
    country_id: Union[str, None] = None
    province_id: Union[str, None] = None
    county_id: Union[str, None] = None


class PlaceProperty(Enum):
    ID = "id"
    ALPHA_2_CODE = "alpha2Code"
    ALPHA_3_CODE = "alpha3Code"


class OnConflictStrategy(IntEnum):
    REPLACE = 1
    ADD = 2


class PlaceTable(Enum):
    COUNTRY = "countries"
    PROVINCE = "provinces"
    COUNTY = "counties"
