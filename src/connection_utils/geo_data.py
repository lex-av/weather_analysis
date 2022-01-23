# -*- coding: utf-8 -*-

from random import randint
from time import sleep
from typing import List, Union

import pandas as pd
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from joblib import Memory

from src.service_utils import project_root

memory = Memory(project_root() / "src" / "cache")


@memory.cache
def get_address_worker(coordinate: str) -> Union[str, None]:
    """
    Function to get address from external API
    Caching results is highly recommended due to API limitations

    :param coordinate: Concatenated Latitude and Longitude. example: "45.7865, -56.9483"
    :return: Geographical address for given coordinates or None in case of API errors
    """

    geocoder = Nominatim(user_agent="weather_hotels_1206")
    reverse = RateLimiter(geocoder.reverse, min_delay_seconds=1.5, return_value_on_exception=None, max_retries=2)
    # Small random sleep to help avoiding API TimeOuts
    sleep(randint(3 * 100, 10 * 100) / 1000)
    location = reverse(coordinate, language="en")

    return location.address


def collect_geo_data(coordinates: pd.DataFrame, max_index: int = None) -> List:
    """
    Uses get_address_worker to form list of geographical addresses by given
    DataFrame coordinates

    :param coordinates: pandas DataFrame
    :param max_index: index to use slice of DataFrame as such as df[0:max_index]
    :return: List of addresses for given DataFrame of coordinates
    """

    address_list = []
    if max_index is None:
        max_index = len(coordinates)

    for coordinate in coordinates.values[0:max_index]:
        address_list.append(get_address_worker(coordinate))

    return address_list


def calc_centre(coordinates: pd.DataFrame) -> List[float]:
    """
    Finds the centre of given DataFrame of coordinates.
    Treats given coordinates as planar and calculates average
    of them. That's why given coordinates should be from one
    city

    :param coordinates: pandas DataFrame[["Latitude", "Longitude"]], containing data for one city
    :return: calculated centre coordinates
    """

    latitude_values = list(coordinates["Latitude"].values)
    longitude_values = list(coordinates["Longitude"].values)

    return [sum(latitude_values) / len(latitude_values), sum(longitude_values) / len(longitude_values)]


if __name__ == "__main__":
    pass
