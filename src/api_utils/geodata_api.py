# -*- coding: utf-8 -*-

import json
import logging
import os
from multiprocessing.pool import ThreadPool
from random import randint
from time import sleep
from typing import List, Union

import pandas as pd
import requests
from dotenv import load_dotenv
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from joblib import Memory

from src.service_utils import project_root

load_dotenv()
KEY = os.getenv("GEO_API_KEY")
logging.basicConfig(level=logging.INFO)
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


def get_address_worker_v2(coordinate: str) -> Union[str, None]:
    """
    Function to get address from external API (positionstack)
    Caching results is recommended due to API limitations

    :param coordinate: Concatenated Latitude and Longitude. example: "45.7865,-56.9483"
    :return: Geographical address for given coordinates or None in case of API errors
    """

    query = f"http://api.positionstack.com/v1/reverse?access_key={KEY}&query={coordinate}"
    retries_count = 2
    retried = 0
    info = None

    # Two retries with small ping and corrupted data detection
    while True:
        resp = requests.get(query)
        if resp.status_code != 200 and retried < retries_count:
            retried += 1
            sleep(randint(5 * 100, 15 * 100) / 1000)
            continue

        info = json.loads(resp.text)

        if info["data"][0] == [] and retried < retries_count:
            retried += 1
            sleep(randint(5 * 100, 15 * 100) / 1000)
            continue
        break

    if info is None:
        logging.warning("API error")
        return None

    # Build address from responded fields
    fields = info["data"][0]
    address_parts = [
        fields["name"],
        fields["number"],
        fields["street"],
        fields["region"],
        fields["postal_code"],
        fields["country"],
    ]
    address_parts = [part for part in address_parts if part is not None]

    # Pick the best address definition and error-proof
    if (address_parts is [None] or len(address_parts) <= 1) and fields["label"] is not None:
        address = fields["label"]
    elif address_parts is not [None]:
        address = ", ".join(address_parts)
    else:
        address = None

    return address


# Decorate geodata_worker "manually"
# to use it decorated with multithreading
get_address_worker_v2 = memory.cache(get_address_worker_v2)


def collect_geo_data(coordinates: pd.DataFrame, threads_count: int = 10) -> List:
    """
    Uses get_address_worker to form list of geographical addresses by given
    DataFrame coordinates

    :param coordinates: pandas DataFrame
    :param threads_count: count of threads to request API
    :return: List of addresses for given DataFrame of coordinates
    """

    coordinates_list = []

    for coordinate in coordinates.values:
        list(coordinate)
        coordinates_list.append(",".join([str(coordinate[0]), str(coordinate[1])]))

    with ThreadPool(threads_count) as tp:
        address_list = tp.map(get_address_worker_v2, coordinates_list)

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
