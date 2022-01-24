# -*- coding: utf-8 -*-

import datetime
import json

import requests

from src.connection_utils.constants import WEATHER_API_KEY


def weather_api_worker(lat: float, lon: float) -> dict:
    """
    Gets weather data for given coordinate from openweathermap.org
    API key is constants.py required

    :param lat: Latitude
    :param lon: Longitude
    :return: Dictionary of json data
    """

    resp = requests.get(f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={WEATHER_API_KEY}")

    return json.loads(resp.text)


def weather_api_historical_worker(lat: float, lon: float, utc_time: int) -> dict:
    """
    Gets weather data for given coordinate and datetime from openweathermap.org
    API key is constants.py required

    :param lat: Latitude
    :param lon: Longitude
    :param utc_time: Timestamp of datetime, weather data needed to
    :return: Dictionary of json data
    """

    resp = requests.get(
        (
            f"https://api.openweathermap.org/data/2.5/onecall/timemachine"
            f"?lat={lat}&lon={lon}&dt={utc_time}&appid={WEATHER_API_KEY}"
        )
    )

    return json.loads(resp.text)


if __name__ == "__main__":
    new_lat = 33.44
    new_lon = -94.04

    dt_past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(5)
    utc_time = dt_past.replace(tzinfo=datetime.timezone.utc)
    utc_past = int(utc_time.timestamp())

    data = weather_api_historical_worker(new_lat, new_lon, utc_past)

    print()

# data['daily'][0]['dt']
# datetime.fromtimestamp(data["current"]["dt"]).date()
