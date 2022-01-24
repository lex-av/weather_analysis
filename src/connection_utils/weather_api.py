# -*- coding: utf-8 -*-

import datetime
import json
from multiprocessing.pool import ThreadPool
from typing import List

import pandas as pd
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
            f"?lat={lat}&lon={lon}&dt={utc_time}&appid={WEATHER_API_KEY}&units=metric"
        )
    )

    return json.loads(resp.text)


def get_city_centre_current_forecast_weather():
    """"""

    return


def get_city_centre_historical_weather(latitude: float, longitude: float, city_name: str) -> List:
    """
    Gets weather historical data for five days from API in parallel

    :param latitude: city_centre latitude
    :param longitude: city_centre longitude
    :param city_name: marker for future DataFrame, built from this structure
    :return: List of dicts, containing response information
    """

    # Time values preparation
    # Converting datetime to utc timestamp
    current_time = datetime.datetime.now(datetime.timezone.utc)
    dt_to_request = [current_time - datetime.timedelta(t) for t in range(1, 6)]
    utc_dt_to_request = [dt.replace(tzinfo=datetime.timezone.utc) for dt in dt_to_request]
    timestamps_to_request = [int(utc_time.timestamp()) for utc_time in utc_dt_to_request]

    # Five requests for five days data
    threads_count = len(timestamps_to_request)

    # Parallel requesting of historical data
    with ThreadPool(threads_count) as tp:
        weather_data = tp.starmap(
            weather_api_historical_worker,
            zip(
                [latitude for i in range(len(timestamps_to_request))],
                [longitude for i in range(len(timestamps_to_request))],
                timestamps_to_request,
            ),
        )

    # City marker for later DataFrame generating
    for data in weather_data:
        data["City"] = city_name

    return weather_data


def build_current_forecast_weather_data_df():
    """"""

    return


def build_historical_weather_data_df(w_data: List) -> pd.DataFrame:
    """
    Collects useful data from API-responses and puts it
    into pd DataFrame, able to concatenate

    :param w_data: Weather historical data list of dict
    :return: pandas DataFrame
    """

    date = [datetime.datetime.fromtimestamp(w_data[i]["current"]["dt"]).date() for i in range(len(w_data))]
    city = w_data[0]["City"]
    latitude = w_data[0]["lat"]
    longitude = w_data[0]["lon"]
    current_temp_lst = [data["current"]["temp"] for data in w_data]
    hourly_data = [data["hourly"] for data in w_data]

    min_temp_values = [min([hour["temp"] for hour in hourly_data[i]]) for i in range(len(hourly_data))]
    max_temp_values = [max([hour["temp"] for hour in hourly_data[i]]) for i in range(len(hourly_data))]

    center_weather_dict = {
        "Date": date,
        "City": city,
        "Latitude": latitude,
        "Longitude": longitude,
        "DayTemp": current_temp_lst,
        "MinTemp": min_temp_values,
        "MaxTemp": max_temp_values,
    }

    df = pd.DataFrame.from_dict(center_weather_dict)

    return df


if __name__ == "__main__":
    pass
