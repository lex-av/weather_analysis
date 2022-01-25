# -*- coding: utf-8 -*-

import datetime
import json
from multiprocessing.pool import ThreadPool
from typing import List

import pandas as pd
import requests

from src.constants import WEATHER_API_KEY


def weather_api_worker(lat: float, lon: float) -> dict:
    """
    Gets weather data for given coordinate from openweathermap.org
    API key is constants.py required

    :param lat: Latitude
    :param lon: Longitude
    :return: Dictionary of json data
    """

    resp = requests.get(
        f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={WEATHER_API_KEY}&units=metric"
    )

    return json.loads(resp.text)


def weather_api_historical_worker(lat: float, lon: float, utc_time: int) -> dict:
    """
    Gets historical weather data for given coordinate and datetime from openweathermap.org
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


def extract_weather_data(json_resp: dict, latitude: float, longitude: float, city_name: str) -> dict:
    """
    Reads api response, converted to dict and extracts needed for
    future analysis data. Uses data from worker, that requests
    data for current and forecast weather

    :param json_resp: Response from external API, converted
    into dict structure
    :param latitude: Centre latitude used for request
    :param longitude: Centre longitude used for request
    :param city_name: city for this coordinates - marker for future DataFrame
    :return: Dict of lists with data to load into pd.DataFrame
    """

    # Need today's complete info (from forecast section) and only 5-day forecast
    daily_data = json_resp["daily"][:6]
    dates = [datetime.datetime.fromtimestamp(day["dt"]).date() for day in daily_data]
    temp_values = [day["temp"]["day"] for day in daily_data]
    min_temp_values = [day["temp"]["min"] for day in daily_data]
    max_temp_values = [day["temp"]["max"] for day in daily_data]

    center_data_dict = {
        "Date": dates,
        "City": [city_name for i in range(len(dates))],
        "Latitude": latitude,
        "Longitude": longitude,
        "DayTemp": temp_values,
        "MinTemp": min_temp_values,
        "MaxTemp": max_temp_values,
    }

    return center_data_dict


def extract_hist_weather_data(json_resp: List[dict], latitude: float, longitude: float, city_name: str) -> dict:
    """
    Reads api response, converted to dict and extracts needed for
    future analysis data. Uses data from historical worker, that
    requests data for historical weather data

    :param json_resp: Response from external API, converted
    into dict structure
    :param latitude: Centre latitude used for request
    :param longitude: Centre longitude used for request
    :param city_name: city for this coordinates - marker for future DataFrame
    :return: Dict of lists with data to load into pd.DataFrame
    """

    date = [datetime.datetime.fromtimestamp(json_resp[i]["current"]["dt"]).date() for i in range(len(json_resp))]
    # latitude = json_resp[0]["lat"]
    # longitude = json_resp[0]["lon"]
    current_temp_lst = [data["current"]["temp"] for data in json_resp]
    hourly_data = [data["hourly"] for data in json_resp]

    min_temp_values = [min([hour["temp"] for hour in hourly_data[i]]) for i in range(len(hourly_data))]
    max_temp_values = [max([hour["temp"] for hour in hourly_data[i]]) for i in range(len(hourly_data))]

    center_data_dict = {
        "Date": date,
        "City": city_name,
        "Latitude": latitude,
        "Longitude": longitude,
        "DayTemp": current_temp_lst,
        "MinTemp": min_temp_values,
        "MaxTemp": max_temp_values,
    }

    return center_data_dict


def get_centre_current_forecast_weather(latitude: float, longitude: float, city_name: str) -> pd.DataFrame:
    """
    Gathers weather information for given city coordinates from openweathermap.org API
    and packs it into pandas DataFrame, using extract function. Has low effect on
    API load - only one request per City

    :param latitude: city_centre latitude
    :param longitude: city_centre longitude
    :param city_name: marker for DataFrame
    :return: pandas DataFrame of weather info for current city
    """

    weather_data = weather_api_worker(latitude, longitude)
    center_weather_dict = extract_weather_data(weather_data, latitude, longitude, city_name)

    return pd.DataFrame.from_dict(center_weather_dict)


def get_centre_historical_weather(latitude: float, longitude: float, city_name: str) -> pd.DataFrame:
    """
    Gets weather historical data for five days from API in parallel and
    packs it into pandas DataFrame, using extract function.
    Has high effect on API load - 5 request per City

    :param latitude: city_centre latitude
    :param longitude: city_centre longitude
    :param city_name: marker for future DataFrame, built from this structure
    :return: List of dicts, containing response information
    """

    # Time values preparation
    # Converting datetime to utc timestamp
    current_time = datetime.datetime.now(datetime.timezone.utc)
    dt_to_request = [current_time - datetime.timedelta(t) for t in range(1, 6)]
    timestamps_to_request = [int(utc_time.timestamp()) for utc_time in dt_to_request]

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

    center_weather_dict = extract_hist_weather_data(weather_data, latitude, longitude, city_name)

    return pd.DataFrame.from_dict(center_weather_dict)


if __name__ == "__main__":
    pass
