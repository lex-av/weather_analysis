# -*- coding: utf-8 -*-

from unittest import mock

from src.api_utils.weather_api import (
    get_centre_current_forecast_weather,
    get_centre_historical_weather,
)


def test_get_centre_current_forecast_weather(api_get_weather):
    def fake_weather_api_worker(lat, lon):
        return api_get_weather

    with mock.patch("src.api_utils.weather_api.weather_api_worker", fake_weather_api_worker):
        result = get_centre_current_forecast_weather(52.3375677, 4.8178172, "Amsterdam")

    assert list(result.keys()) == ["Date", "City", "Latitude", "Longitude", "DayTemp", "MinTemp", "MaxTemp"]


def test_get_centre_historical_weather(api_get_hist_weather):
    def fake_api_historical_worker(lat, lon, utc_time):
        return api_get_hist_weather

    with mock.patch("src.api_utils.weather_api.weather_api_historical_worker", fake_api_historical_worker):
        result = get_centre_historical_weather(52.3375677, 4.8178172, "Amsterdam")

    assert list(result.keys()) == ["Date", "City", "Latitude", "Longitude", "DayTemp", "MinTemp", "MaxTemp"]
