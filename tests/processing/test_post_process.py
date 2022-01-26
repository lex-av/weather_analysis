# -*- coding: utf-8 -*-
import json
from unittest.mock import patch

from src.processing.enriching import enrich_with_weather_data
from src.processing.post_process import generate_centres_df, generate_top_df
from src.processing.pre_process import df_cleaner, df_generator, df_group_and_filter


def test_generate_centres_df(get_path):
    data_frames = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    df_full = df_group_and_filter([df_cleaner(df) for df in data_frames])
    centre_df = generate_centres_df(df_full)

    assert list(centre_df.keys()) == ["City", "Latitude", "Longitude"]


def test_generate_top_df(get_path):
    data_frames = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    df_full = df_group_and_filter([df_cleaner(df) for df in data_frames])
    centre_df = generate_centres_df(df_full).iloc[1:2]  # Only Amsterdam city row

    def fake_weather_api_worker(lat, lon):
        with open(get_path + "/tests/test_data/weather_api_worker_sample_resp.json") as src:
            payload = json.load(src)
        return payload

    def fake_weather_api_historical_worker(lat, lon, time):
        with open(get_path + "/tests/test_data/weather_api_historical_worker_sample_resp.json") as src:
            payload = json.load(src)
        return payload

    with patch("src.api_utils.weather_api.weather_api_worker", fake_weather_api_worker):
        with patch("src.api_utils.weather_api.weather_api_historical_worker", fake_weather_api_historical_worker):
            weather_df = enrich_with_weather_data(centre_df)

    top = generate_top_df(weather_df)

    assert list(top.index) == [
        "City with max temperature",
        "City with max delta of max temp",
        "City with min temperature",
        "City with max delta of max and min temp",
    ]
