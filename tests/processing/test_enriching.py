import json
from unittest.mock import patch

from src.processing.enriching import enrich_with_geo_data, enrich_with_weather_data
from src.processing.post_process import generate_centres_df
from src.processing.pre_process import df_cleaner, df_generator, df_group_and_filter


def fake_get_address_worker(coordinate):
    return "Totally real address of real Country"


def test_enrich_with_geo_data(get_path):
    data_frames = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    df_full = df_group_and_filter([df_cleaner(df) for df in data_frames])

    with patch("src.api_utils.geodata_api.get_address_worker", fake_get_address_worker):
        enrich_with_geo_data(df_full)

    assert df_full["Address"].values[0] == "Totally real address of real Country"


def test_enrich_with_weather_data(get_path):
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

    assert list(weather_df.keys()) == ["Date", "City", "Latitude", "Longitude", "DayTemp", "MinTemp", "MaxTemp"]
