# -*- coding: utf-8 -*-

from unittest.mock import patch

from src.api_utils.geodata_api import calc_centre, collect_geo_data
from src.processing.pre_process import df_cleaner, df_generator, df_group_and_filter


def fake_get_address_worker(coordinate):
    return "Totally real address of real Country"


def test_collect_geo_data(get_path):
    data_frames_gen = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    df_full = df_group_and_filter([df_cleaner(df) for df in data_frames_gen])

    with patch("src.api_utils.geodata_api.get_address_worker_v2", fake_get_address_worker):
        val = collect_geo_data(df_full[["Latitude", "Longitude"]])

    assert val[0] == "Totally real address of real Country"


def test_calc_centre(get_path):
    data_frames_gen = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    df_full = df_group_and_filter([df_cleaner(df) for df in data_frames_gen])

    centroid = calc_centre(df_full[df_full["City"] == "Amsterdam"])

    assert centroid == [52.3375677, 4.8178172]
