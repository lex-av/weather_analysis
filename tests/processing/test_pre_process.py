# -*- coding: utf-8 -*-

import pytest

from src.processing.pre_process import df_cleaner, df_generator, df_group_and_filter


def test_df_generator_read_csv(get_path):
    data_frames = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    assert list(next(data_frames).columns) == ["Name", "Country", "City", "Latitude", "Longitude"]


def test_df_generator_read_all(get_path):
    data_frames = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    next(data_frames)
    next(data_frames)
    next(data_frames)
    with pytest.raises(StopIteration):
        next(data_frames)


def test_df_cleaner(get_path):
    data_frames = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    next(data_frames)  # Skip first file in archive
    df_clear = df_cleaner(next(data_frames))
    assert df_clear.City.count() == 1


def test_df_group_and_filter(get_path):
    data_frames = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    df_full = df_group_and_filter([df_cleaner(df) for df in data_frames])

    assert df_full["City"].values[0] == "Oak Brook"
