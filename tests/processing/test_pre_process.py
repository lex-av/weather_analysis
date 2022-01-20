# -*- coding: utf-8 -*-

import os

import pytest

from src.processing.pre_process import df_cleaner, df_generator


@pytest.fixture()
def get_path():
    return os.getcwd()  # CWD in configuration has to be root of a project


def test_df_generator_read_csv(get_path):
    new_gen = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    assert list(next(new_gen).columns) == ["Name", "Country", "City", "Latitude", "Longitude"]


def test_df_generator_read_all(get_path):
    new_gen = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    next(new_gen)
    next(new_gen)
    with pytest.raises(StopIteration):
        next(new_gen)


def test_df_cleaner(get_path):
    new_gen = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    next(new_gen)  # Skip first file in archive
    df_clear = df_cleaner(next(new_gen))
    assert df_clear.City.count() == 1
