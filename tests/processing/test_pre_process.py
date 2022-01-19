# -*- coding: utf-8 -*-

import os

import pytest

from src.processing.pre_process import df_generator


@pytest.fixture()
def get_path():
    return os.getcwd()  # CWD in configuration has to be root of a project


def test_df_generator_read_csv(get_path):
    new_gen = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    assert list(next(new_gen).columns) == ["Id", "Name", "Country", "City", "Latitude", "Longitude"]


def test_df_generator_read_all(get_path):
    new_gen = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    next(new_gen)
    next(new_gen)
    with pytest.raises(StopIteration):
        next(new_gen)


def test_df_generator_clear_csv(get_path):
    new_gen = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    next(new_gen)  # Skip first file in archive
    df_with_nan = next(new_gen)
    assert df_with_nan.City.count() == 1
